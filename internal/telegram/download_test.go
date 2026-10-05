package telegram

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/tg"
)

func TestDownloadFileName(t *testing.T) {
	tests := []struct {
		name      string
		msgID     int
		mediaType string
		title     string
		fallback  string
		want      string
	}{
		{"keeps existing extension", 5, "document", "report.pdf", "media", "5_report.pdf"},
		{"traversal path collapses to base", 5, "photo", "../../etc/passwd", "media", "5_passwd.jpg"},
		{"nested path collapses to base", 7, "video", "a/b", "media", "7_b.mp4"},
		{"backslash separators stripped", 6, "photo", `a\b.jpg`, "media", "6_ab.jpg"},
		{"control characters stripped", 8, "audio", "weird\x01\x07name.mp3", "media", "8_weirdname.mp3"},
		{"empty title uses fallback", 9, "voice", "", "media", "9_media.oga"},
		{"blank title uses fallback", 3, "photo", "   ", "media", "3_media.jpg"},
		{"empty fallback renamed", 13, "sticker", "", "", "13_media"},
		{"photo extension fallback", 1, "photo", "cover", "media", "1_cover.jpg"},
		{"document extension fallback", 11, "document", "blob", "media", "11_blob.bin"},
		{"webpage extension fallback", 2, "webpage", "Article title", "media", "2_Article title.html"},
		{"video extension fallback", 4, "video", "clip", "media", "4_clip.mp4"},
		{"audio extension fallback", 12, "audio", "track", "media", "12_track.mp3"},
		{"unknown media type gets none", 14, "sticker", "anim", "media", "14_anim"},
		{"dot-only extension counts as missing", 15, "photo", "file.", "media", "15_file.jpg"},
		{"trailing separator dropped", 16, "photo", "dir/", "media", "16_dir.jpg"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DownloadFileName(tt.msgID, tt.mediaType, tt.title, tt.fallback)
			if got != tt.want {
				t.Fatalf("DownloadFileName(%d, %q, %q, %q) = %q, want %q", tt.msgID, tt.mediaType, tt.title, tt.fallback, got, tt.want)
			}
			if filepath.Base(got) != got {
				t.Fatalf("name %q must stay inside the destination directory", got)
			}
		})
	}
}

// mediaTestFetcher builds a fetcher over a client that fails on any direct
// network call, with injectable seams.
func mediaTestFetcher(t *testing.T, pager mediaDialogsPager, fetcher mediaMessagesFetcher) *MediaFetcher {
	t.Helper()
	raw := tg.NewClient(telegram.InvokeFunc(func(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
		return errors.New("unexpected direct network call")
	}))
	f := NewMediaFetcher(raw, 1)
	f.dialogsPager = pager
	f.messagesFetcher = fetcher
	return f
}

// recordPager returns a dialogs pager seam that replays elems, stops when the
// callback stops, and records invocation counts.
func recordPager(elems []dialogs.Elem, calls *int) mediaDialogsPager {
	return func(ctx context.Context, fn func(context.Context, dialogs.Elem) error) error {
		*calls++
		for _, elem := range elems {
			if err := fn(ctx, elem); err != nil {
				return err
			}
		}
		return nil
	}
}

func TestMediaFetcherResolvesPeerAliases(t *testing.T) {
	channelPeer := &tg.InputPeerChannel{ChannelID: 123, AccessHash: 42}
	elems := []dialogs.Elem{
		{Peer: &tg.InputPeerChat{ChatID: 456}},
		{Peer: channelPeer},
		{Peer: &tg.InputPeerUser{UserID: 99}},
	}
	var pagerCalls int
	fetcher := mediaTestFetcher(t, recordPager(elems, &pagerCalls), func(_ context.Context, _ tg.InputPeerClass, _ []tg.InputMessageClass) (tg.MessagesMessagesClass, error) {
		return &tg.MessagesMessagesNotModified{}, nil
	})

	tests := []struct {
		name      string
		chatID    string
		peer      tg.InputPeerClass
		wantCalls int
	}{
		{"channel short alias", "0000000123", channelPeer, 1},
		{"basic group alias", "456", elems[0].Peer, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := fetcher.FetchChatMedia(context.Background(), tt.chatID, []int{101}, DownloadOptions{Dest: t.TempDir()})
			if err != nil {
				t.Fatalf("FetchChatMedia: %v", err)
			}
			if len(results) != 1 || results[0].Status != "not_found" {
				t.Fatalf("unexpected results: %+v", results)
			}
			if tt.peer == channelPeer {
				got, ok := fetcher.peerByID[tt.chatID]
				if !ok || got != tt.peer {
					t.Fatalf("cached peer = %+v, want %+v", got, channelPeer)
				}
			}
		})
	}
	if pagerCalls != 2 {
		t.Fatalf("second resolution must hit the peer cache: pager calls = %d", pagerCalls)
	}
}

// fakePeerHashes is an in-memory PeerHashStore seam.
type fakePeerHashes struct {
	channels     map[int64]int64
	users        map[int64]int64
	err          error
	channelCalls int
	userCalls    int
}

func (f *fakePeerHashes) ChannelAccessHash(_ context.Context, channelID int64) (int64, bool, error) {
	f.channelCalls++
	if f.err != nil {
		return 0, false, f.err
	}
	hash, ok := f.channels[channelID]
	return hash, ok, nil
}

func (f *fakePeerHashes) UserAccessHash(_ context.Context, userID int64) (int64, bool, error) {
	f.userCalls++
	if f.err != nil {
		return 0, false, f.err
	}
	hash, ok := f.users[userID]
	return hash, ok, nil
}

// TestMediaFetcherResolvesPeerFromHashStore covers chats absent from the
// dialog list: the persisted hash answers before any dialog scan, for both
// channels (-100 jids) and users (plain positive jids).
func TestMediaFetcherResolvesPeerFromHashStore(t *testing.T) {
	hashes := &fakePeerHashes{
		channels: map[int64]int64{123: 777},
		users:    map[int64]int64{99: 555},
	}
	var pagerCalls int
	var fetched tg.InputPeerClass
	f := NewMediaFetcherWithHashes(tg.NewClient(telegram.InvokeFunc(func(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
		return errors.New("unexpected direct network call")
	})), 1, hashes)
	f.dialogsPager = recordPager(nil, &pagerCalls)
	f.messagesFetcher = func(_ context.Context, peer tg.InputPeerClass, _ []tg.InputMessageClass) (tg.MessagesMessagesClass, error) {
		fetched = peer
		return &tg.MessagesMessagesNotModified{}, nil
	}

	for _, tt := range []struct {
		chatID string
		want   tg.InputPeerClass
	}{
		{"-100123", &tg.InputPeerChannel{ChannelID: 123, AccessHash: 777}},
		{"99", &tg.InputPeerUser{UserID: 99, AccessHash: 555}},
	} {
		results, err := f.FetchChatMedia(context.Background(), tt.chatID, []int{101}, DownloadOptions{Dest: t.TempDir()})
		if err != nil {
			t.Fatalf("FetchChatMedia(%s): %v", tt.chatID, err)
		}
		if len(results) != 1 || results[0].Status != "not_found" {
			t.Fatalf("results = %+v, want not_found", results)
		}
		if !reflect.DeepEqual(fetched, tt.want) {
			t.Fatalf("fetched peer = %+v, want %+v", fetched, tt.want)
		}
	}
	if pagerCalls != 0 {
		t.Fatalf("hash hits must skip the dialog scan: pager calls = %d", pagerCalls)
	}
	if hashes.channelCalls != 1 || hashes.userCalls != 1 {
		t.Fatalf("hash store calls = %d/%d, want one each (cache must absorb repeats)", hashes.channelCalls, hashes.userCalls)
	}
}

// TestMediaFetcherHashMissFallsBackToDialogs keeps the dialog scan in play
// when the store has no entry; store errors surface instead of degrading.
func TestMediaFetcherHashMissFallsBackToDialogs(t *testing.T) {
	channelPeer := &tg.InputPeerChannel{ChannelID: 123, AccessHash: 42}
	elems := []dialogs.Elem{{Peer: channelPeer}}

	var pagerCalls int
	f := NewMediaFetcherWithHashes(tg.NewClient(telegram.InvokeFunc(func(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
		return errors.New("unexpected direct network call")
	})), 1, &fakePeerHashes{channels: map[int64]int64{}})
	f.dialogsPager = recordPager(elems, &pagerCalls)
	f.messagesFetcher = func(_ context.Context, _ tg.InputPeerClass, _ []tg.InputMessageClass) (tg.MessagesMessagesClass, error) {
		return &tg.MessagesMessagesNotModified{}, nil
	}
	if _, err := f.FetchChatMedia(context.Background(), "-100123", []int{101}, DownloadOptions{Dest: t.TempDir()}); err != nil {
		t.Fatalf("FetchChatMedia: %v", err)
	}
	if pagerCalls != 1 {
		t.Fatalf("hash miss must fall back to the dialog scan: pager calls = %d", pagerCalls)
	}

	broken := NewMediaFetcherWithHashes(tg.NewClient(telegram.InvokeFunc(func(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
		return errors.New("unexpected direct network call")
	})), 1, &fakePeerHashes{err: errors.New("store offline")})
	broken.dialogsPager = recordPager(elems, &pagerCalls)
	if _, err := broken.FetchChatMedia(context.Background(), "-100123", []int{101}, DownloadOptions{Dest: t.TempDir()}); err == nil || !strings.Contains(err.Error(), "store offline") {
		t.Fatalf("hash store error = %v, want surfaced store failure", err)
	}
}

func TestMediaFetcherUnknownChat(t *testing.T) {
	var pagerCalls int
	fetcher := mediaTestFetcher(t, recordPager([]dialogs.Elem{{Peer: &tg.InputPeerChat{ChatID: 456}}}, &pagerCalls), nil)

	results, err := fetcher.FetchChatMedia(context.Background(), "777", []int{1, 2}, DownloadOptions{Dest: t.TempDir()})
	if err != nil {
		t.Fatalf("FetchChatMedia: %v", err)
	}
	if pagerCalls != 1 {
		t.Fatalf("pager calls = %d, want 1", pagerCalls)
	}
	if len(results) != 2 {
		t.Fatalf("results = %+v, want one per message", results)
	}
	for i, result := range results {
		if result.Status != "not_found" || result.ChatID != "777" || result.MessageID != i+1 {
			t.Fatalf("result[%d] = %+v", i, result)
		}
	}
}

func documentMessage(id int, size int64) *tg.Message {
	doc := &tg.Document{
		ID:            7,
		AccessHash:    8,
		FileReference: []byte("ref"),
		MimeType:      "application/pdf",
		Size:          size,
		Attributes:    []tg.DocumentAttributeClass{&tg.DocumentAttributeFilename{FileName: "notes.pdf"}},
	}
	return &tg.Message{ID: id, Message: "attachment", Media: &tg.MessageMediaDocument{Document: doc}}
}

func TestMediaFetcherStatuses(t *testing.T) {
	plain := &tg.Message{ID: 102, Message: "hello"}
	doc := documentMessage(101, 512)
	huge := documentMessage(104, 1<<20)

	tests := []struct {
		name     string
		msgIDs   []int
		response tg.MessagesMessagesClass
		want     map[int]string
	}{
		{
			name:   "found not_found and no_media",
			msgIDs: []int{101, 102, 103},
			response: &tg.MessagesMessages{Messages: []tg.MessageClass{
				doc,
				plain,
				&tg.MessageService{ID: 555},
			}},
			want: map[int]string{101: "error", 102: "no_media", 103: "not_found"},
		},
		{
			name:     "not modified response yields not_found",
			msgIDs:   []int{101, 102},
			response: &tg.MessagesMessagesNotModified{},
			want:     map[int]string{101: "not_found", 102: "not_found"},
		},
		{
			name:   "declared size over cap is too_large",
			msgIDs: []int{104},
			response: &tg.MessagesMessagesSlice{
				Count:    1,
				Messages: []tg.MessageClass{huge},
			},
			want: map[int]string{104: "too_large"},
		},
		{
			name:   "channel response shape parses",
			msgIDs: []int{101},
			response: &tg.MessagesChannelMessages{
				Count:    1,
				Messages: []tg.MessageClass{documentMessage(101, 4)},
			},
			want: map[int]string{101: "error"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fetcher := mediaTestFetcher(t,
				func(ctx context.Context, fn func(context.Context, dialogs.Elem) error) error {
					return fn(ctx, dialogs.Elem{Peer: &tg.InputPeerChannel{ChannelID: 123, AccessHash: 42}})
				},
				func(_ context.Context, _ tg.InputPeerClass, _ []tg.InputMessageClass) (tg.MessagesMessagesClass, error) {
					return tt.response, nil
				},
			)
			results, err := fetcher.FetchChatMedia(context.Background(), "-1000000000123", tt.msgIDs, DownloadOptions{Dest: t.TempDir(), MaxBytes: 1000})
			if err != nil {
				t.Fatalf("FetchChatMedia: %v", err)
			}
			if len(results) != len(tt.msgIDs) {
				t.Fatalf("results = %+v, want %d entries", results, len(tt.msgIDs))
			}
			for _, result := range results {
				if want := tt.want[result.MessageID]; result.Status != want {
					t.Fatalf("message %d status = %q (%+v), want %q", result.MessageID, result.Status, result, want)
				}
			}
		})
	}
}

func TestMediaFetcherTooLargeDetail(t *testing.T) {
	fetcher := mediaTestFetcher(t,
		func(ctx context.Context, fn func(context.Context, dialogs.Elem) error) error {
			return fn(ctx, dialogs.Elem{Peer: &tg.InputPeerChat{ChatID: 456}})
		},
		func(_ context.Context, _ tg.InputPeerClass, _ []tg.InputMessageClass) (tg.MessagesMessagesClass, error) {
			return &tg.MessagesMessages{Messages: []tg.MessageClass{documentMessage(104, 1<<20)}}, nil
		},
	)
	results, err := fetcher.FetchChatMedia(context.Background(), "-456", []int{104}, DownloadOptions{Dest: t.TempDir(), MaxBytes: 1000})
	if err != nil {
		t.Fatalf("FetchChatMedia: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %+v", results)
	}
	result := results[0]
	if result.Status != "too_large" || result.Size != 1<<20 || result.Title != "notes.pdf" {
		t.Fatalf("too_large result = %+v", result)
	}
}

func TestMediaFetcherDedupesAndOrders(t *testing.T) {
	var requestedIDs []tg.InputMessageClass
	fetcher := mediaTestFetcher(t,
		func(ctx context.Context, fn func(context.Context, dialogs.Elem) error) error {
			return fn(ctx, dialogs.Elem{Peer: &tg.InputPeerChat{ChatID: 456}})
		},
		func(_ context.Context, _ tg.InputPeerClass, ids []tg.InputMessageClass) (tg.MessagesMessagesClass, error) {
			requestedIDs = ids
			messages := make([]tg.MessageClass, 0, len(ids))
			for _, id := range ids {
				messages = append(messages, &tg.Message{ID: id.(*tg.InputMessageID).ID, Message: "plain"})
			}
			return &tg.MessagesMessages{Messages: messages}, nil
		},
	)
	results, err := fetcher.FetchChatMedia(context.Background(), "-456", []int{303, 202, 303, 101, 202}, DownloadOptions{Dest: t.TempDir()})
	if err != nil {
		t.Fatalf("FetchChatMedia: %v", err)
	}
	got := make([]int, 0, len(results))
	for _, result := range results {
		got = append(got, result.MessageID)
		if result.Status != "no_media" {
			t.Fatalf("result %+v, want no_media", result)
		}
	}
	want := []int{303, 202, 101}
	if len(got) != len(want) {
		t.Fatalf("results %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("results %v, want %v", got, want)
		}
	}
	if len(requestedIDs) != len(want) {
		t.Fatalf("requested ids %v, want one per unique message", requestedIDs)
	}
}

func TestDownloadViaTDataMissingSource(t *testing.T) {
	_, err := DownloadViaTData(context.Background(), filepath.Join(t.TempDir(), "missing"), "456", nil, DownloadOptions{})
	if err == nil {
		t.Fatal("missing tdata source must fail")
	}
}

func TestAcquireConnectionLock(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "archive.db")
	release, err := AcquireConnectionLock(dbPath)
	if err != nil {
		t.Fatalf("AcquireConnectionLock: %v", err)
	}
	if _, err := AcquireConnectionLock(dbPath); err == nil {
		t.Fatal("second acquisition must fail while held")
	}
	if !WatchLockHeld(dbPath) {
		t.Fatal("lock must be reported as held")
	}
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if WatchLockHeld(dbPath) {
		t.Fatal("lock must be reported as free after release")
	}
}
