package telegram

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/mukhanov/telemcp/internal/store"
)

// tgTestUser builds a user with the flag-aware setters, so optional-field
// getters (FirstName, Username, ...) behave as after a wire decode.
func tgTestUser(id int64, firstName string) *tg.User {
	u := &tg.User{ID: id}
	u.SetFirstName(firstName)
	return u
}

// newTestWatchHandler builds a handler that captures live batches instead of
// writing them anywhere.
func newTestWatchHandler(selfID int64) (*watchHandler, *[]store.Message, *[]store.Chat) {
	var (
		messages []store.Message
		chats    []store.Chat
	)
	h := &watchHandler{
		selfID:   selfID,
		liveOpts: ImportOptions{},
		stats: store.ImportStats{
			SourcePath:          "/data/tdata",
			SourcePathCanonical: true,
			SourceIdentity:      sourceIdentity("tdata", "1"),
		},
		hooks: WatchHooks{
			Live: func(ctx context.Context, st *store.Store, stats store.ImportStats, c []store.Chat, m []store.Message) error {
				chats = append(chats, c...)
				messages = append(messages, m...)
				return nil
			},
		},
	}
	h.liveSession = &tdataImportSession{raw: nil, selfID: selfID, opts: h.liveOpts}
	return h, &messages, &chats
}

func TestWatchHandlerConvertsContainerMessages(t *testing.T) {
	ctx := context.Background()
	h, messages, chats := newTestWatchHandler(1)

	container := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewChannelMessage{Message: &tg.Message{
				ID:      55,
				PeerID:  &tg.PeerChannel{ChannelID: 42},
				Message: "hello channel",
				Date:    1700000100,
			}},
			&tg.UpdateNewMessage{Message: &tg.Message{
				ID:      7,
				PeerID:  &tg.PeerUser{UserID: 10},
				Message: "hello alice",
				Date:    1700000200,
			}},
			// Edits are deferred to the reconcile pass in v1.
			&tg.UpdateEditChannelMessage{Message: &tg.Message{
				ID:      50,
				PeerID:  &tg.PeerChannel{ChannelID: 42},
				Message: "edited",
				Date:    1600000000,
			}},
		},
		Chats: []tg.ChatClass{&tg.Channel{ID: 42, Title: "Test Channel"}},
		Users: []tg.UserClass{tgTestUser(10, "Alice")},
	}
	if err := h.Handle(ctx, container); err != nil {
		t.Fatalf("handle container: %v", err)
	}

	if len(*messages) != 2 {
		t.Fatalf("converted %d messages, want 2 (edit must be skipped): %#v", len(*messages), *messages)
	}
	channelMsg, userMsg := (*messages)[0], (*messages)[1]
	if channelMsg.ChatJID != "-1000000000042" || channelMsg.Text != "hello channel" || channelMsg.MessageID != "55" {
		t.Fatalf("channel message: %#v", channelMsg)
	}
	if channelMsg.ChatName != "Test Channel" {
		t.Fatalf("channel chat name = %q, want Test Channel", channelMsg.ChatName)
	}
	if want := stableTDataSourcePK("-1000000000042", 55); channelMsg.SourcePK != want {
		t.Fatalf("channel source pk = %d, want %d (import-path parity)", channelMsg.SourcePK, want)
	}
	if !channelMsg.Timestamp.Equal(time.Unix(1700000100, 0).UTC()) {
		t.Fatalf("channel timestamp = %v", channelMsg.Timestamp)
	}
	if userMsg.ChatJID != "10" || userMsg.Text != "hello alice" || userMsg.ChatName != "Alice" {
		t.Fatalf("user message: %#v", userMsg)
	}

	if len(*chats) != 2 {
		t.Fatalf("converted %d chats, want 2", len(*chats))
	}
	channelChat, userChat := (*chats)[0], (*chats)[1]
	if channelChat.JID != "-1000000000042" || channelChat.Kind != "channel" || channelChat.Name != "Test Channel" {
		t.Fatalf("channel chat row: %#v", channelChat)
	}
	if userChat.JID != "10" || userChat.Kind != "user" || userChat.Name != "Alice" {
		t.Fatalf("user chat row: %#v", userChat)
	}
}

func TestWatchHandlerConvertsWithoutEntities(t *testing.T) {
	// Containers may arrive without any entities (e.g. gap-recovered
	// difference slices); conversion must still produce rows keyed by chat id.
	ctx := context.Background()
	h, messages, _ := newTestWatchHandler(1)

	if err := h.Handle(ctx, &tg.Updates{Updates: []tg.UpdateClass{
		&tg.UpdateNewMessage{Message: &tg.Message{
			ID:      8,
			PeerID:  &tg.PeerUser{UserID: 10},
			Message: "no entities",
			Date:    1700000300,
		}},
	}}); err != nil {
		t.Fatalf("handle container: %v", err)
	}
	if len(*messages) != 1 {
		t.Fatalf("converted %d messages, want 1", len(*messages))
	}
	m := (*messages)[0]
	if m.ChatJID != "10" || m.ChatName != "10" {
		t.Fatalf("message without entities: %#v", m)
	}
}

func TestWatchHandlerExpandsShortMessages(t *testing.T) {
	ctx := context.Background()
	h, messages, _ := newTestWatchHandler(1)

	if err := h.Handle(ctx, &tg.UpdateShortMessage{
		ID:      9,
		UserID:  10,
		Message: "hi",
		Date:    1700000400,
		Out:     true,
	}); err != nil {
		t.Fatalf("handle short message: %v", err)
	}
	if len(*messages) != 1 {
		t.Fatalf("converted %d messages, want 1", len(*messages))
	}
	m := (*messages)[0]
	if m.ChatJID != "10" || m.Text != "hi" || m.MessageID != "9" || !m.FromMe {
		t.Fatalf("short message: %#v", m)
	}

	if err := h.Handle(ctx, &tg.UpdateShortChatMessage{
		ID:      12,
		ChatID:  77,
		FromID:  10,
		Message: "group hi",
		Date:    1700000500,
	}); err != nil {
		t.Fatalf("handle short chat message: %v", err)
	}
	if len(*messages) != 2 {
		t.Fatalf("converted %d messages, want 2", len(*messages))
	}
	m = (*messages)[1]
	if m.ChatJID != "-77" || m.Text != "group hi" {
		t.Fatalf("short chat message: %#v", m)
	}
}

func TestWatchLiveReconcileMergeIdempotent(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "telecrawl.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }()

	// Live batch: message arrives with no entities, so names degrade to ids.
	liveH := &watchHandler{
		selfID:   1,
		liveOpts: ImportOptions{},
		stats: store.ImportStats{
			SourcePath:          "/data/tdata",
			SourcePathCanonical: true,
			SourceIdentity:      sourceIdentity("tdata", "1"),
		},
		hooks: WatchHooks{
			Live: func(ctx context.Context, st *store.Store, stats store.ImportStats, chats []store.Chat, messages []store.Message) error {
				return st.MergeAll(ctx, stats, nil, chats, nil, nil, nil, messages)
			},
		},
	}
	liveH.st = st
	liveH.liveSession = &tdataImportSession{raw: nil, selfID: 1, opts: liveH.liveOpts}
	if err := liveH.Handle(ctx, &tg.Updates{Updates: []tg.UpdateClass{
		&tg.UpdateNewMessage{Message: &tg.Message{
			ID:      7,
			PeerID:  &tg.PeerUser{UserID: 10},
			Message: "hello alice",
			Date:    1700000200,
		}},
	}}); err != nil {
		t.Fatalf("live handle: %v", err)
	}

	liveRows, err := st.Messages(ctx, store.MessageFilter{ChatJID: "10", Limit: 10})
	if err != nil {
		t.Fatalf("read live messages: %v", err)
	}
	if len(liveRows) != 1 {
		t.Fatalf("archive holds %d live rows for chat 10, want 1", len(liveRows))
	}
	if liveRows[0].ChatName != "10" {
		t.Fatalf("live row should degrade names to ids before reconcile: %#v", liveRows[0])
	}

	// Reconcile batch: the full pass knows the chat and sender names.
	reconcileMsg := liveRows[0]
	reconcileMsg.ChatName = "Alice"
	reconcileMsg.SenderName = "Alice"
	ts := time.Unix(1700000200, 0).UTC()
	if err := st.MergeAll(ctx, liveH.stats, nil, []store.Chat{{
		JID: "10", Kind: "user", Name: "Alice", LastMessageAt: ts,
	}}, nil, nil, nil, []store.Message{reconcileMsg}); err != nil {
		t.Fatalf("reconcile merge: %v", err)
	}

	rows, err := st.Messages(ctx, store.MessageFilter{ChatJID: "10", Limit: 10})
	if err != nil {
		t.Fatalf("read messages: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("archive holds %d rows for chat 10, want 1 (live + reconcile must not duplicate)", len(rows))
	}
	if rows[0].ChatName != "Alice" || rows[0].SenderName != "Alice" {
		t.Fatalf("reconcile did not enrich names: %#v", rows[0])
	}
}

func TestWatchLockGuardsImport(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "telecrawl.db")

	release, err := acquireWatchLock(dbPath)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if !WatchLockHeld(dbPath) {
		t.Fatal("held lock not detected")
	}
	if _, err := Import(ctx, ImportOptions{Path: "/nonexistent/tdata"}, dbPath); err == nil || !strings.Contains(err.Error(), "watch is running") {
		t.Fatalf("import under watch: err = %v, want watch guard error", err)
	}
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	if WatchLockHeld(dbPath) {
		t.Fatal("released lock still reported held")
	}
}
