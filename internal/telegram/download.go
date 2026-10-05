package telegram

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"unicode"

	"github.com/gotd/td/session"
	"github.com/gotd/td/session/tdesktop"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/telegram/query/dialogs"
	querymessages "github.com/gotd/td/telegram/query/messages"
	"github.com/gotd/td/tg"
)

// MediaDownload reports the outcome of downloading one message's media.
type MediaDownload struct {
	ChatID    string `json:"chat_id"`
	MessageID int    `json:"message"`
	Status    string `json:"status"` // downloaded | archived | no_media | not_found | too_large | timeout | error
	Path      string `json:"path,omitempty"`
	Size      int64  `json:"size,omitempty"`
	Title     string `json:"title,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

// DownloadOptions bounds a media download batch.
type DownloadOptions struct {
	// Dest is the absolute target directory; FetchChatMedia creates it (0o755).
	Dest string
	// MaxBytes is the per-file declared-size cap; 0 = unlimited.
	MaxBytes int64
}

// mediaDialogsPager enumerates dialogs and stops early when the callback
// returns an error. The default implementation wraps the gotd dialogs query.
type mediaDialogsPager func(ctx context.Context, fn func(ctx context.Context, elem dialogs.Elem) error) error

// mediaMessagesFetcher fetches a batch of messages by id for an already
// resolved peer.
type mediaMessagesFetcher func(ctx context.Context, peer tg.InputPeerClass, ids []tg.InputMessageClass) (tg.MessagesMessagesClass, error)

// PeerHashStore resolves access hashes persisted next to the archive: chats
// whose dialog left the dialog list (a deleted supergroup dialog, an archived
// chat) still deliver updates, and the watch daemon records their hashes in
// the updates state tables.
type PeerHashStore interface {
	ChannelAccessHash(ctx context.Context, channelID int64) (int64, bool, error)
	UserAccessHash(ctx context.Context, userID int64) (int64, bool, error)
}

// MediaFetcher downloads message media over a live Telegram connection.
// Peers are resolved from the account's dialog list (main and archived
// folders) and cached per chat id, so repeated downloads skip the dialog
// scan. A PeerHashStore, when set, answers before the scan.
type MediaFetcher struct {
	raw    *tg.Client
	selfID int64

	// Seams for tests; NewMediaFetcher installs the real implementations.
	dialogsPager    mediaDialogsPager
	messagesFetcher mediaMessagesFetcher
	hashes          PeerHashStore

	peerMu   sync.Mutex
	peerByID map[string]tg.InputPeerClass
}

// NewMediaFetcher builds a fetcher over a connected Telegram client.
func NewMediaFetcher(raw *tg.Client, selfID int64) *MediaFetcher {
	f := &MediaFetcher{
		raw:      raw,
		selfID:   selfID,
		peerByID: make(map[string]tg.InputPeerClass),
	}
	f.dialogsPager = f.pageDialogs
	f.messagesFetcher = f.fetchMessages
	return f
}

// NewMediaFetcherWithHashes builds a fetcher that consults persisted access
// hashes before scanning the dialog list. The watch daemon passes its
// updates state store, which also covers chats without a dialog entry.
func NewMediaFetcherWithHashes(raw *tg.Client, selfID int64, hashes PeerHashStore) *MediaFetcher {
	f := NewMediaFetcher(raw, selfID)
	f.hashes = hashes
	return f
}

// pageDialogs iterates the account's dialogs in batches: the main folder and
// the archived folder — Telegram returns only one folder per getDialogs, and
// together they cover every dialog.
func (f *MediaFetcher) pageDialogs(ctx context.Context, fn func(ctx context.Context, elem dialogs.Elem) error) error {
	for _, folderID := range []int{0, 1} {
		if err := query.GetDialogs(f.raw).BatchSize(tdataBatchSize).FolderID(folderID).ForEach(ctx, fn); err != nil {
			return err
		}
	}
	return nil
}

// fetchMessages dispatches by peer kind: channels use channels.getMessages,
// the rest use messages.getMessages.
func (f *MediaFetcher) fetchMessages(ctx context.Context, peer tg.InputPeerClass, ids []tg.InputMessageClass) (tg.MessagesMessagesClass, error) {
	if channel, ok := peer.(*tg.InputPeerChannel); ok {
		return f.raw.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
			Channel: &tg.InputChannel{ChannelID: channel.ChannelID, AccessHash: channel.AccessHash},
			ID:      ids,
		})
	}
	return f.raw.MessagesGetMessages(ctx, ids)
}

// resolvePeer returns the cached input peer for a chat id: persisted access
// hashes answer first, then the dialog list is scanned once on a miss. A nil
// peer (with nil error) means the chat id matches neither.
func (f *MediaFetcher) resolvePeer(ctx context.Context, chatID string) (tg.InputPeerClass, error) {
	f.peerMu.Lock()
	cached, ok := f.peerByID[chatID]
	f.peerMu.Unlock()
	if ok {
		return cached, nil
	}
	if f.hashes != nil {
		if peer, ok, err := f.peerFromHashes(ctx, chatID); err != nil || ok {
			if err == nil && peer != nil {
				f.peerMu.Lock()
				f.peerByID[chatID] = peer
				f.peerMu.Unlock()
			}
			return peer, err
		}
	}
	var matched tg.InputPeerClass
	err := f.dialogsPager(ctx, func(_ context.Context, elem dialogs.Elem) error {
		if elem.Deleted() {
			return nil
		}
		jid := tdataInputPeerIDString(elem.Peer, f.selfID)
		if jid == "" || !tdataChatFilterMatches(jid, chatID) {
			return nil
		}
		matched = elem.Peer
		return errTDataStop
	})
	if err != nil && !errors.Is(err, errTDataStop) {
		return nil, err
	}
	if matched == nil {
		return nil, nil
	}
	f.peerMu.Lock()
	f.peerByID[chatID] = matched
	f.peerMu.Unlock()
	return matched, nil
}

// peerFromHashes builds an input peer from the persisted hash store. ok is
// false when the chat id is not a hash-resolvable form (basic groups carry
// no access hash) or the store has no entry.
func (f *MediaFetcher) peerFromHashes(ctx context.Context, chatID string) (tg.InputPeerClass, bool, error) {
	if rest, ok := strings.CutPrefix(chatID, "-100"); ok {
		channelID, err := strconv.ParseInt(rest, 10, 64)
		if err != nil || channelID <= 0 {
			return nil, false, nil
		}
		hash, ok, err := f.hashes.ChannelAccessHash(ctx, channelID)
		if err != nil || !ok {
			return nil, false, err
		}
		return &tg.InputPeerChannel{ChannelID: channelID, AccessHash: hash}, true, nil
	}
	if userID, err := strconv.ParseInt(chatID, 10, 64); err == nil && userID > 0 {
		hash, ok, err := f.hashes.UserAccessHash(ctx, userID)
		if err != nil || !ok {
			return nil, false, err
		}
		return &tg.InputPeerUser{UserID: userID, AccessHash: hash}, true, nil
	}
	return nil, false, nil
}

// FetchChatMedia downloads the given messages' media into opts.Dest, one
// result per unique message id, in msgIDs order (duplicates deduped).
func (f *MediaFetcher) FetchChatMedia(ctx context.Context, chatID string, msgIDs []int, opts DownloadOptions) ([]MediaDownload, error) {
	chatID = strings.TrimSpace(chatID)
	if chatID == "" {
		return nil, errors.New("chat id is required")
	}
	ordered := dedupeMessageIDs(msgIDs)
	if len(ordered) == 0 {
		return []MediaDownload{}, nil
	}
	if strings.TrimSpace(opts.Dest) == "" {
		return nil, errors.New("destination directory is required")
	}
	if err := os.MkdirAll(opts.Dest, 0o755); err != nil {
		return nil, fmt.Errorf("create destination directory: %w", err)
	}
	peer, err := f.resolvePeer(ctx, chatID)
	if err != nil {
		return nil, fmt.Errorf("resolve chat %s: %w", chatID, err)
	}
	results := make([]MediaDownload, 0, len(ordered))
	if peer == nil {
		for _, id := range ordered {
			results = append(results, MediaDownload{
				ChatID:    chatID,
				MessageID: id,
				Status:    "not_found",
				Detail:    fmt.Sprintf("chat %q not found", chatID),
			})
		}
		return results, nil
	}
	ids := make([]tg.InputMessageClass, 0, len(ordered))
	for _, id := range ordered {
		ids = append(ids, &tg.InputMessageID{ID: id})
	}
	response, err := f.messagesFetcher(ctx, peer, ids)
	if err != nil {
		return nil, fmt.Errorf("fetch messages for chat %s: %w", chatID, err)
	}
	messages := mediaMessagesByID(response)
	for _, id := range ordered {
		results = append(results, f.downloadMessage(ctx, chatID, id, messages[id], opts))
	}
	return results, nil
}

// downloadMessage downloads one message's attachment and reports the outcome.
func (f *MediaFetcher) downloadMessage(ctx context.Context, chatID string, msgID int, msg *tg.Message, opts DownloadOptions) MediaDownload {
	result := MediaDownload{ChatID: chatID, MessageID: msgID}
	if msg == nil {
		result.Status = "not_found"
		result.Detail = fmt.Sprintf("message %d not found in chat %q", msgID, chatID)
		return result
	}
	file, ok := telegramMessageFile(querymessages.Elem{Msg: msg})
	if !ok {
		result.Status = "no_media"
		return result
	}
	title := strings.TrimSpace(file.Name)
	mediaType := tdataMediaType(msg)
	if opts.MaxBytes > 0 {
		if size := telegramMessageMediaSize(msg); size > opts.MaxBytes {
			result.Status = "too_large"
			result.Size = size
			result.Title = title
			return result
		}
	}
	outputPath := filepath.Join(opts.Dest, DownloadFileName(msgID, mediaType, title, "media"))
	size, status := downloadToFile(ctx, f.raw, file, outputPath)
	result.Title = title
	switch status {
	case "":
		if size <= 0 {
			result.Status = "error"
			result.Detail = "downloaded media is empty"
			return result
		}
		result.Status = "downloaded"
		result.Path = outputPath
		result.Size = size
	case "timeout":
		result.Status = "timeout"
	default:
		result.Status = "error"
		result.Detail = "media download failed"
	}
	return result
}

// mediaMessagesByID flattens a messages.getMessages / channels.getMessages
// response into id-keyed normal messages. Service messages and
// messagesMessagesNotModified yield no entries, so missing ids surface as
// not_found at the caller.
func mediaMessagesByID(response tg.MessagesMessagesClass) map[int]*tg.Message {
	var classes []tg.MessageClass
	switch messages := response.(type) {
	case *tg.MessagesMessages:
		classes = messages.Messages
	case *tg.MessagesMessagesSlice:
		classes = messages.Messages
	case *tg.MessagesChannelMessages:
		classes = messages.Messages
	default:
		return map[int]*tg.Message{}
	}
	out := make(map[int]*tg.Message, len(classes))
	for _, class := range classes {
		if msg, ok := class.(*tg.Message); ok && msg != nil {
			out[msg.ID] = msg
		}
	}
	return out
}

// dedupeMessageIDs drops repeated ids, keeping first-occurrence order.
func dedupeMessageIDs(msgIDs []int) []int {
	seen := make(map[int]struct{}, len(msgIDs))
	out := make([]int, 0, len(msgIDs))
	for _, id := range msgIDs {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// downloadMediaExtensionFallbacks maps stored media_type strings to the
// extension appended when a media title carries none.
var downloadMediaExtensionFallbacks = map[string]string{
	"photo":    ".jpg",
	"document": ".bin",
	"webpage":  ".html",
	"voice":    ".oga",
	"video":    ".mp4",
	"audio":    ".mp3",
}

// DownloadFileName builds the destination file name for a downloaded message
// attachment: the sanitized title (base name, separators and control
// characters stripped) prefixed with "<msgID>_", extended with a media-type
// fallback when the title carries no extension. The result never contains a
// separator, so it cannot escape the destination directory.
func DownloadFileName(msgID int, mediaType, title, fallback string) string {
	name := sanitizeDownloadFileName(title)
	if name == "" {
		name = sanitizeDownloadFileName(fallback)
	}
	if name == "" {
		name = "media"
	}
	if ext := filepath.Ext(name); ext == "." {
		name = strings.TrimSuffix(name, ".") + downloadMediaExtensionFallbacks[mediaType]
	} else if ext == "" {
		name += downloadMediaExtensionFallbacks[mediaType]
	}
	return strconv.Itoa(msgID) + "_" + name
}

// sanitizeDownloadFileName reduces a media title to a single safe path
// element: the base name with separators, control characters and surrounding
// whitespace removed. Degenerate names (".", "..", empty) become "".
func sanitizeDownloadFileName(title string) string {
	name := filepath.Base(strings.TrimSpace(title))
	var b strings.Builder
	for _, r := range name {
		if r == '/' || r == '\\' || r == os.PathSeparator || unicode.IsControl(r) || r == 0x7f {
			continue
		}
		b.WriteRune(r)
	}
	if sanitized := b.String(); sanitized == "." || sanitized == ".." {
		return ""
	} else {
		return sanitized
	}
}

// downloadToFile fetches file.Location into outputPath over the given
// connection, returning the written byte count, or 0 plus a status
// ("timeout", "error", "unavailable") on failure.
func downloadToFile(ctx context.Context, raw *tg.Client, file querymessages.File, outputPath string) (int64, string) {
	// Reserve the retry budget so a valid flood wait cannot consume the transfer deadline.
	downloadCtx, cancel := context.WithTimeout(ctx, telegramMediaDownloadTimeout)
	defer cancel()
	if _, err := downloader.NewDownloader().WithAllowCDN(true).Download(raw, file.Location).ToPath(downloadCtx, outputPath); err != nil {
		if errors.Is(downloadCtx.Err(), context.DeadlineExceeded) {
			return 0, "timeout"
		}
		return 0, "error"
	}
	info, err := os.Stat(outputPath)
	if err != nil || info.Size() <= 0 {
		return 0, "unavailable"
	}
	return info.Size(), ""
}

// DownloadViaTData downloads message media over an ephemeral tdata-authorized
// Telegram connection. The caller must hold the archive connection lock. An
// empty sourcePath resolves to the default Telegram Desktop tdata directory.
func DownloadViaTData(ctx context.Context, sourcePath string, chatID string, msgIDs []int, opts DownloadOptions) ([]MediaDownload, error) {
	source := resolveImportSource(sourcePath)
	accounts, err := tdesktop.Read(source.path, nil)
	if err != nil {
		return nil, fmt.Errorf("read Telegram Desktop tdata: %w", err)
	}
	if len(accounts) == 0 {
		return nil, errors.New("no Telegram Desktop accounts found")
	}
	data, err := session.TDesktopSession(accounts[0])
	if err != nil {
		return nil, fmt.Errorf("read Telegram Desktop session: %w", err)
	}
	storage := &session.StorageMemory{}
	if err := (&session.Loader{Storage: storage}).Save(ctx, data); err != nil {
		return nil, fmt.Errorf("store Telegram Desktop session: %w", err)
	}
	client := telegram.NewClient(telegramDesktopAPIID, telegramDesktopAPIHash, telegram.Options{
		SessionStorage: storage,
		NoUpdates:      true,
		AllowCDN:       true,
		Middlewares:    []telegram.Middleware{newTelegramFloodWaitPolicy(nil)},
		Device: telegram.DeviceConfig{
			DeviceModel:    "Desktop",
			SystemVersion:  "Windows 11",
			AppVersion:     "6.5 x64",
			SystemLangCode: "en-US",
			LangPack:       "tdesktop",
			LangCode:       "en",
		},
	})
	var results []MediaDownload
	err = client.Run(ctx, func(ctx context.Context) error {
		self, err := client.Self(ctx)
		if err != nil {
			return fmt.Errorf("telegram session is not authorized: %w", err)
		}
		results, err = NewMediaFetcher(tg.NewClient(client), self.ID).FetchChatMedia(ctx, chatID, msgIDs, opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}
