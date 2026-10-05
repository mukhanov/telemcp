package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/mukhanov/telemcp/internal/archive"
	"github.com/mukhanov/telemcp/internal/localfile"
	"github.com/mukhanov/telemcp/internal/telegram"
)

// mediaListLimit caps how many recent media messages download_media lists
// when no explicit message id is given; the jsonschema documents the cap.
const mediaListLimit = 100

// controlResponseTimeout bounds one control-socket media request: big files
// download for minutes, so the cap is generous. The caller's own context
// deadline wins when it is sooner.
const controlResponseTimeout = 30 * time.Minute

// registerDownloadMedia wires the download_media tool. It runs from Register
// and shares the exclusions closure, so excluded chats never surface here.
func registerDownloadMedia(server *mcp.Server, db *archive.DB, exclusions func() []string) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "download_media",
		Description: "Download a message's media — or a chat's recent media — into a local folder: archived files are copied instantly, the rest are fetched over Telegram (watch daemon socket, else a short-lived tdata session under the sync lock).",
	}, func(ctx context.Context, req *mcp.CallToolRequest, args downloadMediaArgs) (*mcp.CallToolResult, *DownloadResult, error) {
		return runDownloadMedia(ctx, db, exclusions(), args)
	})
}

type downloadMediaArgs struct {
	Chat    string   `json:"chat" jsonschema:"chat id or exact name, as shown by list_chats"`
	Message string   `json:"message,omitempty" jsonschema:"single message id; omit to fetch the chat's recent media"`
	Dest    string   `json:"dest,omitempty" jsonschema:"destination directory; default ~/Downloads/telemcp/<chat>"`
	Limit   int      `json:"limit,omitempty" jsonschema:"max media messages when message is omitted (default 20, max 100)"`
	Types   []string `json:"types,omitempty" jsonschema:"media type filter: photo, document, webpage, voice, video, audio"`
	MaxMB   int64    `json:"max_mb,omitempty" jsonschema:"per-file size cap for Telegram downloads; 0 = unlimited"`
}

// runDownloadMedia implements download_media: entries whose archived file
// still exists are copied to dest; the rest are fetched over Telegram (the
// watch daemon's control socket first, then a direct tdata session). The
// result preserves archive order and always carries a files array.
func runDownloadMedia(ctx context.Context, db *archive.DB, excluded []string, args downloadMediaArgs) (*mcp.CallToolResult, *DownloadResult, error) {
	if args.Chat == "" {
		return nil, nil, errors.New("chat is required")
	}
	// Mirror the CLI's --fetch-media-max-mb guard: reject negatives and
	// values whose MB-to-bytes conversion would overflow.
	if args.MaxMB < 0 || args.MaxMB > math.MaxInt64/(1024*1024) {
		return nil, nil, fmt.Errorf("max_mb must be between 0 and %d", math.MaxInt64/(1024*1024))
	}
	limit := args.Limit
	if limit > mediaListLimit {
		limit = mediaListLimit
	}

	var entries []archive.MediaFile
	if args.Message != "" {
		entry, err := db.MediaMessage(ctx, args.Chat, args.Message, excluded...)
		if err != nil {
			if strings.Contains(err.Error(), "not found in chat") {
				return &mcp.CallToolResult{}, &DownloadResult{Files: []DownloadedMedia{{
					Chat:    args.Chat,
					Message: args.Message,
					Status:  "not_found",
					Detail:  err.Error(),
				}}}, nil
			}
			return nil, nil, err
		}
		entries = []archive.MediaFile{*entry}
	} else {
		var err error
		entries, err = db.MediaMessages(ctx, args.Chat, args.Types, limit, excluded...)
		if err != nil {
			return nil, nil, err
		}
		if len(entries) == 0 {
			return &mcp.CallToolResult{}, &DownloadResult{Files: []DownloadedMedia{}}, nil
		}
	}

	dest := args.Dest
	if dest == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, nil, err
		}
		id, name, err := db.ChatRef(ctx, args.Chat)
		if err != nil {
			return nil, nil, err
		}
		dest = filepath.Join(home, "Downloads", "telemcp", safeChatDirName(name, id))
	} else if filepath.Clean(dest) != dest || !filepath.IsAbs(dest) {
		return nil, nil, fmt.Errorf("dest must be an absolute clean path, got %q", args.Dest)
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create destination directory: %w", err)
	}

	files := make([]DownloadedMedia, len(entries))
	remoteIdx := make([]int, 0, len(entries))
	remoteIDs := make([]int, 0, len(entries))
	mediaRoot := filepath.Join(filepath.Dir(db.Path()), "media")

	for i, entry := range entries {
		files[i] = DownloadedMedia{
			Chat:    entry.Chat,
			Message: entry.MessageID,
			Status:  "no_media",
			Title:   entry.MediaTitle,
		}
		if entry.MediaType == "" {
			continue // message without media: reported as no_media
		}
		id, err := strconv.Atoi(entry.MessageID)
		if err != nil {
			files[i].Status = "error"
			files[i].Detail = fmt.Sprintf("invalid message id %q", entry.MessageID)
			continue
		}
		if entry.MediaPath != "" {
			if src, err := localfile.OpenRegular(mediaRoot, entry.MediaPath); err == nil {
				outPath := filepath.Join(dest, downloadTargetName(entry))
				size, copyErr := copyMediaFile(src, outPath)
				_ = src.Close()
				if copyErr == nil {
					files[i].Status = "archived"
					files[i].Path = outPath
					files[i].Size = size
					continue
				}
			}
			// The archived file is missing or unreadable: retry over Telegram.
		}
		remoteIdx = append(remoteIdx, i)
		remoteIDs = append(remoteIDs, id)
	}

	if len(remoteIDs) > 0 {
		results, err := remoteMediaFetch(ctx, db.Path(), entries[0].Chat, remoteIDs, dest, args.MaxMB)
		if err != nil {
			for _, i := range remoteIdx {
				files[i].Status = "error"
				files[i].Detail = err.Error()
			}
		} else {
			byID := make(map[int]telegram.MediaDownload, len(results))
			for _, r := range results {
				byID[r.MessageID] = r
			}
			for j, i := range remoteIdx {
				r, ok := byID[remoteIDs[j]]
				if !ok {
					files[i].Status = "error"
					files[i].Detail = fmt.Sprintf("no result for message %d", remoteIDs[j])
					continue
				}
				files[i].Status = r.Status
				files[i].Path = r.Path
				files[i].Size = r.Size
				files[i].Detail = r.Detail
				if r.Title != "" {
					files[i].Title = r.Title
				}
			}
		}
	}

	return &mcp.CallToolResult{}, &DownloadResult{Files: files}, nil
}

// DownloadedMedia reports the outcome for one message's media.
type DownloadedMedia struct {
	Chat    string `json:"chat"`
	Message string `json:"message"`
	Status  string `json:"status"`
	Path    string `json:"path,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Title   string `json:"title,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

// DownloadResult wraps download_media output. Files is always an array so
// clients never see a JSON null.
type DownloadResult struct {
	Files []DownloadedMedia `json:"files"`
}

// copyMediaFile streams src into dstPath and reports the copied byte count.
// A partial destination file is removed on failure.
func copyMediaFile(src *os.File, dstPath string) (int64, error) {
	dst, err := os.OpenFile(dstPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, err
	}
	size, err := io.Copy(dst, src)
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(dstPath)
		return 0, err
	}
	return size, nil
}

// downloadTargetName builds the destination file name for an archived media
// entry, mirroring the live downloader's names: "<msgID>_<title><ext>". A
// non-numeric message id cannot drive the numeric prefix, so its sanitized
// form replaces it.
func downloadTargetName(entry archive.MediaFile) string {
	id, err := strconv.Atoi(entry.MessageID)
	if err == nil {
		return telegram.DownloadFileName(id, entry.MediaType, entry.MediaTitle, "media")
	}
	stem := strings.TrimPrefix(telegram.DownloadFileName(0, entry.MediaType, entry.MediaTitle, "media"), "0_")
	if prefix := sanitizeNameToken(entry.MessageID); prefix != "" {
		return prefix + "_" + stem
	}
	return stem
}

// sanitizeNameToken reduces an id to a single safe path element the same way
// the telegram package reduces media titles: base name, then separators,
// control characters and surrounding whitespace stripped. Degenerate tokens
// (".", "..", empty) become "".
func sanitizeNameToken(id string) string {
	name := filepath.Base(strings.TrimSpace(id))
	var b strings.Builder
	for _, r := range name {
		if r == '/' || r == '\\' || r == os.PathSeparator || unicode.IsControl(r) || r == 0x7f {
			continue
		}
		b.WriteRune(r)
	}
	sanitized := b.String()
	if sanitized == "." || sanitized == ".." {
		return ""
	}
	return sanitized
}

// safeChatDirName reduces a chat display name to a single directory name for
// the default destination: ASCII letters, digits, dot, underscore and hyphen
// survive; anything else becomes "_". A name that reduces to nothing falls
// back to the chat id.
func safeChatDirName(name, chatID string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return chatID
	}
	return b.String()
}

// daemonRejectedError marks a request the daemon received and refused
// (ok:false). Unlike transport failures it must not trigger the direct
// fallback: the daemon already answered, and the lock error from a fallback
// attempt would only mask its verdict.
type daemonRejectedError struct{ msg string }

func (e *daemonRejectedError) Error() string { return "daemon download failed: " + e.msg }

// remoteMediaFetch downloads the given messages' media over Telegram: the
// watch daemon's control socket first, then a direct ephemeral tdata
// connection under the archive connection lock. It is a package var so tests
// can stub the Telegram side out.
var remoteMediaFetch = func(ctx context.Context, dbPath, chatID string, msgIDs []int, dest string, maxMB int64) ([]telegram.MediaDownload, error) {
	files, err := fetchViaControlSocket(ctx, dbPath, chatID, msgIDs, dest, maxMB)
	if err == nil {
		return files, nil
	}
	var rejected *daemonRejectedError
	if errors.As(err, &rejected) || ctx.Err() != nil {
		return nil, err
	}
	// No daemon (or the control socket failed): open our own ephemeral
	// connection under the lock the sync commands use.
	release, err := telegram.AcquireConnectionLock(dbPath)
	if err != nil {
		return nil, err // the watch daemon holds the lock
	}
	defer func() { _ = release() }()
	return telegram.DownloadViaTData(ctx, os.Getenv("TELEMCP_SOURCE"), chatID, msgIDs, telegram.DownloadOptions{
		Dest:     dest,
		MaxBytes: maxMB * 1024 * 1024,
	})
}

// downloadMediaRequest is the fixed wire format of the daemon control
// socket's download_media request line.
type downloadMediaRequest struct {
	Cmd      string `json:"cmd"`
	Chat     string `json:"chat"`
	Messages []int  `json:"messages"`
	Dest     string `json:"dest"`
	MaxMB    int64  `json:"max_mb"`
}

// downloadMediaResponse is the fixed wire format of the daemon's reply:
// {"ok":true,"files":[...]} or {"ok":false,"error":"..."}.
type downloadMediaResponse struct {
	OK    bool                     `json:"ok"`
	Files []telegram.MediaDownload `json:"files"`
	Error string                   `json:"error"`
}

// fetchViaControlSocket performs one download_media exchange on the watch
// daemon's control socket: write the newline-terminated request, read the
// one-line response. Any failure (absent socket, protocol garbage, daemon
// rejection) is reported so the caller can fall back to a direct session.
func fetchViaControlSocket(ctx context.Context, dbPath, chatID string, msgIDs []int, dest string, maxMB int64) ([]telegram.MediaDownload, error) {
	conn, err := net.DialTimeout("unix", telegram.ControlSocketPath(dbPath), 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	deadline := time.Now().Add(controlResponseTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)
	abort := make(chan struct{})
	defer close(abort)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-abort:
		}
	}()

	line, err := json.Marshal(downloadMediaRequest{
		Cmd:      "download_media",
		Chat:     chatID,
		Messages: msgIDs,
		Dest:     dest,
		MaxMB:    maxMB,
	})
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(append(line, '\n')); err != nil {
		return nil, err
	}
	// One request per connection: the daemon replies with a single line and
	// closes, so EOF right after the line is the normal end.
	respLine, readErr := bufio.NewReader(conn).ReadBytes('\n')
	if len(respLine) == 0 {
		return nil, readErr
	}
	var resp downloadMediaResponse
	if err := json.Unmarshal(respLine, &resp); err != nil {
		return nil, err
	}
	if !resp.OK {
		return nil, &daemonRejectedError{msg: resp.Error}
	}
	return resp.Files, nil
}
