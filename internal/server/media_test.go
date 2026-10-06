package server

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	_ "modernc.org/sqlite"

	"github.com/mukhanov/telemcp/internal/archive"
	"github.com/mukhanov/telemcp/internal/config"
	"github.com/mukhanov/telemcp/internal/telegram"
)

// mediaFixtureBase is a fixed recent instant so fixture rows stay newest-first.
var mediaFixtureBase = time.Date(2026, 3, 4, 12, 0, 0, 0, time.UTC)

// mediaFixture builds a real archive database plus media root:
//   - chat "-100123" ("Work Chat"): message 102 carries a document whose
//     archived file is missing from disk, message 101 a photo whose file
//     exists, message 103 no media at all;
//   - chat "555000" (empty name): message 601 a voice note whose file exists.
func mediaFixture(t *testing.T) (*archive.DB, string, string) {
	t.Helper()
	// A short directory: the control-socket tests bind watch.sock next to the
	// archive, and macOS rejects unix paths longer than 104 bytes.
	dir, err := os.MkdirTemp("", "tmcpt")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	dbPath := filepath.Join(dir, "telemcp.db")
	sqlDB, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sqlDB.Close() })
	for _, stmt := range []string{
		`CREATE TABLE chats (id TEXT PRIMARY KEY, kind TEXT, name TEXT, username TEXT, last_message_at INTEGER, unread_count INTEGER DEFAULT 0, message_count INTEGER DEFAULT 0, folder_id TEXT, forum INTEGER DEFAULT 0, deleted_at INTEGER, deletion_source TEXT, deletion_reason TEXT)`,
		`CREATE TABLE messages (rowid INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, source_pk INTEGER, chat_jid TEXT, chat_name TEXT, msg_id TEXT, sender_jid TEXT, sender_name TEXT, ts INTEGER NOT NULL, from_me INTEGER, text TEXT, raw_type INTEGER, message_type TEXT, media_type TEXT, media_title TEXT, media_path TEXT, media_url TEXT, media_size INTEGER, starred INTEGER, topic_id TEXT, deleted_at INTEGER, deletion_source TEXT, deletion_reason TEXT)`,
	} {
		if _, err := sqlDB.Exec(stmt); err != nil {
			t.Fatalf("fixture schema: %v", err)
		}
	}

	mediaRoot := filepath.Join(dir, "media")
	writeMedia := func(rel, content string) string {
		t.Helper()
		path := filepath.Join(mediaRoot, rel)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return path
	}
	photo := writeMedia(filepath.Join("aa", "101.jpg"), "jpeg-bytes-101")
	voice := writeMedia(filepath.Join("vo", "601.oga"), "ogg-bytes-601")
	missing := filepath.Join(mediaRoot, "bb", "102.pdf") // referenced but absent

	base := mediaFixtureBase.Unix()
	exec := func(q string, args ...any) {
		t.Helper()
		if _, err := sqlDB.Exec(q, args...); err != nil {
			t.Fatalf("fixture data (%s): %v", q, err)
		}
	}
	exec(`INSERT INTO chats (id, kind, name, username, last_message_at) VALUES (?,?,?,?,?)`,
		"-100123", "group", "Work Chat", "workchat", base+300)
	exec(`INSERT INTO chats (id, kind, name, username, last_message_at) VALUES (?,?,?,?,?)`,
		"555000", "user", "", "noname", base+100)
	media := []struct {
		chat, name, msgID, mediaType, title, path string
		size                                      int64
	}{
		{"-100123", "Work Chat", "102", "document", "budget.pdf", missing, 34000},
		{"-100123", "Work Chat", "101", "photo", "Фото отчёта", photo, 14},
		{"-100123", "Work Chat", "103", "", "", "", 0},
		{"555000", "", "601", "voice", "voice.oga", voice, 13},
	}
	for i, m := range media {
		exec(`INSERT INTO messages (event_id, chat_jid, chat_name, msg_id, ts, from_me, text, media_type, media_title, media_path, media_size)
			VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
			fmt.Sprintf("evt-%d", i), m.chat, m.name, m.msgID, base+200-30*int64(i), 0, "", m.mediaType, m.title, m.path, m.size)
	}

	db, err := archive.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mediaRoot, dir
}

// mediaSession registers the server tools over an in-memory transport and
// returns a client session for calling them.
func mediaSession(t *testing.T, db *archive.DB, configPath string) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	srv := mcp.NewServer(&mcp.Implementation{Name: "telemcp-test", Version: "test"}, nil)
	Register(srv, db, configPath)
	clientTransport, serverTransport := mcp.NewInMemoryTransports()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, serverTransport) }()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "test"}, nil)
	cs, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cs.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
		}
	})
	return cs
}

// callDownloadMedia invokes the tool and fails the test on transport errors.
func callDownloadMedia(t *testing.T, cs *mcp.ClientSession, args map[string]any) (*mcp.CallToolResult, error) {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: "download_media", Arguments: args})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	return res, nil
}

// decodeDownloadResult extracts the typed structured output, tolerating both
// a typed pointer and a decoded raw value for StructuredContent.
func decodeDownloadResult(t *testing.T, res *mcp.CallToolResult) *DownloadResult {
	t.Helper()
	if res.StructuredContent == nil {
		t.Fatalf("no structured content in %+v", res)
	}
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatalf("marshal structured content: %v", err)
	}
	var out DownloadResult
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode structured content %s: %v", raw, err)
	}
	return &out
}

// toolErrorText expects a failed call and returns the error text, whether the
// SDK surfaced it as a transport error or an IsError result.
func toolErrorText(t *testing.T, res *mcp.CallToolResult, err error) string {
	t.Helper()
	if err != nil {
		return err.Error()
	}
	if res == nil || !res.IsError {
		t.Fatalf("expected tool error, got %+v", res)
	}
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			return tc.Text
		}
	}
	return ""
}

// remoteCall records one remoteMediaFetch invocation.
type remoteCall struct {
	chatID string
	msgIDs []int
	dest   string
	maxMB  int64
}

// stubRemoteMediaFetch replaces remoteMediaFetch with a canned reply and
// restores it at cleanup. calls, when non-nil, records invocations.
func stubRemoteMediaFetch(t *testing.T, files []telegram.MediaDownload, err error, calls *[]remoteCall) {
	t.Helper()
	prev := remoteMediaFetch
	remoteMediaFetch = func(ctx context.Context, dbPath, chatID string, msgIDs []int, dest string, maxMB int64) ([]telegram.MediaDownload, error) {
		if calls != nil {
			*calls = append(*calls, remoteCall{chatID: chatID, msgIDs: append([]int(nil), msgIDs...), dest: dest, maxMB: maxMB})
		}
		return files, err
	}
	t.Cleanup(func() { remoteMediaFetch = prev })
}

// forbidRemoteFetch fails the test if the tool goes near Telegram.
func forbidRemoteFetch(t *testing.T) {
	t.Helper()
	prev := remoteMediaFetch
	remoteMediaFetch = func(ctx context.Context, dbPath, chatID string, msgIDs []int, dest string, maxMB int64) ([]telegram.MediaDownload, error) {
		t.Error("unexpected remote media fetch")
		return nil, errors.New("unexpected remote media fetch")
	}
	t.Cleanup(func() { remoteMediaFetch = prev })
}

// useRealRemoteFetch points remoteMediaFetch back at the genuine
// implementation (captured before any stub runs).
func useRealRemoteFetch(t *testing.T) {
	t.Helper()
	remoteMediaFetch = remoteMediaFetchImpl
}

func TestDownloadMediaArchiveCopyAndRemote(t *testing.T) {
	db, _, dir := mediaFixture(t)
	dest := filepath.Join(t.TempDir(), "out")
	cs := mediaSession(t, db, filepath.Join(dir, "config.json"))

	var calls []remoteCall
	stubRemoteMediaFetch(t, []telegram.MediaDownload{
		{ChatID: "-100123", MessageID: 102, Status: "downloaded", Path: filepath.Join(dest, "102_budget.pdf"), Size: 34000, Title: "budget.pdf"},
	}, nil, &calls)

	res, err := callDownloadMedia(t, cs, map[string]any{"chat": "Work Chat", "dest": dest})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("tool error: %s", toolErrorText(t, res, nil))
	}
	out := decodeDownloadResult(t, res)
	if len(out.Files) != 2 {
		t.Fatalf("files = %+v, want 2 entries", out.Files)
	}
	// Newest first: the remote result merges into 102's slot, 101 is copied
	// from the archive.
	got102, got101 := out.Files[0], out.Files[1]
	if got102.Chat != "-100123" || got102.Message != "102" || got102.Status != "downloaded" ||
		got102.Path != filepath.Join(dest, "102_budget.pdf") || got102.Size != 34000 || got102.Detail != "" {
		t.Fatalf("remote entry = %+v", got102)
	}
	wantPath := filepath.Join(dest, telegram.DownloadFileName(101, "photo", "Фото отчёта", "media"))
	if got101.Chat != "-100123" || got101.Message != "101" || got101.Status != "archived" ||
		got101.Path != wantPath || got101.Size != int64(len("jpeg-bytes-101")) {
		t.Fatalf("archived entry = %+v, want path %s", got101, wantPath)
	}
	if data, err := os.ReadFile(got101.Path); err != nil || string(data) != "jpeg-bytes-101" {
		t.Fatalf("copied file content = %q, err = %v", data, err)
	}
	if len(calls) != 1 {
		t.Fatalf("remote calls = %+v, want 1", calls)
	}
	if calls[0].chatID != "-100123" || calls[0].dest != dest || calls[0].maxMB != 0 ||
		!reflect.DeepEqual(calls[0].msgIDs, []int{102}) {
		t.Fatalf("remote call = %+v", calls[0])
	}
}

func TestDownloadMediaFilesAlwaysArray(t *testing.T) {
	db, _, dir := mediaFixture(t)
	cs := mediaSession(t, db, filepath.Join(dir, "config.json"))
	forbidRemoteFetch(t)

	// A chat filter matching nothing still yields a JSON array, not null.
	res, err := callDownloadMedia(t, cs, map[string]any{"chat": "Work Chat", "types": []string{"video"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("tool error: %s", toolErrorText(t, res, nil))
	}
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"files":[]`) {
		t.Fatalf("structured content = %s, want empty files array", raw)
	}
}

func TestDownloadMediaSingleMessage(t *testing.T) {
	db, _, dir := mediaFixture(t)
	cs := mediaSession(t, db, filepath.Join(dir, "config.json"))
	forbidRemoteFetch(t)

	res, err := callDownloadMedia(t, cs, map[string]any{"chat": "Work Chat", "message": "999"})
	if err != nil {
		t.Fatal(err)
	}
	out := decodeDownloadResult(t, res)
	if len(out.Files) != 1 || out.Files[0].Chat != "Work Chat" || out.Files[0].Message != "999" ||
		out.Files[0].Status != "not_found" || out.Files[0].Detail == "" {
		t.Fatalf("not-found entry = %+v", out.Files)
	}

	// An existing message without media reports no_media, without any
	// Telegram round-trip.
	res, err = callDownloadMedia(t, cs, map[string]any{"chat": "-100123", "message": "103", "dest": filepath.Join(t.TempDir(), "d")})
	if err != nil {
		t.Fatal(err)
	}
	out = decodeDownloadResult(t, res)
	if len(out.Files) != 1 || out.Files[0].Message != "103" || out.Files[0].Status != "no_media" {
		t.Fatalf("no-media entry = %+v", out.Files)
	}
}

func TestDownloadMediaDestDefault(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	db, _, dir := mediaFixture(t)
	cs := mediaSession(t, db, filepath.Join(dir, "config.json"))
	forbidRemoteFetch(t)

	res, err := callDownloadMedia(t, cs, map[string]any{"chat": "Work Chat", "message": "101"})
	if err != nil {
		t.Fatal(err)
	}
	out := decodeDownloadResult(t, res)
	if len(out.Files) != 1 || out.Files[0].Status != "archived" {
		t.Fatalf("files = %+v", out.Files)
	}
	wantDir := filepath.Join(home, "Downloads", "telemcp", "Work_Chat")
	if out.Files[0].Path != filepath.Join(wantDir, telegram.DownloadFileName(101, "photo", "Фото отчёта", "media")) {
		t.Fatalf("archived path = %s, want under %s", out.Files[0].Path, wantDir)
	}
	if info, err := os.Stat(wantDir); err != nil || !info.IsDir() {
		t.Fatalf("default dest dir: %v", err)
	}

	// A chat whose name reduces to nothing falls back to its id.
	res, err = callDownloadMedia(t, cs, map[string]any{"chat": "555000", "message": "601"})
	if err != nil {
		t.Fatal(err)
	}
	out = decodeDownloadResult(t, res)
	if len(out.Files) != 1 || out.Files[0].Status != "archived" {
		t.Fatalf("files = %+v", out.Files)
	}
	if want := filepath.Join(home, "Downloads", "telemcp", "555000"); filepath.Dir(out.Files[0].Path) != want {
		t.Fatalf("archived path = %s, want under %s", out.Files[0].Path, want)
	}
}

func TestSafeChatDirName(t *testing.T) {
	for _, tt := range []struct{ name, id, want string }{
		{"Work Chat", "-100123", "Work_Chat"},
		{"Иван Петров", "777", "___________"},
		{"a/b\\c:d", "5", "a_b_c_d"},
		{"emoji😀x", "1", "emoji_x"},
		{"", "555000", "555000"},
		{"ok.name-2", "9", "ok.name-2"},
	} {
		if got := safeChatDirName(tt.name, tt.id); got != tt.want {
			t.Errorf("safeChatDirName(%q, %q) = %q, want %q", tt.name, tt.id, got, tt.want)
		}
	}
}

func TestDownloadMediaDestValidation(t *testing.T) {
	db, _, dir := mediaFixture(t)
	cs := mediaSession(t, db, filepath.Join(dir, "config.json"))
	forbidRemoteFetch(t)

	for _, dest := range []string{"relative/out", filepath.Join(t.TempDir(), "x") + "/../out"} {
		res, err := callDownloadMedia(t, cs, map[string]any{"chat": "Work Chat", "message": "101", "dest": dest})
		if err != nil {
			t.Fatal(err)
		}
		if text := toolErrorText(t, res, nil); !strings.Contains(text, "absolute") {
			t.Errorf("dest %q: error = %q, want absolute-path complaint", dest, text)
		}
	}
}

func TestDownloadMediaExcludedChat(t *testing.T) {
	db, _, dir := mediaFixture(t)
	configPath := filepath.Join(dir, "config.json")
	var cfg config.Config
	if _, err := cfg.Exclude("-100123", "Work Chat", "test"); err != nil {
		t.Fatal(err)
	}
	if err := config.Save(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	cs := mediaSession(t, db, configPath)
	forbidRemoteFetch(t)

	for _, args := range []map[string]any{
		{"chat": "Work Chat"},
		{"chat": "-100123", "message": "101"},
	} {
		res, err := callDownloadMedia(t, cs, args)
		if err != nil {
			t.Fatal(err)
		}
		if text := toolErrorText(t, res, nil); !strings.Contains(text, "excluded") {
			t.Errorf("args %v: error = %q, want exclusion notice", args, text)
		}
	}
}

func TestDownloadMediaMaxMBValidation(t *testing.T) {
	db, _, dir := mediaFixture(t)
	cs := mediaSession(t, db, filepath.Join(dir, "config.json"))
	forbidRemoteFetch(t)

	for _, maxMB := range []int64{-1, math.MaxInt64/(1024*1024) + 1} {
		res, err := callDownloadMedia(t, cs, map[string]any{"chat": "Work Chat", "max_mb": maxMB})
		if err != nil {
			t.Fatal(err)
		}
		if text := toolErrorText(t, res, nil); !strings.Contains(text, "between 0 and") {
			t.Errorf("max_mb %d: error = %q, want range complaint", maxMB, text)
		}
	}
}

func TestDownloadMediaRemoteError(t *testing.T) {
	db, _, dir := mediaFixture(t)
	dest := filepath.Join(t.TempDir(), "out")
	cs := mediaSession(t, db, filepath.Join(dir, "config.json"))

	wantErr := errors.New("telemcp watch is already running for this archive (watch.lock is held); stop it first")
	stubRemoteMediaFetch(t, nil, wantErr, nil)

	res, err := callDownloadMedia(t, cs, map[string]any{"chat": "-100123", "dest": dest, "types": []string{"document"}})
	if err != nil {
		t.Fatal(err)
	}
	out := decodeDownloadResult(t, res)
	if len(out.Files) != 1 || out.Files[0].Message != "102" || out.Files[0].Chat != "-100123" ||
		out.Files[0].Status != "error" || out.Files[0].Detail != wantErr.Error() {
		t.Fatalf("error entry = %+v, want detail %q", out.Files, wantErr)
	}
}

// TestDownloadMediaFallbackLockHeld exercises the production remoteMediaFetch
// with the connection lock pre-held, so no Telegram connection is ever
// attempted. The no-daemon case falls back and lands on the lock error; a
// daemon refusal (ok:false) surfaces the daemon's own verdict instead of
// masking it with the fallback's lock error.
func TestDownloadMediaFallbackLockHeld(t *testing.T) {
	db, _, dir := mediaFixture(t)
	dest := filepath.Join(t.TempDir(), "out")
	release, err := telegram.AcquireConnectionLock(db.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = release() }()
	useRealRemoteFetch(t)

	cs := mediaSession(t, db, filepath.Join(dir, "config.json"))

	// Variant 1: no daemon socket at all — dial fails, fallback takes the
	// lock path.
	res, err := callDownloadMedia(t, cs, map[string]any{"chat": "-100123", "dest": dest, "types": []string{"document"}})
	if err != nil {
		t.Fatal(err)
	}
	out := decodeDownloadResult(t, res)
	if len(out.Files) != 1 || out.Files[0].Message != "102" || out.Files[0].Status != "error" ||
		!strings.Contains(out.Files[0].Detail, "watch.lock") {
		t.Fatalf("no-daemon entry = %+v, want lock error detail", out.Files)
	}

	// Variant 2: daemon present but refusing (ok:false) — its own error wins,
	// the lock fallback never runs.
	serve := serveDaemonSocket(t, db.Path(), nil, &controlRequestRecord{})
	if serve == nil {
		t.Log("daemon socket unavailable, refused variant skipped")
		return
	}
	serve.setRefuse(true)
	res, err = callDownloadMedia(t, cs, map[string]any{"chat": "-100123", "dest": dest, "types": []string{"document"}})
	if err != nil {
		t.Fatal(err)
	}
	out = decodeDownloadResult(t, res)
	if len(out.Files) != 1 || out.Files[0].Status != "error" ||
		!strings.Contains(out.Files[0].Detail, "daemon download failed") ||
		strings.Contains(out.Files[0].Detail, "watch.lock") {
		t.Fatalf("refused entry = %+v, want daemon verdict without lock fallback", out.Files)
	}
}

// TestDownloadMediaDaemonSocket runs the production remoteMediaFetch against
// a hand-rolled daemon on the real control socket: the wire request carries
// the documented fields and the ok:true reply merges into the result.
func TestDownloadMediaDaemonSocket(t *testing.T) {
	db, _, dir := mediaFixture(t)
	dest := filepath.Join(t.TempDir(), "out")
	useRealRemoteFetch(t)

	var request controlRequestRecord
	serve := serveDaemonSocket(t, db.Path(), []telegram.MediaDownload{
		{ChatID: "-100123", MessageID: 102, Status: "downloaded", Path: filepath.Join(dest, "102_budget.pdf"), Size: 9, Title: "budget.pdf"},
	}, &request)
	if serve == nil {
		t.Skip("daemon socket unavailable")
	}

	cs := mediaSession(t, db, filepath.Join(dir, "config.json"))
	res, err := callDownloadMedia(t, cs, map[string]any{"chat": "Work Chat", "dest": dest, "types": []string{"document"}, "max_mb": 2048})
	if err != nil {
		t.Fatal(err)
	}
	out := decodeDownloadResult(t, res)
	if len(out.Files) != 1 || out.Files[0].Message != "102" || out.Files[0].Status != "downloaded" ||
		out.Files[0].Size != 9 || out.Files[0].Path != filepath.Join(dest, "102_budget.pdf") {
		t.Fatalf("merged entry = %+v", out.Files)
	}
	request.mu.Lock()
	cmd, chat, msgs, reqDest, maxMB := request.cmd, request.chat, request.messages, request.dest, request.maxMB
	request.mu.Unlock()
	if cmd != "download_media" || chat != "-100123" ||
		!reflect.DeepEqual(msgs, []int{102}) || reqDest != dest || maxMB != 2048 {
		t.Fatalf("daemon request = cmd=%s chat=%s messages=%v dest=%s maxMB=%d", cmd, chat, msgs, reqDest, maxMB)
	}
}

// controlRequestRecord collects what the fake daemon received.
type controlRequestRecord struct {
	mu              sync.Mutex
	cmd, chat, dest string
	messages        []int
	maxMB           int64
}

// daemonSocket is the handle for the fake daemon started by serveDaemonSocket.
type daemonSocket struct {
	mu     sync.Mutex
	refuse bool // reply ok:false instead of ok:true
}

func (d *daemonSocket) setRefuse(v bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.refuse = v
}

func (d *daemonSocket) refusing() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.refuse
}

// serveDaemonSocket listens on ControlSocketPath(dbPath) and answers exactly
// one request per connection, per the wire contract: ok:true with files when
// given, ok:false otherwise. A nil return means the socket could not be
// served.
func serveDaemonSocket(t *testing.T, dbPath string, files []telegram.MediaDownload, record *controlRequestRecord) *daemonSocket {
	t.Helper()
	sock := telegram.ControlSocketPath(dbPath)
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Logf("serve daemon socket: %v", err)
		return nil
	}
	t.Cleanup(func() { _ = ln.Close() })
	handle := &daemonSocket{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				line, err := bufio.NewReader(conn).ReadBytes('\n')
				if err != nil && len(line) == 0 {
					return
				}
				var req struct {
					Cmd      string `json:"cmd"`
					Chat     string `json:"chat"`
					Messages []int  `json:"messages"`
					Dest     string `json:"dest"`
					MaxMB    int64  `json:"max_mb"`
				}
				if json.Unmarshal(line, &req) != nil {
					return
				}
				record.mu.Lock()
				record.cmd, record.chat, record.dest, record.maxMB = req.Cmd, req.Chat, req.Dest, req.MaxMB
				record.messages = req.Messages
				record.mu.Unlock()
				resp := map[string]any{"ok": true, "files": files}
				if handle.refusing() {
					resp = map[string]any{"ok": false, "error": "daemon download failed"}
				}
				out, _ := json.Marshal(resp)
				_, _ = conn.Write(append(out, '\n'))
			}()
		}
	}()
	return handle
}
