package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// stubControlFetcher records FetchChatMedia calls and returns canned results.
type stubControlFetcher struct {
	mu     sync.Mutex
	calls  int
	chat   string
	msgIDs []int
	opts   DownloadOptions
	files  []MediaDownload
	err    error
}

func (f *stubControlFetcher) FetchChatMedia(ctx context.Context, chatID string, msgIDs []int, opts DownloadOptions) ([]MediaDownload, error) {
	f.mu.Lock()
	f.calls++
	f.chat = chatID
	f.msgIDs = append([]int(nil), msgIDs...)
	f.opts = opts
	files, err := f.files, f.err
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return files, nil
}

// callCount reports how many times the fetcher ran.
func (f *stubControlFetcher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// newControlArchive creates a temporary archive directory and returns its
// database path. t.TempDir paths exceed the 104-byte Unix socket path limit
// on macOS once the long per-test suffix is included, so the directory is
// created directly under TMPDIR and removed at cleanup.
func newControlArchive(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "tmcpt-")
	if err != nil {
		t.Fatalf("create archive dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "archive.db")
}

// startTestControlSocket serves a control socket for a fresh temp archive and
// registers cleanup. Returns the socket path.
func startTestControlSocket(t *testing.T, fetcher mediaDownloader) string {
	t.Helper()
	dbPath := newControlArchive(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cleanup, err := serveControlSocket(ctx, dbPath, fetcher, func(string, ...any) {})
	if err != nil {
		t.Fatalf("serve control socket: %v", err)
	}
	t.Cleanup(cleanup)
	return ControlSocketPath(dbPath)
}

// controlRequestBody marshals req into one newline-terminated request line.
func controlRequestBody(t *testing.T, req controlRequest) []byte {
	t.Helper()
	line, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return append(line, '\n')
}

// controlRoundTrip sends body over a fresh connection and returns the parsed
// response plus the raw response bytes.
func controlRoundTrip(t *testing.T, path string, body []byte) (controlResponse, []byte) {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("dial control socket: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := conn.Write(body); err != nil {
		// The server may answer and close before the full body is written
		// (e.g. oversized request lines); the response still arrives.
		t.Logf("write request: %v", err)
	}
	raw, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var resp controlResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("parse response %q: %v", raw, err)
	}
	return resp, raw
}

func TestControlSocketPath(t *testing.T) {
	if got, want := ControlSocketPath("/data/archive/telemcp.db"), "/data/archive/watch.sock"; got != want {
		t.Fatalf("ControlSocketPath = %q, want %q", got, want)
	}
}

func TestControlSocketRoundTrip(t *testing.T) {
	wantFiles := []MediaDownload{
		{ChatID: "-1001234567890", MessageID: 101, Status: "downloaded", Path: "/data/media/101_photo.jpg", Size: 1234, Title: "101_photo.jpg"},
		{ChatID: "-1001234567890", MessageID: 102, Status: "no_media"},
	}
	stub := &stubControlFetcher{files: wantFiles}
	path := startTestControlSocket(t, stub)

	dest := filepath.Join(t.TempDir(), "out")
	req := controlRequest{
		Cmd:      "download_media",
		Chat:     "-1001234567890",
		Messages: []int{101, 102},
		Dest:     dest,
		MaxMB:    2048,
	}
	resp, _ := controlRoundTrip(t, path, controlRequestBody(t, req))
	if !resp.OK {
		t.Fatalf("ok:false, want ok:true: %s", resp.Error)
	}
	if len(resp.Files) != len(wantFiles) {
		t.Fatalf("got %d files, want %d: %#v", len(resp.Files), len(wantFiles), resp.Files)
	}
	for i, f := range wantFiles {
		if resp.Files[i] != f {
			t.Fatalf("files[%d] = %#v, want %#v", i, resp.Files[i], f)
		}
	}

	if stub.callCount() != 1 {
		t.Fatalf("fetcher called %d times, want 1", stub.callCount())
	}
	stub.mu.Lock()
	defer stub.mu.Unlock()
	if stub.chat != req.Chat {
		t.Errorf("fetcher chat = %q, want %q", stub.chat, req.Chat)
	}
	if len(stub.msgIDs) != 2 || stub.msgIDs[0] != 101 || stub.msgIDs[1] != 102 {
		t.Errorf("fetcher msgIDs = %v, want [101 102]", stub.msgIDs)
	}
	if stub.opts.Dest != dest {
		t.Errorf("fetcher dest = %q, want %q", stub.opts.Dest, dest)
	}
	if want := int64(2048) * 1024 * 1024; stub.opts.MaxBytes != want {
		t.Errorf("fetcher MaxBytes = %d, want %d", stub.opts.MaxBytes, want)
	}
}

func TestControlSocketEmptyFilesArray(t *testing.T) {
	stub := &stubControlFetcher{} // files stays nil
	path := startTestControlSocket(t, stub)

	req := controlRequest{Cmd: "download_media", Chat: "-100123", Messages: []int{7}, Dest: "/tmp/out", MaxMB: 1}
	resp, raw := controlRoundTrip(t, path, controlRequestBody(t, req))
	if !resp.OK {
		t.Fatalf("ok:false, want ok:true: %s", resp.Error)
	}
	if !bytes.Contains(raw, []byte(`"files":[]`)) {
		t.Fatalf("response %q lacks empty files array", raw)
	}
	if len(resp.Files) != 0 {
		t.Fatalf("got %d files, want 0", len(resp.Files))
	}
}

func TestControlSocketInvalidRequests(t *testing.T) {
	stub := &stubControlFetcher{}
	path := startTestControlSocket(t, stub)

	valid := controlRequest{Cmd: "download_media", Chat: "-100123", Messages: []int{1}, Dest: "/tmp/out", MaxMB: 1}
	mut := func(f func(*controlRequest)) []byte {
		req := valid
		f(&req)
		return controlRequestBody(t, req)
	}

	tests := []struct {
		name string
		body []byte
	}{
		{"unknown cmd", mut(func(r *controlRequest) { r.Cmd = "ping" })},
		{"empty cmd", mut(func(r *controlRequest) { r.Cmd = "" })},
		{"empty chat", mut(func(r *controlRequest) { r.Chat = "" })},
		{"no ids", mut(func(r *controlRequest) { r.Messages = nil })},
		{"too many ids", mut(func(r *controlRequest) {
			r.Messages = make([]int, 201)
			for i := range r.Messages {
				r.Messages[i] = i + 1
			}
		})},
		{"negative id", mut(func(r *controlRequest) { r.Messages = []int{5, -5} })},
		{"zero id", mut(func(r *controlRequest) { r.Messages = []int{0} })},
		{"relative dest", mut(func(r *controlRequest) { r.Dest = "relative/out" })},
		{"dirty relative dest", mut(func(r *controlRequest) { r.Dest = "a/../b" })},
		{"dirty absolute dest", mut(func(r *controlRequest) { r.Dest = "/tmp/a/../b" })},
		{"negative max_mb", mut(func(r *controlRequest) { r.MaxMB = -1 })},
		{"overflowing max_mb", mut(func(r *controlRequest) { r.MaxMB = 1<<63 - 1 })},
		{"garbage line", []byte("}{ not json\n")},
		{"empty line", []byte("\n")},
		{"oversized line", append(bytes.Repeat([]byte("x"), controlMaxRequestBytes+1), '\n')},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, raw := controlRoundTrip(t, path, tt.body)
			if resp.OK {
				t.Fatalf("ok:true, want ok:false: %s", raw)
			}
			if resp.Error == "" {
				t.Fatal("empty error message")
			}
			if bytes.Contains(raw, []byte(`"files"`)) {
				t.Fatalf("error response %q carries files key", raw)
			}
		})
	}
	if got := stub.callCount(); got != 0 {
		t.Fatalf("fetcher called %d times for invalid requests, want 0", got)
	}
}

func TestControlSocketFetcherError(t *testing.T) {
	stub := &stubControlFetcher{err: errors.New("chat not found in dialogs")}
	path := startTestControlSocket(t, stub)

	req := controlRequest{Cmd: "download_media", Chat: "-100999", Messages: []int{1}, Dest: "/tmp/out", MaxMB: 1}
	resp, _ := controlRoundTrip(t, path, controlRequestBody(t, req))
	if resp.OK {
		t.Fatal("ok:true, want ok:false on fetcher error")
	}
	if !strings.Contains(resp.Error, "chat not found in dialogs") {
		t.Fatalf("error %q does not carry the fetcher failure", resp.Error)
	}
}

func TestControlSocketConcurrentRequests(t *testing.T) {
	stub := &stubControlFetcher{files: []MediaDownload{{ChatID: "-100123", MessageID: 1, Status: "downloaded"}}}
	path := startTestControlSocket(t, stub)

	const conns = 2
	dests := make([]string, conns)
	for i := range dests {
		dests[i] = filepath.Join(t.TempDir(), "out")
	}
	var wg sync.WaitGroup
	fail := make(chan error, conns)
	for i := range conns {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := controlRequest{
				Cmd:      "download_media",
				Chat:     "-100123",
				Messages: []int{i + 1},
				Dest:     dests[i],
				MaxMB:    16,
			}
			conn, err := net.Dial("unix", path)
			if err != nil {
				fail <- err
				return
			}
			defer conn.Close()
			if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
				fail <- err
				return
			}
			body, err := json.Marshal(req)
			if err != nil {
				fail <- err
				return
			}
			if _, err := conn.Write(append(body, '\n')); err != nil {
				fail <- err
				return
			}
			raw, err := io.ReadAll(conn)
			if err != nil {
				fail <- err
				return
			}
			var resp controlResponse
			if err := json.Unmarshal(raw, &resp); err != nil {
				fail <- err
				return
			}
			if !resp.OK || len(resp.Files) != 1 || resp.Files[0].MessageID != 1 {
				fail <- errors.New("unexpected response")
			}
		}()
	}
	wg.Wait()
	close(fail)
	for err := range fail {
		t.Error(err)
	}
	if got := stub.callCount(); got != conns {
		t.Fatalf("fetcher called %d times, want %d", got, conns)
	}
}

func TestControlSocketLifecycle(t *testing.T) {
	dbPath := newControlArchive(t)
	sockPath := ControlSocketPath(dbPath)

	// A leftover socket file from a crashed daemon must not block startup.
	if err := os.WriteFile(sockPath, nil, 0o600); err != nil {
		t.Fatalf("create stale socket file: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cleanup, err := serveControlSocket(ctx, dbPath, &stubControlFetcher{}, func(string, ...any) {})
	if err != nil {
		t.Fatalf("serve over stale socket file: %v", err)
	}

	info, err := os.Stat(sockPath)
	if err != nil {
		t.Fatalf("stat socket: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("socket perms = %o, want 600", perm)
	}

	cancel()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, statErr := os.Stat(sockPath); os.IsNotExist(statErr) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("socket file still present after ctx cancel")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cleanup() // idempotent after ctx-driven removal
}

func TestControlSocketCleanupRemovesSocket(t *testing.T) {
	dbPath := newControlArchive(t)
	sockPath := ControlSocketPath(dbPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cleanup, err := serveControlSocket(ctx, dbPath, &stubControlFetcher{}, func(string, ...any) {})
	if err != nil {
		t.Fatalf("serve control socket: %v", err)
	}
	cleanup()
	if _, err := os.Stat(sockPath); !os.IsNotExist(err) {
		t.Fatalf("socket file survived cleanup: stat err = %v", err)
	}
	// Further dials must fail: the listener is closed.
	if conn, dialErr := net.Dial("unix", sockPath); dialErr == nil {
		conn.Close()
		t.Fatal("dial succeeded after cleanup, want connection refused")
	}
}
