package telegram

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
)

const (
	// controlMaxRequestBytes caps one control-socket request line.
	controlMaxRequestBytes = 64 << 10
	// controlMaxMessages caps the per-request message id count.
	controlMaxMessages = 200
)

// mediaDownloader is the piece of the live connection the control socket
// serves: batch media downloads for archived messages. *MediaFetcher
// implements it; tests inject a stub.
type mediaDownloader interface {
	FetchChatMedia(ctx context.Context, chatID string, msgIDs []int, opts DownloadOptions) ([]MediaDownload, error)
}

// ControlSocketPath returns the Unix domain socket the watch daemon serves
// media download requests on, next to the archive database. The MCP server
// dials it when the daemon runs.
func ControlSocketPath(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), "watch.sock")
}

// controlRequest is one newline-terminated JSON request line on the control
// socket.
type controlRequest struct {
	Cmd      string `json:"cmd"`
	Chat     string `json:"chat"`
	Messages []int  `json:"messages"`
	Dest     string `json:"dest"`
	MaxMB    int64  `json:"max_mb"`
}

// validate rejects requests the daemon will not serve. maxBytes is only
// meaningful for a validated request.
func (r controlRequest) validate() error {
	if r.Cmd != "download_media" {
		return fmt.Errorf("unknown cmd %q", r.Cmd)
	}
	if r.Chat == "" {
		return errors.New("chat is required")
	}
	if len(r.Messages) == 0 {
		return errors.New("messages must contain at least one id")
	}
	if len(r.Messages) > controlMaxMessages {
		return fmt.Errorf("messages must contain at most %d ids, got %d", controlMaxMessages, len(r.Messages))
	}
	for _, id := range r.Messages {
		if id <= 0 {
			return fmt.Errorf("message id %d must be positive", id)
		}
	}
	if !filepath.IsAbs(r.Dest) {
		return fmt.Errorf("dest must be an absolute path, got %q", r.Dest)
	}
	if filepath.Clean(r.Dest) != r.Dest {
		return fmt.Errorf("dest must be a clean path, got %q", r.Dest)
	}
	if r.MaxMB < 0 {
		return fmt.Errorf("max_mb must not be negative, got %d", r.MaxMB)
	}
	if r.MaxMB > (1<<63-1)/(1024*1024) {
		return fmt.Errorf("max_mb %d overflows the byte cap", r.MaxMB)
	}
	return nil
}

// maxBytes converts the request's max_mb into the DownloadOptions byte cap.
// Only valid after validate: MaxMB is non-negative and cannot overflow.
func (r controlRequest) maxBytes() int64 {
	return r.MaxMB * 1024 * 1024
}

// controlResponse is one newline-terminated JSON response line. It marshals
// to exactly {"ok":true,"files":[...]} or {"ok":false,"error":"..."}: files
// is never null on success, error is absent on success.
type controlResponse struct {
	OK    bool
	Error string
	Files []MediaDownload
}

// MarshalJSON implements the fixed wire format.
func (r controlResponse) MarshalJSON() ([]byte, error) {
	if !r.OK {
		return json.Marshal(struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		}{OK: r.OK, Error: r.Error})
	}
	files := r.Files
	if files == nil {
		files = []MediaDownload{}
	}
	return json.Marshal(struct {
		OK    bool            `json:"ok"`
		Files []MediaDownload `json:"files"`
	}{OK: r.OK, Files: files})
}

// serveControlSocket serves media download requests on
// ControlSocketPath(dbPath) until ctx is cancelled or the returned cleanup
// runs. The daemon holds the archive connection lock for its lifetime, so a
// socket file present at startup belongs to a crashed daemon and is removed.
// Requests are served concurrently; fetcher must be safe for concurrent use.
//
// cleanup closes the listener and removes the socket file. It is idempotent
// and safe to call after ctx cancellation. A listen failure returns an error
// and no cleanup: the caller logs and continues without the socket.
func serveControlSocket(ctx context.Context, dbPath string, fetcher mediaDownloader, logf func(string, ...any)) (cleanup func(), err error) {
	path := ControlSocketPath(dbPath)
	if rmErr := os.Remove(path); rmErr != nil && !os.IsNotExist(rmErr) {
		return nil, fmt.Errorf("remove stale control socket: %w", rmErr)
	}
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen on control socket: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		_ = os.Remove(path)
		return nil, fmt.Errorf("chmod control socket: %w", err)
	}

	var once sync.Once
	cleanup = func() {
		once.Do(func() {
			_ = ln.Close()
			_ = os.Remove(path)
		})
	}

	go func() {
		<-ctx.Done()
		cleanup()
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
					return
				}
				logf("watch: control socket: accept: %v\n", err)
				return
			}
			go serveControlConn(ctx, conn, fetcher, logf)
		}
	}()

	return cleanup, nil
}

// serveControlConn handles one control-socket connection: a single JSON
// request line and one JSON response line, then close.
func serveControlConn(ctx context.Context, conn net.Conn, fetcher mediaDownloader, logf func(string, ...any)) {
	defer conn.Close()

	req, err := readControlRequest(conn)
	if err != nil {
		_ = writeControlResponse(conn, controlResponse{Error: err.Error()})
		return
	}
	files, err := fetcher.FetchChatMedia(ctx, req.Chat, req.Messages, DownloadOptions{Dest: req.Dest, MaxBytes: req.maxBytes()})
	if err != nil {
		logf("watch: control socket: chat %s: %v\n", req.Chat, err)
		_ = writeControlResponse(conn, controlResponse{Error: err.Error()})
		return
	}
	_ = writeControlResponse(conn, controlResponse{OK: true, Files: files})
}

// readControlRequest reads and validates one request line, capped at
// controlMaxRequestBytes. A final line without the trailing newline is
// tolerated.
func readControlRequest(conn net.Conn) (controlRequest, error) {
	var req controlRequest
	reader := bufio.NewReaderSize(conn, controlMaxRequestBytes)
	line, err := reader.ReadSlice('\n')
	if err != nil {
		switch {
		case errors.Is(err, bufio.ErrBufferFull):
			return req, fmt.Errorf("request line exceeds %d bytes", controlMaxRequestBytes)
		case errors.Is(err, io.EOF) && len(line) > 0:
			// tolerate a final line without the newline
		default:
			return req, fmt.Errorf("read request: %w", err)
		}
	}
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return req, errors.New("empty request")
	}
	if err := json.Unmarshal(line, &req); err != nil {
		return req, fmt.Errorf("parse request: %w", err)
	}
	return req, req.validate()
}

// writeControlResponse writes one response line; the connection closes right
// after, so write failures only mean the client went away.
func writeControlResponse(conn net.Conn, resp controlResponse) error {
	line, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	_, err = conn.Write(line)
	return err
}
