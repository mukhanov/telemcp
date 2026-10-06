package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/mukhanov/telemcp/internal/store"
	"github.com/mukhanov/telemcp/internal/telegram"
)

func TestApplyCredentialFlags(t *testing.T) {
	t.Setenv("TELEMCP_API_ID", "")
	t.Setenv("TELEMCP_API_HASH", "")
	tests := []struct {
		name     string
		apiID    int64
		apiHash  string
		wantErr  bool
		wantID   string
		wantHash string
	}{
		{name: "both empty is a no-op", apiID: 0, apiHash: "", wantID: "", wantHash: ""},
		{name: "pair sets env", apiID: 12345, apiHash: "0abc", wantID: "12345", wantHash: "0abc"},
		{name: "max int32 accepted", apiID: math.MaxInt32, apiHash: "0abc", wantID: "2147483647", wantHash: "0abc"},
		{name: "id without hash", apiID: 12345, apiHash: "", wantErr: true},
		{name: "hash without id", apiID: 0, apiHash: "0abc", wantErr: true},
		{name: "negative id", apiID: -1, apiHash: "0abc", wantErr: true},
		{name: "id above int32", apiID: int64(math.MaxInt32) + 1, apiHash: "0abc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TELEMCP_API_ID", "")
			t.Setenv("TELEMCP_API_HASH", "")
			err := applyCredentialFlags(tt.apiID, tt.apiHash)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("applyCredentialFlags(%d, %q) = nil, want error", tt.apiID, tt.apiHash)
				}
				if ExitCode(err) != 2 {
					t.Fatalf("ExitCode(%v) = %d, want 2 (usage error)", err, ExitCode(err))
				}
			} else if err != nil {
				t.Fatalf("applyCredentialFlags(%d, %q) = %v", tt.apiID, tt.apiHash, err)
			}
			if got := os.Getenv("TELEMCP_API_ID"); got != tt.wantID {
				t.Fatalf("TELEMCP_API_ID = %q, want %q", got, tt.wantID)
			}
			if got := os.Getenv("TELEMCP_API_HASH"); got != tt.wantHash {
				t.Fatalf("TELEMCP_API_HASH = %q, want %q", got, tt.wantHash)
			}
		})
	}
}

func TestApplyCredentialFlagsLeavesEnvWhenEmpty(t *testing.T) {
	t.Setenv("TELEMCP_API_ID", "preset-id")
	t.Setenv("TELEMCP_API_HASH", "preset-hash")
	if err := applyCredentialFlags(0, ""); err != nil {
		t.Fatalf("applyCredentialFlags(0, \"\") = %v", err)
	}
	if got := os.Getenv("TELEMCP_API_ID"); got != "preset-id" {
		t.Fatalf("TELEMCP_API_ID = %q, want preset value kept", got)
	}
	if got := os.Getenv("TELEMCP_API_HASH"); got != "preset-hash" {
		t.Fatalf("TELEMCP_API_HASH = %q, want preset value kept", got)
	}
}

func TestRunLogin(t *testing.T) {
	t.Setenv("TELEMCP_API_ID", "")
	t.Setenv("TELEMCP_API_HASH", "")
	sentinel := errors.New("stubbed login")
	var calls int
	var gotCtx context.Context
	var gotOpts telegram.LoginOptions
	orig := loginFlow
	loginFlow = func(ctx context.Context, opts telegram.LoginOptions) error {
		calls++
		gotCtx, gotOpts = ctx, opts
		return sentinel
	}
	t.Cleanup(func() { loginFlow = orig })

	var out bytes.Buffer
	r := &runtime{ctx: context.Background(), stdout: &out, stderr: io.Discard}

	t.Run("flags wire options and credentials", func(t *testing.T) {
		sessionPath := filepath.Join(t.TempDir(), "s.json")
		err := r.runLogin([]string{"--session", sessionPath, "--api-id", "777", "--api-hash", "hash"})
		if !errors.Is(err, sentinel) {
			t.Fatalf("runLogin err = %v, want sentinel", err)
		}
		if calls != 1 {
			t.Fatalf("login flow calls = %d, want 1", calls)
		}
		if gotOpts.SessionPath != sessionPath {
			t.Fatalf("SessionPath = %q, want %q", gotOpts.SessionPath, sessionPath)
		}
		if gotOpts.In != io.Reader(os.Stdin) {
			t.Fatalf("In = %v, want os.Stdin", gotOpts.In)
		}
		if gotOpts.Out != io.Writer(&out) {
			t.Fatalf("Out = %v, want the runtime stdout writer", gotOpts.Out)
		}
		if gotCtx != r.ctx {
			t.Fatal("login flow received a different context")
		}
		if got := os.Getenv("TELEMCP_API_ID"); got != "777" {
			t.Fatalf("TELEMCP_API_ID = %q, want 777", got)
		}
		if got := os.Getenv("TELEMCP_API_HASH"); got != "hash" {
			t.Fatalf("TELEMCP_API_HASH = %q, want hash", got)
		}
	})

	t.Run("default session path", func(t *testing.T) {
		if err := r.runLogin(nil); !errors.Is(err, sentinel) {
			t.Fatalf("runLogin err = %v, want sentinel", err)
		}
		if gotOpts.SessionPath != telegram.DefaultSessionPath() {
			t.Fatalf("SessionPath = %q, want default %q", gotOpts.SessionPath, telegram.DefaultSessionPath())
		}
	})

	t.Run("credential flags must pair", func(t *testing.T) {
		err := r.runLogin([]string{"--api-id", "777"})
		if ExitCode(err) != 2 {
			t.Fatalf("ExitCode(%v) = %d, want 2 (usage error)", err, ExitCode(err))
		}
		if calls != 2 {
			t.Fatalf("login flow calls = %d, want 2 (validation must precede the flow)", calls)
		}
	})
}

func TestRunImportPassesSessionFlag(t *testing.T) {
	sentinel := errors.New("stubbed import")
	var gotOpts telegram.ImportOptions
	orig := importFlow
	importFlow = func(ctx context.Context, opts telegram.ImportOptions, dbPath string) (telegram.ImportResult, error) {
		gotOpts = opts
		return telegram.ImportResult{}, sentinel
	}
	t.Cleanup(func() { importFlow = orig })

	dir := t.TempDir()
	r := &runtime{ctx: context.Background(), stdout: io.Discard, stderr: io.Discard, dbPath: filepath.Join(dir, "telemcp.db")}
	sessionPath := filepath.Join(dir, "session.json")
	if err := r.runImport([]string{"--session", sessionPath}); !errors.Is(err, sentinel) {
		t.Fatalf("runImport err = %v, want sentinel", err)
	}
	if gotOpts.SessionPath != sessionPath {
		t.Fatalf("SessionPath = %q, want %q", gotOpts.SessionPath, sessionPath)
	}
	if gotOpts.Path != "" {
		t.Fatalf("Path = %q, want empty (no --path given)", gotOpts.Path)
	}

	if err := r.runImport(nil); !errors.Is(err, sentinel) {
		t.Fatalf("runImport without flags err = %v, want sentinel", err)
	}
	if gotOpts.SessionPath != "" {
		t.Fatalf("default SessionPath = %q, want empty (telegram resolves the fallback)", gotOpts.SessionPath)
	}
}

func TestRunWatchPassesSessionFlag(t *testing.T) {
	sentinel := errors.New("stubbed watch")
	var gotOpts telegram.WatchOptions
	orig := watchFlow
	watchFlow = func(ctx context.Context, opts telegram.WatchOptions, st *store.Store, hooks telegram.WatchHooks) error {
		gotOpts = opts
		return sentinel
	}
	t.Cleanup(func() { watchFlow = orig })

	dir := t.TempDir()
	r := &runtime{ctx: context.Background(), stdout: io.Discard, stderr: io.Discard, dbPath: filepath.Join(dir, "telemcp.db")}
	sessionPath := filepath.Join(dir, "session.json")
	if err := r.runWatch([]string{"--session", sessionPath}); !errors.Is(err, sentinel) {
		t.Fatalf("runWatch err = %v, want sentinel", err)
	}
	if gotOpts.SessionPath != sessionPath {
		t.Fatalf("SessionPath = %q, want %q", gotOpts.SessionPath, sessionPath)
	}
}
