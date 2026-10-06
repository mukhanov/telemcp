package telegram

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/gotd/td/session"
)

func TestResolveAppCredentials(t *testing.T) {
	const (
		desktopID   = telegramDesktopAPIID
		desktopHash = telegramDesktopAPIHash
	)
	tests := []struct {
		name    string
		idEnv   string
		hashEnv string
		wantID  int32
		want    string
		wantErr bool
	}{
		{name: "defaults", idEnv: "", hashEnv: "", wantID: desktopID, want: desktopHash},
		{name: "override", idEnv: "12345", hashEnv: "customhash", wantID: 12345, want: "customhash"},
		{name: "whitespace trimmed", idEnv: " 7 ", hashEnv: " h ", wantID: 7, want: "h"},
		{name: "only id set", idEnv: "12345", hashEnv: "", wantErr: true},
		{name: "only hash set", idEnv: "", hashEnv: "customhash", wantErr: true},
		{name: "unparseable id", idEnv: "abc", hashEnv: "h", wantErr: true},
		{name: "zero id", idEnv: "0", hashEnv: "h", wantErr: true},
		{name: "negative id", idEnv: "-5", hashEnv: "h", wantErr: true},
		{name: "empty hash with id", idEnv: "12345", hashEnv: " ", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TELEMCP_API_ID", tc.idEnv)
			t.Setenv("TELEMCP_API_HASH", tc.hashEnv)
			got, err := ResolveAppCredentials()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ResolveAppCredentials() = %+v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveAppCredentials() error: %v", err)
			}
			if got.ID != tc.wantID || got.Hash != tc.want {
				t.Fatalf("ResolveAppCredentials() = %+v, want {%d %s}", got, tc.wantID, tc.want)
			}
		})
	}
}

func TestDefaultSessionPath(t *testing.T) {
	t.Parallel()
	path := DefaultSessionPath()
	if filepath.Base(path) != "session.json" || filepath.Base(filepath.Dir(path)) != ".telemcp" {
		t.Fatalf("DefaultSessionPath() = %q, want ~/.telemcp/session.json", path)
	}
	if !filepath.IsAbs(path) {
		t.Fatalf("DefaultSessionPath() = %q, want absolute path", path)
	}
}

// testSessionData fabricates a minimal valid gotd session payload: a DC id
// and address plus non-empty auth key material. Loader.Save adds the version
// marker.
func testSessionData() *session.Data {
	return &session.Data{
		DC:        2,
		Addr:      "149.154.167.50:443",
		AuthKey:   bytes.Repeat([]byte{0xAB}, 256),
		AuthKeyID: bytes.Repeat([]byte{0xCD}, 8),
		Salt:      42,
	}
}

func writeSessionFile(t *testing.T, data *session.Data) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "session.json")
	if err := (&session.Loader{Storage: &session.FileStorage{Path: path}}).Save(context.Background(), data); err != nil {
		t.Fatalf("write session fixture: %v", err)
	}
	return path
}

func TestResolveAuthSource(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("HOME override is unix-specific")
	}

	t.Run("explicit file becomes session source", func(t *testing.T) {
		path := writeSessionFile(t, testSessionData())
		source, err := ResolveAuthSource(path)
		if err != nil {
			t.Fatalf("ResolveAuthSource() error: %v", err)
		}
		if source.Kind != authSourceSession || source.Path != path {
			t.Fatalf("source = %+v, want {session %s}", source, path)
		}
	})

	t.Run("explicit relative path is cleaned and absolute", func(t *testing.T) {
		dir := t.TempDir()
		previous, err := os.Getwd()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Chdir(dir); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chdir(previous) })
		if err := os.WriteFile(filepath.Join(dir, "session.json"), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		source, err := ResolveAuthSource("./session.json")
		if err != nil {
			t.Fatalf("ResolveAuthSource() error: %v", err)
		}
		// os.Getwd resolves symlinks (e.g. /var -> /private/var on macOS),
		// so the absolute expectation is the resolved file path.
		want, err := filepath.EvalSymlinks(filepath.Join(dir, "session.json"))
		if err != nil {
			t.Fatal(err)
		}
		if source.Kind != authSourceSession || source.Path != want {
			t.Fatalf("source = %+v, want {session %s}", source, want)
		}
	})

	t.Run("explicit missing file errors", func(t *testing.T) {
		if _, err := ResolveAuthSource(filepath.Join(t.TempDir(), "missing.json")); err == nil {
			t.Fatal("missing explicit session file must fail")
		}
	})

	t.Run("explicit directory errors", func(t *testing.T) {
		if _, err := ResolveAuthSource(t.TempDir()); err == nil {
			t.Fatal("directory as explicit session file must fail")
		}
	})

	t.Run("default session file wins", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		if err := os.MkdirAll(filepath.Dir(DefaultSessionPath()), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(DefaultSessionPath(), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		source, err := ResolveAuthSource("")
		if err != nil {
			t.Fatalf("ResolveAuthSource() error: %v", err)
		}
		if source.Kind != authSourceSession || source.Path != DefaultSessionPath() {
			t.Fatalf("source = %+v, want {session %s}", source, DefaultSessionPath())
		}
	})

	t.Run("default absent falls back to tdata", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		source, err := ResolveAuthSource("  ")
		if err != nil {
			t.Fatalf("ResolveAuthSource() error: %v", err)
		}
		if source.Kind != authSourceTData || source.Path != DefaultPath() {
			t.Fatalf("source = %+v, want {tdata %s}", source, DefaultPath())
		}
	})
}

func TestAuthSourceSessionStorageSessionKind(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := writeSessionFile(t, testSessionData())

	storage, err := AuthSource{Kind: authSourceSession, Path: path}.SessionStorage(ctx)
	if err != nil {
		t.Fatalf("SessionStorage() error: %v", err)
	}
	data, err := (&session.Loader{Storage: storage}).Load(ctx)
	if err != nil {
		t.Fatalf("load from memory storage: %v", err)
	}
	want := testSessionData()
	if data.DC != want.DC || data.Addr != want.Addr || data.Salt != want.Salt ||
		!bytes.Equal(data.AuthKey, want.AuthKey) || !bytes.Equal(data.AuthKeyID, want.AuthKeyID) {
		t.Fatalf("round-tripped session = %+v, want %+v", data, want)
	}
}

func TestAuthSourceSessionStorageSessionKindMissingFile(t *testing.T) {
	t.Parallel()
	missing := AuthSource{Kind: authSourceSession, Path: filepath.Join(t.TempDir(), "missing.json")}
	if _, err := missing.SessionStorage(context.Background()); err == nil {
		t.Fatal("missing session file must fail")
	}
}

func TestAuthSourceSessionStorageTDataKindMissingSource(t *testing.T) {
	t.Parallel()
	missing := AuthSource{Kind: authSourceTData, Path: filepath.Join(t.TempDir(), "missing")}
	if _, err := missing.SessionStorage(context.Background()); err == nil {
		t.Fatal("missing tdata source must fail")
	}
}

func TestAuthSourceSessionStorageUnknownKind(t *testing.T) {
	t.Parallel()
	unknown := AuthSource{Kind: "postbox", Path: t.TempDir()}
	if _, err := unknown.SessionStorage(context.Background()); err == nil {
		t.Fatal("unknown kind must fail")
	}
}

func TestAuthSourceIdentityCanonicalizes(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "session.json")
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	canonical, err := canonicalImportSourcePath(path)
	if err != nil {
		t.Fatal(err)
	}
	source := AuthSource{Kind: authSourceSession, Path: path}
	if got := source.Identity(); got != canonical {
		t.Fatalf("Identity() = %q, want %q", got, canonical)
	}
	// A path that cannot be canonicalized degrades to the cleaned path
	// instead of failing.
	missing := AuthSource{Kind: authSourceTData, Path: filepath.Join(dir, "missing")}
	if got := missing.Identity(); got != filepath.Join(dir, "missing") {
		t.Fatalf("Identity() = %q, want %q", got, filepath.Join(dir, "missing"))
	}
}

func TestResolveDownloadSource(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("HOME override is unix-specific")
	}

	t.Run("empty prefers default session", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		if err := os.MkdirAll(filepath.Dir(DefaultSessionPath()), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(DefaultSessionPath(), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		source, err := resolveDownloadSource("")
		if err != nil {
			t.Fatalf("resolveDownloadSource() error: %v", err)
		}
		if source.Kind != authSourceSession || source.Path != DefaultSessionPath() {
			t.Fatalf("source = %+v, want {session %s}", source, DefaultSessionPath())
		}
	})

	t.Run("empty without session falls back to tdata", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		source, err := resolveDownloadSource("")
		if err != nil {
			t.Fatalf("resolveDownloadSource() error: %v", err)
		}
		if source.Kind != authSourceTData || source.Path != DefaultPath() {
			t.Fatalf("source = %+v, want {tdata %s}", source, DefaultPath())
		}
	})

	t.Run("explicit tdata directory stays tdata", func(t *testing.T) {
		tdataDir := t.TempDir()
		source, err := resolveDownloadSource(tdataDir)
		if err != nil {
			t.Fatalf("resolveDownloadSource() error: %v", err)
		}
		if source.Kind != authSourceTData || source.Path != tdataDir {
			t.Fatalf("source = %+v, want {tdata %s}", source, tdataDir)
		}
	})

	t.Run("explicit session file wins", func(t *testing.T) {
		path := writeSessionFile(t, testSessionData())
		source, err := resolveDownloadSource(path)
		if err != nil {
			t.Fatalf("resolveDownloadSource() error: %v", err)
		}
		if source.Kind != authSourceSession || source.Path != path {
			t.Fatalf("source = %+v, want {session %s}", source, path)
		}
	})

	t.Run("explicit missing path stays tdata for today's error", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing")
		source, err := resolveDownloadSource(missing)
		if err != nil {
			t.Fatalf("resolveDownloadSource() error: %v", err)
		}
		if source.Kind != authSourceTData || source.Path != missing {
			t.Fatalf("source = %+v, want {tdata %s}", source, missing)
		}
	})
}

func TestLoginCollectorPrompts(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	collector := newLoginCollector(strings.NewReader("+15551234567\n98765\nhunter2\n"), &out)

	phone, err := collector.Phone(context.Background())
	if err != nil {
		t.Fatalf("Phone() error: %v", err)
	}
	if phone != "+15551234567" {
		t.Fatalf("Phone() = %q", phone)
	}
	code, err := collector.Code(context.Background(), nil)
	if err != nil {
		t.Fatalf("Code() error: %v", err)
	}
	if code != "98765" {
		t.Fatalf("Code() = %q", code)
	}
	password, err := collector.Password(context.Background())
	if err != nil {
		t.Fatalf("Password() error: %v", err)
	}
	if password != "hunter2" {
		t.Fatalf("Password() = %q", password)
	}

	prompts := out.String()
	for _, want := range []string{"phone:", "+15551234567: code required", "code:", "2FA password:"} {
		if !strings.Contains(prompts, want) {
			t.Fatalf("prompts %q missing %q", prompts, want)
		}
	}
	if strings.Contains(prompts, "98765") || strings.Contains(prompts, "hunter2") {
		t.Fatalf("prompts echo answers back: %q", prompts)
	}
}

func TestLoginCollectorEndOfInput(t *testing.T) {
	t.Parallel()
	collector := newLoginCollector(strings.NewReader(""), &bytes.Buffer{})
	if _, err := collector.Phone(context.Background()); err == nil {
		t.Fatal("empty input must fail")
	}
}

func TestLoginCollectorDeclinesSignUp(t *testing.T) {
	t.Parallel()
	collector := newLoginCollector(strings.NewReader("+15551234567\n"), &bytes.Buffer{})
	if _, err := collector.SignUp(context.Background()); err == nil {
		t.Fatal("sign-up must be unsupported")
	}
}
