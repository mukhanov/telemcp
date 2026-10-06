package telegram

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/gotd/td/session"
	"github.com/gotd/td/session/tdesktop"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
)

// Auth source kinds: what a connection authorizes with.
const (
	authSourceSession = "session"
	authSourceTData   = "tdata"
)

// AppCredentials are the Telegram API credentials client connections use.
type AppCredentials struct {
	ID   int32
	Hash string
}

// ResolveAppCredentials returns the API credentials: TELEMCP_API_ID and
// TELEMCP_API_HASH override the borrowed Telegram Desktop constants when both
// are set; the Desktop constants are used otherwise. The two variables form a
// pair — setting only one (or an unparseable, non-positive id) is an error.
func ResolveAppCredentials() (AppCredentials, error) {
	idEnv := strings.TrimSpace(os.Getenv("TELEMCP_API_ID"))
	hashEnv := strings.TrimSpace(os.Getenv("TELEMCP_API_HASH"))
	if idEnv == "" && hashEnv == "" {
		return AppCredentials{ID: telegramDesktopAPIID, Hash: telegramDesktopAPIHash}, nil
	}
	if idEnv == "" || hashEnv == "" {
		return AppCredentials{}, errors.New("TELEMCP_API_ID and TELEMCP_API_HASH must be set together")
	}
	id, err := strconv.ParseInt(idEnv, 10, 32)
	if err != nil {
		return AppCredentials{}, fmt.Errorf("invalid TELEMCP_API_ID %q: %w", idEnv, err)
	}
	if id <= 0 {
		return AppCredentials{}, fmt.Errorf("invalid TELEMCP_API_ID %q: must be positive", idEnv)
	}
	return AppCredentials{ID: int32(id), Hash: hashEnv}, nil
}

// AuthSource is what the daemon, an import or a media download authorizes
// with: a dedicated telemcp session file, or the Telegram Desktop tdata.
type AuthSource struct {
	Kind string // "session" | "tdata"
	Path string // session file path, or tdata directory
}

// DefaultSessionPath returns the dedicated session file path:
// ~/.telemcp/session.json.
func DefaultSessionPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".telemcp", "session.json")
}

// ResolveAuthSource picks the authorization source. An explicit sessionPath
// must name an existing session file. Empty prefers the default session file
// when it exists and falls back to the Telegram Desktop tdata directory, so a
// telemcp-owned session is preferred over the Desktop-shared authorization
// (and its FLOOD_WAIT quota) whenever one has been created.
func ResolveAuthSource(sessionPath string) (AuthSource, error) {
	sessionPath = strings.TrimSpace(sessionPath)
	if sessionPath != "" {
		absolute, err := filepath.Abs(filepath.Clean(sessionPath))
		if err != nil {
			return AuthSource{}, fmt.Errorf("resolve session file: %w", err)
		}
		if info, err := os.Stat(absolute); err != nil || !info.Mode().IsRegular() {
			return AuthSource{}, fmt.Errorf("session file %s: not found", absolute)
		}
		return AuthSource{Kind: authSourceSession, Path: absolute}, nil
	}
	if info, err := os.Stat(DefaultSessionPath()); err == nil && info.Mode().IsRegular() {
		return AuthSource{Kind: authSourceSession, Path: DefaultSessionPath()}, nil
	}
	return AuthSource{Kind: authSourceTData, Path: resolveImportSource("").path}, nil
}

// SessionStorage builds the in-memory session storage a client connects with.
// Every source is read once into memory: the session file itself is never
// written by watch, import or download, and the tdata flow mirrors the
// previous per-site tdesktop.Read handling.
func (s AuthSource) SessionStorage(ctx context.Context) (session.Storage, error) {
	switch s.Kind {
	case authSourceSession:
		data, err := (&session.Loader{Storage: &session.FileStorage{Path: s.Path}}).Load(ctx)
		if err != nil {
			return nil, fmt.Errorf("load session file %s: %w", s.Path, err)
		}
		storage := &session.StorageMemory{}
		if err := (&session.Loader{Storage: storage}).Save(ctx, data); err != nil {
			return nil, fmt.Errorf("store session file %s: %w", s.Path, err)
		}
		return storage, nil
	case authSourceTData:
		accounts, err := tdesktop.Read(s.Path, nil)
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
		return storage, nil
	default:
		return nil, fmt.Errorf("unknown auth source kind %q", s.Kind)
	}
}

// Identity returns the canonicalized source path recorded in
// ImportStats.SourcePath and used for media-ref reuse validation, so an
// import, the watch daemon and their media references compare consistently.
func (s AuthSource) Identity() string {
	canonical, err := canonicalImportSourcePath(s.Path)
	if err != nil {
		return filepath.Clean(strings.TrimSpace(s.Path))
	}
	return canonical
}

// resolveDownloadSource keeps the media-download sourcePath semantics: empty
// prefers the default session file over the default tdata directory; an
// explicit path is a session file when it names an existing regular file and
// a tdata directory otherwise (the TELEMCP_SOURCE escape hatch).
func resolveDownloadSource(sourcePath string) (AuthSource, error) {
	trimmed := strings.TrimSpace(sourcePath)
	if trimmed == "" {
		return ResolveAuthSource("")
	}
	if info, err := os.Stat(trimmed); err == nil && info.Mode().IsRegular() {
		return ResolveAuthSource(trimmed)
	}
	return AuthSource{Kind: authSourceTData, Path: trimmed}, nil
}

// telegramDeviceConfig mirrors the Telegram Desktop client every connection
// presents as.
func telegramDeviceConfig() telegram.DeviceConfig {
	return telegram.DeviceConfig{
		DeviceModel:    "Desktop",
		SystemVersion:  "Windows 11",
		AppVersion:     "6.5 x64",
		SystemLangCode: "en-US",
		LangPack:       "tdesktop",
		LangCode:       "en",
	}
}

// LoginOptions configure the interactive login.
type LoginOptions struct {
	// SessionPath stores the authorized session, created atomically
	// (tmp+rename) with 0600 permissions; empty stores it at
	// DefaultSessionPath().
	SessionPath string
	// In carries the phone/code/password answers, Out the prompts and
	// progress lines. Nil defaults to stdin/stdout.
	In  io.Reader
	Out io.Writer
}

// Login authorizes a fresh telemcp-owned Telegram session: phone, login code
// and — only when the server asks — the 2FA password, prompted from
// LoginOptions.In with prompts and progress on LoginOptions.Out. The
// authorized session is stored atomically at LoginOptions.SessionPath.
func Login(ctx context.Context, opts LoginOptions) error {
	creds, err := ResolveAppCredentials()
	if err != nil {
		return err
	}
	sessionPath := strings.TrimSpace(opts.SessionPath)
	if sessionPath == "" {
		sessionPath = DefaultSessionPath()
	}
	in := opts.In
	if in == nil {
		in = os.Stdin
	}
	out := opts.Out
	if out == nil {
		out = os.Stdout
	}
	storage := &session.StorageMemory{}
	client := telegram.NewClient(int(creds.ID), creds.Hash, telegram.Options{
		SessionStorage: storage,
		NoUpdates:      true,
		Middlewares:    []telegram.Middleware{newTelegramFloodWaitPolicy(out)},
		Device:         telegramDeviceConfig(),
	})
	err = client.Run(ctx, func(ctx context.Context) error {
		if err := auth.NewFlow(newLoginCollector(in, out), auth.SendCodeOptions{}).Run(ctx, client.Auth()); err != nil {
			return fmt.Errorf("telegram login: %w", err)
		}
		self, err := client.Self(ctx)
		if err != nil {
			return fmt.Errorf("telegram session is not authorized: %w", err)
		}
		fmt.Fprintf(out, "authorized as %s (%d)\n", tdataUserInfo(self).name, self.ID)
		return nil
	})
	if err != nil {
		return err
	}
	return persistSessionFile(ctx, sessionPath, storage)
}

// persistSessionFile writes the authorized session to path atomically: the
// JSON payload lands in a 0600 temp file in the target directory and is
// renamed into place, so readers never observe a partial session.
func persistSessionFile(ctx context.Context, path string, storage *session.StorageMemory) error {
	data, err := (&session.Loader{Storage: storage}).Load(ctx)
	if err != nil {
		return fmt.Errorf("load stored session: %w", err)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create session directory: %w", err)
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create session temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() // no-op after a successful rename
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("create session temp file: %w", err)
	}
	if err := (&session.Loader{Storage: &session.FileStorage{Path: tmpPath}}).Save(ctx, data); err != nil {
		return fmt.Errorf("store session file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("store session file: %w", err)
	}
	return nil
}

// loginCollector feeds the interactive login answers into the gotd auth flow:
// phone, then the login code, then the 2FA password when the server asks for
// one. It is a seam, so the prompt protocol is testable without a Telegram
// connection.
type loginCollector struct {
	in    *bufio.Reader
	out   io.Writer
	phone string
}

func newLoginCollector(in io.Reader, out io.Writer) *loginCollector {
	return &loginCollector{in: bufio.NewReader(in), out: out}
}

func (c *loginCollector) Phone(ctx context.Context) (string, error) {
	phone, err := c.prompt("phone: ")
	if err != nil {
		return "", err
	}
	c.phone = phone
	return phone, nil
}

func (c *loginCollector) Code(ctx context.Context, sentCode *tg.AuthSentCode) (string, error) {
	fmt.Fprintf(c.out, "%s: code required\n", c.phone)
	return c.prompt("code: ")
}

func (c *loginCollector) Password(ctx context.Context) (string, error) {
	return c.prompt("2FA password: ")
}

// prompt writes the label to Out and reads one line from In. Answers are
// never echoed back.
func (c *loginCollector) prompt(label string) (string, error) {
	if _, err := fmt.Fprint(c.out, label); err != nil {
		return "", fmt.Errorf("write prompt: %w", err)
	}
	line, err := c.in.ReadString('\n')
	if err != nil && line == "" {
		return "", fmt.Errorf("read %s: %w", strings.TrimSpace(label), err)
	}
	return strings.TrimSpace(line), nil
}

// AcceptTermsOfService declines silent acceptance; the flow surfaces the
// required sign-up instead.
func (c *loginCollector) AcceptTermsOfService(ctx context.Context, tos tg.HelpTermsOfService) error {
	return &auth.SignUpRequired{TermsOfService: tos}
}

// SignUp is unsupported: telemcp only authorizes existing accounts.
func (c *loginCollector) SignUp(ctx context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New("sign-up is not supported; create the account in Telegram first")
}
