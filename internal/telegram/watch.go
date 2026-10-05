package telegram

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	gotdlog "github.com/gotd/log"
	"github.com/gotd/td/session"
	"github.com/gotd/td/session/tdesktop"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message/peer"
	querymessages "github.com/gotd/td/telegram/query/messages"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/mukhanov/telemcp/internal/localfile"
	"github.com/mukhanov/telemcp/internal/store"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

// WatchOptions configures the resident live-sync daemon.
type WatchOptions struct {
	ImportOptions
	// ReconcileEvery is the period of full re-import passes over the same
	// connection; zero disables periodic passes (the startup pass and
	// gap-triggered passes still run).
	ReconcileEvery time.Duration
}

// WatchHooks receive converted data as it arrives. Every hook gets the single
// store connection the daemon owns, so all writes serialize on it.
type WatchHooks struct {
	// Live is called with messages converted from incoming updates, batched
	// per update container. Chats carries just enough to upsert the chat row.
	Live func(ctx context.Context, st *store.Store, stats store.ImportStats, chats []store.Chat, messages []store.Message) error
	// Reconcile is called after each full fetch pass. mediaStage holds media
	// copied during the pass; promote or archive it before returning — the
	// directory is removed right after the hook.
	Reconcile func(ctx context.Context, st *store.Store, result ImportResult, mediaStage string) error
}

// Watch runs the resident daemon: one persistent tdata-authorized connection
// that receives live updates into the archive, plus periodic full reconcile
// passes on the same connection. It blocks until ctx is cancelled. The store
// connection stays the caller's: the daemon writes through it only, so every
// archive write serializes on one SQLite connection.
//
// The updates.Manager state (pts/qts, channel pts, access hashes) is persisted
// in the archive itself, so a restarted daemon resumes from its saved position
// and getDifference covers the offline gap.
func Watch(ctx context.Context, opts WatchOptions, st *store.Store, hooks WatchHooks) error {
	dbPath := st.Path()
	release, err := acquireWatchLock(dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = release() }()

	source := resolveImportSource(strings.TrimSpace(opts.Path))
	canonicalPath, err := canonicalImportSourcePath(source.path)
	if err != nil {
		return fmt.Errorf("resolve Telegram source target: %w", err)
	}
	source.path = canonicalPath

	// Mirror Import(): keep only existing media refs that still resolve.
	archiveRoot := mediaArchiveDir(dbPath)
	var verifiedRefs []ExistingMediaRef
	if sameImportSourcePath(opts.ExistingMediaSourcePath, source.path) {
		for _, ref := range opts.ExistingMediaRefs {
			f, err := localfile.OpenRegular(archiveRoot, ref.MediaPath)
			if err != nil {
				continue
			}
			_ = f.Close()
			verifiedRefs = append(verifiedRefs, ref)
		}
	}
	opts.ExistingMediaRefs = verifiedRefs

	accounts, err := tdesktop.Read(source.path, nil)
	if err != nil {
		return fmt.Errorf("read Telegram Desktop tdata: %w", err)
	}
	if len(accounts) == 0 {
		return errors.New("no Telegram Desktop accounts found")
	}
	data, err := session.TDesktopSession(accounts[0])
	if err != nil {
		return fmt.Errorf("read Telegram Desktop session: %w", err)
	}
	storage := &session.StorageMemory{}
	if err := (&session.Loader{Storage: storage}).Save(ctx, data); err != nil {
		return fmt.Errorf("store Telegram Desktop session: %w", err)
	}

	handler := &watchHandler{
		hooks:      hooks,
		st:         st,
		progress:   opts.Progress,
		sourcePath: source.path,
		liveOpts:   liveImportOptions(opts.ImportOptions),
		reconcile:  make(chan struct{}, 1),
	}
	stateStore := newSQLiteUpdatesState(st)
	manager := updates.New(updates.Config{
		Handler:          handler,
		Logger:           progressLoggerOf(opts.Progress),
		Storage:          stateStore,
		AccessHasher:     stateStore,
		UserAccessHasher: stateStore,
		// The manager cannot self-recover from these gaps; a full pass can.
		OnChannelTooLong:      func(int64) { handler.requestReconcile() },
		OnTooLong:             func() { handler.requestReconcile() },
		OnLoadUserStateFailed: func() { handler.requestReconcile() },
	})

	// Setting UpdateHandler switches NoUpdates off; one connection then feeds
	// both the live path and the reconcile passes below.
	client := telegram.NewClient(telegramDesktopAPIID, telegramDesktopAPIHash, telegram.Options{
		SessionStorage: storage,
		UpdateHandler:  manager,
		AllowCDN:       true,
		Middlewares:    []telegram.Middleware{newTelegramFloodWaitPolicy(opts.Progress)},
		Device: telegram.DeviceConfig{
			DeviceModel:    "Desktop",
			SystemVersion:  "Windows 11",
			AppVersion:     "6.5 x64",
			SystemLangCode: "en-US",
			LangPack:       "tdesktop",
			LangCode:       "en",
		},
	})

	return client.Run(ctx, func(ctx context.Context) error {
		self, err := client.Self(ctx)
		if err != nil {
			return fmt.Errorf("telegram session is not authorized: %w", err)
		}
		handler.selfID = self.ID
		handler.raw = tg.NewClient(client)
		handler.stats = store.ImportStats{
			SourcePath:          source.path,
			SourcePathCanonical: true,
			SourceIdentity:      sourceIdentity("tdata", strconv.FormatInt(self.ID, 10)),
			DBPath:              dbPath,
		}
		handler.liveSession = &tdataImportSession{
			raw:        handler.raw,
			selfID:     self.ID,
			opts:       handler.liveOpts,
			sourcePath: source.path,
		}
		if opts.Progress != nil {
			fmt.Fprintf(opts.Progress, "watch: authorized as %d; receiving live updates\n", self.ID)
		}

		g, gctx := errgroup.WithContext(ctx)
		// One shared fetcher for the control socket: dialog peer cache is
		// per instance, and it is safe for concurrent use.
		mediaFetch := NewMediaFetcherWithHashes(handler.raw, self.ID, daemonPeerHashes{st: stateStore, userID: self.ID})
		g.Go(func() error {
			handler.bootstrapChannelPts(ctx, stateStore)
			return manager.Run(gctx, handler.raw, self.ID, updates.AuthOptions{})
		})
		g.Go(func() error {
			handler.reconcileLoop(gctx, opts, dbPath)
			return nil
		})
		g.Go(func() error {
			cleanupControl, err := serveControlSocket(gctx, dbPath, mediaFetch, handler.logf)
			if err != nil {
				// Downloads still work without the socket (the MCP server
				// falls back to an ephemeral connection); never fatal.
				handler.logf("watch: media requests unavailable: %v\n", err)
				return nil
			}
			handler.logf("watch: media requests on %s\n", ControlSocketPath(dbPath))
			defer cleanupControl()
			<-gctx.Done()
			return nil
		})
		return g.Wait()
	})
}

// liveImportOptions derives the conversion options for the push path: no media
// downloads there (reconcile fills media via the non-empty-only overwrite).
func liveImportOptions(opts ImportOptions) ImportOptions {
	live := opts
	live.FetchMedia = false
	return live
}

// bootstrapChannelPts repairs persisted channel pts before the updates manager
// loads its state. A stored pts the server no longer tracks (channels unread
// for a long time, hash-only rows) makes the manager's channel worker abort
// its first getChannelDifference with PERSISTENT_TIMESTAMP_EMPTY and drop that
// channel's live updates for the rest of the run. Asking for the difference
// from pts=1 with limit=1 makes the server answer channelDifferenceTooLong
// carrying its current pts — the same recovery the manager itself performs for
// UpdateChannelTooLong — which we persist so loadChannels boots live workers.
func (h *watchHandler) bootstrapChannelPts(ctx context.Context, stateStore *sqliteUpdatesState) {
	channels, err := stateStore.st.Channels(ctx, h.selfID)
	if err != nil {
		h.logf("watch: channel bootstrap: list channels: %v\n", err)
		return
	}
	repaired := 0
	for _, ch := range channels {
		if ctx.Err() != nil {
			return
		}
		diff, err := h.raw.UpdatesGetChannelDifference(ctx, &tg.UpdatesGetChannelDifferenceRequest{
			Channel: &tg.InputChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash},
			Filter:  &tg.ChannelMessagesFilterEmpty{},
			Pts:     1,
			Limit:   1,
		})
		if err != nil {
			if !tgerr.Is(err, "CHANNEL_PRIVATE") {
				h.logf("watch: channel bootstrap: channel %d: %v\n", ch.ChannelID, err)
			}
			continue
		}
		var pts int
		switch diff := diff.(type) {
		case *tg.UpdatesChannelDifference:
			pts = diff.Pts
		case *tg.UpdatesChannelDifferenceEmpty:
			pts = diff.Pts
		case *tg.UpdatesChannelDifferenceTooLong:
			if dialog, ok := diff.Dialog.(*tg.Dialog); ok {
				pts, _ = dialog.GetPts()
			}
		}
		if pts > ch.Pts {
			if err := stateStore.st.SetChannelPts(ctx, h.selfID, ch.ChannelID, pts); err != nil {
				h.logf("watch: channel bootstrap: channel %d: set pts: %v\n", ch.ChannelID, err)
				continue
			}
			repaired++
		}
	}
	if repaired > 0 {
		h.logf("watch: channel bootstrap: repaired pts for %d of %d channels\n", repaired, len(channels))
	}
}

// logf writes a daemon log line when progress output is configured.
func (h *watchHandler) logf(format string, args ...any) {
	if h.progress != nil {
		fmt.Fprintf(h.progress, format, args...)
	}
}

// watchHandler is the telegram.UpdateHandler fed by the updates manager. The
// manager normalizes every stream entry into *tg.Updates containers (short
// messages get expanded, channel batches combined, min access hashes
// backfilled); the leaf cases below only cover the pre-Run passthrough window.
type watchHandler struct {
	hooks       WatchHooks
	st          *store.Store
	progress    io.Writer
	raw         *tg.Client
	selfID      int64
	sourcePath  string
	liveOpts    ImportOptions
	liveSession *tdataImportSession
	stats       store.ImportStats
	reconcile   chan struct{}
	mu          sync.Mutex // serializes hooks.Live batches
}

// requestReconcile schedules an out-of-band full pass (non-blocking).
func (h *watchHandler) requestReconcile() {
	select {
	case h.reconcile <- struct{}{}:
	default:
	}
}

// Handle implements telegram.UpdateHandler.
func (h *watchHandler) Handle(ctx context.Context, u tg.UpdatesClass) error {
	switch u := u.(type) {
	case *tg.Updates:
		return h.handleContainer(ctx, peer.EntitiesFromResult(u), u.Updates)
	case *tg.UpdatesCombined:
		return h.handleContainer(ctx, peer.EntitiesFromResult(u), u.Updates)
	case *tg.UpdateShort:
		return h.handleContainer(ctx, peer.Entities{}, []tg.UpdateClass{u.Update})
	case *tg.UpdateShortMessage:
		return h.handleContainer(ctx, peer.Entities{}, []tg.UpdateClass{&tg.UpdateNewMessage{Message: shortMessage(h.selfID, u)}})
	case *tg.UpdateShortChatMessage:
		return h.handleContainer(ctx, peer.Entities{}, []tg.UpdateClass{&tg.UpdateNewMessage{Message: shortChatMessage(h.selfID, u)}})
	default:
		return nil
	}
}

// handleContainer converts every new-message update in the batch and hands
// them to hooks.Live in one call. Edits and other update types are skipped in
// v1: the next reconcile pass refreshes them (MergeAll overwrites non-empty
// fields, so no stale duplicates).
func (h *watchHandler) handleContainer(ctx context.Context, ents peer.Entities, updates []tg.UpdateClass) error {
	var (
		messages []store.Message
		chats    []store.Chat
	)
	for _, upd := range updates {
		var msg tg.MessageClass
		switch upd := upd.(type) {
		case *tg.UpdateNewMessage:
			msg = upd.Message
		case *tg.UpdateNewChannelMessage:
			msg = upd.Message
		default:
			continue
		}
		converted, chat, ok := h.convertLiveMessage(ctx, msg, ents)
		if !ok {
			continue
		}
		messages = append(messages, converted)
		chats = append(chats, chat)
	}
	if len(messages) == 0 {
		return nil
	}
	stats := h.stats
	stats.StartedAt = time.Now().UTC()
	stats.FinishedAt = stats.StartedAt
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hooks.Live(ctx, h.st, stats, chats, messages)
}

// convertLiveMessage converts one incoming message through the import
// conversion layer. Entities from the update container fill sender and chat
// names; when absent, ids are used until the next reconcile pass enriches
// them.
func (h *watchHandler) convertLiveMessage(ctx context.Context, msg tg.MessageClass, ents peer.Entities) (store.Message, store.Chat, bool) {
	m, ok := msg.(tg.NotEmptyMessage)
	if !ok {
		return store.Message{}, store.Chat{}, false
	}
	peerID := m.GetPeerID()
	chatID := tdataPeerIDString(peerID, h.selfID)
	if chatID == "" {
		return store.Message{}, store.Chat{}, false
	}
	info := tdataPeerInfo(peerID, ents, h.selfID)
	chatName := firstNonEmpty(info.name, chatID)
	row := tdataDialog{
		chatID:   chatID,
		chatName: chatName,
		kind:     firstNonEmpty(info.kind, "unknown"),
		username: info.username,
		forum:    info.forum,
	}
	converted := h.liveSession.convertMessage(ctx, row, querymessages.Elem{Msg: m, Entities: ents})
	chat := store.Chat{
		JID:           chatID,
		Kind:          row.kind,
		Name:          chatName,
		Username:      row.username,
		Forum:         row.forum,
		LastMessageAt: converted.Timestamp,
	}
	return converted, chat, true
}

// shortMessage expands updateShortMessage into a regular message so the
// conversion layer sees the uniform shape the import path uses.
func shortMessage(selfID int64, u *tg.UpdateShortMessage) *tg.Message {
	msg := &tg.Message{
		ID:      u.ID,
		PeerID:  &tg.PeerUser{UserID: u.UserID},
		Message: u.Message,
		Date:    u.Date,
	}
	msg.SetOut(u.Out)
	if u.Out {
		msg.SetFromID(&tg.PeerUser{UserID: selfID})
	}
	if v, ok := u.GetFwdFrom(); ok {
		msg.SetFwdFrom(v)
	}
	if v, ok := u.GetViaBotID(); ok {
		msg.SetViaBotID(v)
	}
	if v, ok := u.GetReplyTo(); ok {
		msg.SetReplyTo(v)
	}
	if v := u.Entities; len(v) > 0 {
		msg.SetEntities(v)
	}
	if v, ok := u.GetTTLPeriod(); ok {
		msg.SetTTLPeriod(v)
	}
	return msg
}

// shortChatMessage mirrors shortMessage for basic-group chats.
func shortChatMessage(selfID int64, u *tg.UpdateShortChatMessage) *tg.Message {
	msg := &tg.Message{
		ID:      u.ID,
		PeerID:  &tg.PeerChat{ChatID: u.ChatID},
		Message: u.Message,
		Date:    u.Date,
	}
	msg.SetOut(u.Out)
	if u.Out {
		msg.SetFromID(&tg.PeerUser{UserID: selfID})
	} else {
		msg.SetFromID(&tg.PeerUser{UserID: u.FromID})
	}
	if v, ok := u.GetFwdFrom(); ok {
		msg.SetFwdFrom(v)
	}
	if v, ok := u.GetViaBotID(); ok {
		msg.SetViaBotID(v)
	}
	if v, ok := u.GetReplyTo(); ok {
		msg.SetReplyTo(v)
	}
	if v := u.Entities; len(v) > 0 {
		msg.SetEntities(v)
	}
	if v, ok := u.GetTTLPeriod(); ok {
		msg.SetTTLPeriod(v)
	}
	return msg
}

// reconcileLoop runs full fetch passes: one immediately on start (covers the
// pre-daemon gap and first-run backfill), then on the configured period, plus
// out-of-band passes requested by the manager gap callbacks. Pass failures
// are logged, never fatal — the daemon keeps its connection.
func (h *watchHandler) reconcileLoop(ctx context.Context, opts WatchOptions, dbPath string) {
	run := func() {
		if err := h.reconcilePass(ctx, opts, dbPath); err != nil && h.progress != nil {
			fmt.Fprintf(h.progress, "watch: reconcile pass failed: %v\n", err)
		}
	}
	run()
	if opts.ReconcileEvery <= 0 {
		for {
			select {
			case <-ctx.Done():
				return
			case <-h.reconcile:
				run()
			}
		}
	}
	ticker := time.NewTicker(opts.ReconcileEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.reconcile:
			ticker.Reset(opts.ReconcileEvery)
			run()
		case <-ticker.C:
			run()
		}
	}
}

// reconcilePass mirrors the Import()+runImport tail on the watch connection:
// a fresh fetch session with per-pass media staging, then hooks.Reconcile.
func (h *watchHandler) reconcilePass(ctx context.Context, opts WatchOptions, dbPath string) error {
	started := time.Now().UTC()
	mediaStage, err := os.MkdirTemp(filepath.Dir(dbPath), ".telemcp-watch-media-**")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(mediaStage) }()

	passOpts := opts.ImportOptions
	passOpts.MediaArchiveDir = mediaStage
	var downloadTemp string
	if opts.FetchMedia {
		if downloadTemp, err = os.MkdirTemp("", "telemcp-telegram-media-*"); err != nil {
			return err
		}
		defer func() { _ = os.RemoveAll(downloadTemp) }()
	}
	importer := &tdataImportSession{
		raw:          h.raw,
		selfID:       h.selfID,
		opts:         passOpts,
		sourcePath:   h.sourcePath,
		mediaTempDir: downloadTemp,
		existingRefs: tdataExistingMediaRefs(passOpts, h.sourcePath),
	}
	result, err := importer.importAccount(ctx)
	if err != nil {
		return err
	}
	result.Stats.SourcePath = h.sourcePath
	result.Stats.SourceIdentity = h.stats.SourceIdentity
	result.Stats.SourcePathCanonical = true
	result.Stats.DBPath = dbPath
	result.Stats.StartedAt = started
	result.Stats.FinishedAt = time.Now().UTC()
	result.Stats.Chats = len(result.Chats)
	result.Stats.Messages = len(result.Messages)

	archiveDir := importMediaArchiveDir(passOpts, dbPath)
	roots := []string{downloadTemp}
	if len(passOpts.ExistingMediaRefs) != 0 {
		roots = append(roots, mediaArchiveDir(dbPath))
	}
	if err := copyImportedContactAvatars(result.Contacts, archiveDir, roots...); err != nil {
		return err
	}
	if err := copyImportedMedia(result.Messages, archiveDir, &result.Stats, roots...); err != nil {
		return err
	}
	if h.progress != nil {
		fmt.Fprintf(h.progress, "watch: reconcile pass: %d chats, %d messages\n", len(result.Chats), len(result.Messages))
	}
	return h.hooks.Reconcile(ctx, h.st, result, mediaStage)
}

// watchLockPath returns the daemon lock file for an archive.
func watchLockPath(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), "watch.lock")
}

// acquireWatchLock takes an exclusive non-blocking flock on <dbdir>/watch.lock
// and holds it for the daemon lifetime, so a second watch (or a one-shot
// import that respects the guard) cannot race the same Telegram auth key.
func acquireWatchLock(dbPath string) (func() error, error) {
	f, err := os.OpenFile(watchLockPath(dbPath), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, errors.New("telemcp watch is already running for this archive (watch.lock is held); stop it first")
	}
	return func() error {
		defer f.Close()
		return unix.Flock(int(f.Fd()), unix.LOCK_UN)
	}, nil
}

// AcquireConnectionLock exposes the archive connection guard: an exclusive
// non-blocking flock on <dbdir>/watch.lock. Media downloads that open an
// ephemeral Telegram connection (DownloadViaTData) take it so they cannot
// race the watch daemon on the same auth key.
func AcquireConnectionLock(dbPath string) (func() error, error) {
	return acquireWatchLock(dbPath)
}

// daemonPeerHashes serves PeerHashStore from the daemon's persisted updates
// state, bound to the daemon's own user id.
type daemonPeerHashes struct {
	st     *sqliteUpdatesState
	userID int64
}

func (d daemonPeerHashes) ChannelAccessHash(ctx context.Context, channelID int64) (int64, bool, error) {
	return d.st.GetChannelAccessHash(ctx, d.userID, channelID)
}

func (d daemonPeerHashes) UserAccessHash(ctx context.Context, userID int64) (int64, bool, error) {
	return d.st.GetUserAccessHash(ctx, d.userID, userID)
}

// WatchLockHeld reports whether a watch daemon currently holds the archive
// lock. One-shot imports check this to avoid a second connection on the same
// Telegram auth key (AUTH_KEY_DUPLICATED).
func WatchLockHeld(dbPath string) bool {
	f, err := os.OpenFile(watchLockPath(dbPath), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return false
	}
	defer f.Close()
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return true
	}
	_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
	return false
}

// progressLogger surfaces updates-manager warnings (gap recovery failures,
// handler errors) in the daemon log. Records below warn stay silent: the
// manager is chatty at debug/info and the reconcile pass already reports
// progress itself.
type progressLogger struct {
	w io.Writer
}

func progressLoggerOf(w io.Writer) progressLogger { return progressLogger{w: w} }

func (l progressLogger) Enabled(_ context.Context, level gotdlog.Level) bool {
	return l.w != nil && level >= gotdlog.LevelWarn
}

func (l progressLogger) Log(_ context.Context, level gotdlog.Level, msg string, attrs ...gotdlog.Attr) {
	if !l.Enabled(nil, level) {
		return
	}
	var pairs strings.Builder
	for _, a := range attrs {
		pairs.WriteString(" ")
		pairs.WriteString(a.Key)
		pairs.WriteString("=")
		pairs.WriteString(a.Value.String())
	}
	fmt.Fprintf(l.w, "watch: updates: %s: %s%s\n", level, msg, pairs.String())
}

// sqliteUpdatesState adapts the plain-typed store persistence to the gotd
// updates manager interfaces (state storage + access hashers).
type sqliteUpdatesState struct {
	st *store.UpdatesStateStore
}

func newSQLiteUpdatesState(st *store.Store) *sqliteUpdatesState {
	return &sqliteUpdatesState{st: store.NewUpdatesStateStore(st)}
}

func (s *sqliteUpdatesState) GetState(ctx context.Context, userID int64) (updates.State, bool, error) {
	state, found, err := s.st.GetUpdatesState(ctx, userID)
	if err != nil || !found {
		return updates.State{}, false, err
	}
	return updates.State{Pts: state.Pts, Qts: state.Qts, Date: state.Date, Seq: state.Seq}, true, nil
}

func (s *sqliteUpdatesState) SetState(ctx context.Context, userID int64, state updates.State) error {
	return s.st.SetUpdatesState(ctx, userID, store.UpdatesState{
		Pts:  state.Pts,
		Qts:  state.Qts,
		Date: state.Date,
		Seq:  state.Seq,
	})
}

func (s *sqliteUpdatesState) SetPts(ctx context.Context, userID int64, pts int) error {
	return s.st.SetUpdatesPts(ctx, userID, pts)
}

func (s *sqliteUpdatesState) SetQts(ctx context.Context, userID int64, qts int) error {
	return s.st.SetUpdatesQts(ctx, userID, qts)
}

func (s *sqliteUpdatesState) SetDate(ctx context.Context, userID int64, date int) error {
	return s.st.SetUpdatesDate(ctx, userID, date)
}

func (s *sqliteUpdatesState) SetSeq(ctx context.Context, userID int64, seq int) error {
	return s.st.SetUpdatesSeq(ctx, userID, seq)
}

func (s *sqliteUpdatesState) SetDateSeq(ctx context.Context, userID int64, date, seq int) error {
	return s.st.SetUpdatesDateSeq(ctx, userID, date, seq)
}

func (s *sqliteUpdatesState) GetChannelPts(ctx context.Context, userID, channelID int64) (int, bool, error) {
	return s.st.GetChannelPts(ctx, userID, channelID)
}

func (s *sqliteUpdatesState) SetChannelPts(ctx context.Context, userID, channelID int64, pts int) error {
	return s.st.SetChannelPts(ctx, userID, channelID, pts)
}

func (s *sqliteUpdatesState) ForEachChannels(ctx context.Context, userID int64, f func(ctx context.Context, channelID int64, pts int) error) error {
	return s.st.ForEachUpdatesChannels(ctx, userID, f)
}

func (s *sqliteUpdatesState) SetChannelAccessHash(ctx context.Context, userID, channelID, accessHash int64) error {
	return s.st.SetChannelAccessHash(ctx, userID, channelID, accessHash)
}

func (s *sqliteUpdatesState) GetChannelAccessHash(ctx context.Context, userID, channelID int64) (int64, bool, error) {
	return s.st.GetChannelAccessHash(ctx, userID, channelID)
}

func (s *sqliteUpdatesState) SetUserAccessHash(ctx context.Context, userID, targetUserID, accessHash int64) error {
	return s.st.SetUserAccessHash(ctx, userID, targetUserID, accessHash)
}

func (s *sqliteUpdatesState) GetUserAccessHash(ctx context.Context, userID, targetUserID int64) (int64, bool, error) {
	return s.st.GetUserAccessHash(ctx, userID, targetUserID)
}
