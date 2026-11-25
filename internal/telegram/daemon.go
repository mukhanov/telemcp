package telegram

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"
	"github.com/mukhanov/telemcp/internal/store"
	"golang.org/x/sync/errgroup"
)

// watchDaemon is one Watch run: the pieces that outlive a single connection
// (control socket, reconcile schedule, channel pts freshness) and the
// supervisor that rebuilds the connection as often as the network demands.
type watchDaemon struct {
	opts       WatchOptions
	hooks      WatchHooks
	st         *store.Store
	dbPath     string
	source     AuthSource
	creds      AppCredentials
	storage    telegram.SessionStorage
	stateStore *sqliteUpdatesState
	log        watchStateLog

	state     *watchStatePublisher
	sup       *watchSupervisor
	reconcile reconcileScheduler
	bootstrap channelBootstrap
	downloads liveDownloader
}

// run serves the archive until ctx is cancelled. The control socket and the
// reconcile schedule live for the whole run; the connection is rebuilt as
// often as the network demands.
func (d *watchDaemon) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	d.state = newWatchStatePublisher(d.dbPath, d.log)
	defer d.state.remove()
	d.reconcile.start(ctx)

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		cleanup, err := serveControlSocket(gctx, d.dbPath, &d.downloads, d.log.printf)
		if err != nil {
			// Downloads still work without the socket (the MCP server
			// falls back to an ephemeral connection); never fatal.
			d.log.printf("media requests unavailable: %v", err)
			<-gctx.Done()
			return nil
		}
		d.log.printf("media requests on %s", ControlSocketPath(d.dbPath))
		defer cleanup()
		<-gctx.Done()
		return nil
	})
	g.Go(func() error { return d.supervise(gctx) })
	return g.Wait()
}

// supervise rebuilds the connection until ctx is cancelled.
func (d *watchDaemon) supervise(ctx context.Context) error {
	maxWait := d.opts.ReconnectMaxWait
	if maxWait <= 0 {
		maxWait = DefaultReconnectMaxWait
	}
	s := &watchSupervisor{
		maxWait: maxWait,
		log:     d.log,
		state:   d.state,
		now:     time.Now,
		jitter:  randomJitter,
		routed:  routeToTelegram,
	}
	s.wait = s.waitFor
	d.sup = s
	return s.run(ctx, d.connect)
}

// connected records the first answer of the current connection.
func (d *watchDaemon) connected() {
	d.sup.live()
}

// connect is one connection lifetime. It returns once the connection is gone
// for good — Telegram rejected the session, or the supervisor replaced a
// stalled one — with how long the connection answered.
func (d *watchDaemon) connect(ctx context.Context) (time.Duration, error) {
	handler := &watchHandler{
		hooks:      d.hooks,
		st:         d.st,
		progress:   d.opts.Progress,
		sourcePath: d.source.Path,
		liveOpts:   liveImportOptions(d.opts.ImportOptions),
	}
	manager := updates.New(updates.Config{
		Handler:          handler,
		Logger:           progressLoggerOf(d.opts.Progress),
		Storage:          d.stateStore,
		AccessHasher:     d.stateStore,
		UserAccessHasher: d.stateStore,
		// The manager cannot self-recover from these gaps; a full pass can.
		OnChannelTooLong:      func(int64) { d.reconcile.requestPass() },
		OnTooLong:             func() { d.reconcile.requestPass() },
		OnLoadUserStateFailed: func() { d.reconcile.requestPass() },
	})
	health, connCtx := newConnHealth(ctx, defaultConnHealthConfig(), d.log, d.connected)
	defer health.stop()

	// Setting UpdateHandler switches NoUpdates off; one connection then
	// feeds both the live path and the reconcile passes.
	client := telegram.NewClient(int(d.creds.ID), d.creds.Hash, telegram.Options{
		SessionStorage:      d.storage,
		UpdateHandler:       manager,
		AllowCDN:            true,
		Middlewares:         []telegram.Middleware{newTelegramFloodWaitPolicy(d.opts.Progress), health},
		Device:              telegramDeviceConfig(),
		DialTimeout:         dialTimeout,
		ReconnectionBackoff: innerReconnectBackoff,
		OnConnectionState:   health.connectionState,
	})
	err := client.Run(connCtx, func(ctx context.Context) error {
		self, err := client.Self(ctx)
		if err != nil {
			return fmt.Errorf("telegram session is not authorized: %w", err)
		}
		handler.selfID = self.ID
		handler.raw = tg.NewClient(client)
		handler.stats = store.ImportStats{
			SourcePath:          d.source.Identity(),
			SourcePathCanonical: true,
			SourceIdentity:      sourceIdentity(d.source.Kind, strconv.FormatInt(self.ID, 10)),
			SourceUserID:        self.ID,
			DBPath:              d.dbPath,
		}
		handler.liveSession = &tdataImportSession{
			raw:        handler.raw,
			selfID:     self.ID,
			opts:       handler.liveOpts,
			sourcePath: d.source.Path,
		}
		if d.opts.Progress != nil {
			fmt.Fprintf(d.opts.Progress, "watch: authorized as %d; receiving live updates\n", self.ID)
		}
		health.setProbe(func(ctx context.Context) error {
			_, err := handler.raw.UpdatesGetState(ctx)
			return err
		})
		// One shared fetcher for the control socket: dialog peer cache is
		// per instance, and it is safe for concurrent use.
		d.downloads.set(NewMediaFetcherWithHashes(handler.raw, self.ID, daemonPeerHashes{st: d.stateStore, userID: self.ID}))
		defer d.downloads.set(nil)
		d.reconcile.attach(handler, ctx)
		defer d.reconcile.detach()

		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			handler.bootstrapChannelPts(ctx, d.stateStore, &d.bootstrap)
			return manager.Run(gctx, handler.raw, self.ID, updates.AuthOptions{})
		})
		return g.Wait()
	})

	live := health.liveFor(time.Now())
	switch {
	case ctx.Err() != nil:
		return 0, ctx.Err() // shutting down
	case health.stalled() != nil:
		return live, health.stalled()
	case err != nil:
		return live, err
	default:
		return live, errConnectionClosed
	}
}

// reconcileMinSpacing is how soon after a finished pass a gap-triggered pass
// may run: a flapping connection raises the same gaps on every return.
const reconcileMinSpacing = 5 * time.Minute

// reconcileScheduler runs full reconcile passes on its own schedule,
// whichever connection is current. A connection loss does not start a pass:
// the updates manager's getDifference covers the gap once it is back.
type reconcileScheduler struct {
	opts   WatchOptions
	dbPath string
	pass   func(context.Context, *watchHandler) error

	requests chan struct{}
	mu       sync.Mutex
	handler  *watchHandler
	connCtx  context.Context
	lastDone time.Time // end of the last completed pass
}

func newReconcileScheduler(opts WatchOptions, dbPath string) reconcileScheduler {
	return reconcileScheduler{
		opts:   opts,
		dbPath: dbPath,
		pass: func(ctx context.Context, h *watchHandler) error {
			return h.reconcilePass(ctx, opts, dbPath)
		},
		requests: make(chan struct{}, 1),
	}
}

func (s *reconcileScheduler) start(ctx context.Context) {
	go s.loop(ctx)
}

// requestPass asks for an out-of-band full pass; repeats are coalesced.
func (s *reconcileScheduler) requestPass() {
	select {
	case s.requests <- struct{}{}:
	default:
	}
}

// attach hands the scheduler a live connection. The startup pass, and a pass
// after the schedule went stale offline, are requested right away.
func (s *reconcileScheduler) attach(h *watchHandler, connCtx context.Context) {
	s.mu.Lock()
	s.handler, s.connCtx = h, connCtx
	stale := s.lastDone.IsZero() || s.opts.ReconcileEvery > 0 && time.Since(s.lastDone) >= s.opts.ReconcileEvery
	s.mu.Unlock()
	if stale {
		s.requestPass()
	}
}

// detach takes the connection away: a pass still running ends with it.
func (s *reconcileScheduler) detach() {
	s.mu.Lock()
	s.handler = nil
	s.mu.Unlock()
}

// loop runs one pass at a time: one when a connection is first attached,
// then every ReconcileEvery, plus the requested ones.
func (s *reconcileScheduler) loop(ctx context.Context) {
	for s.awaitPass(ctx) {
		s.runPass(ctx)
	}
}

// awaitPass waits for the next pass to be due — the periodic one, or a
// requested one that respects the spacing — and reports false once ctx
// ends.
func (s *reconcileScheduler) awaitPass(ctx context.Context) bool {
	var tick <-chan time.Time
	if s.opts.ReconcileEvery > 0 {
		// A fresh timer per pass: draining a reused one after its tick was
		// consumed blocks forever.
		timer := time.NewTimer(s.opts.ReconcileEvery)
		defer timer.Stop()
		tick = timer.C
	}
	for {
		select {
		case <-ctx.Done():
			return false
		case <-s.requests:
			if s.requestDue() {
				return true
			}
		case <-tick:
			return true
		}
	}
}

// requestDue reports whether a requested pass may run now: not within
// reconcileMinSpacing of the last completed one.
func (s *reconcileScheduler) requestDue() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastDone.IsZero() || time.Since(s.lastDone) >= reconcileMinSpacing
}

func (s *reconcileScheduler) runPass(ctx context.Context) {
	s.mu.Lock()
	h, connCtx := s.handler, s.connCtx
	s.mu.Unlock()
	if h == nil {
		return // offline: nothing to pass with
	}
	// The pass ends with its connection; one cut short is not a failure.
	passCtx, cancel := context.WithCancel(connCtx)
	defer cancel()
	defer context.AfterFunc(ctx, cancel)()
	err := s.pass(passCtx, h)
	cancel()
	switch {
	case err != nil && passCtx.Err() == nil:
		if s.opts.Progress != nil {
			fmt.Fprintf(s.opts.Progress, "watch: reconcile pass failed: %v\n", err)
		}
	case err == nil:
		s.mu.Lock()
		s.lastDone = time.Now()
		s.mu.Unlock()
	}
}

// bootstrapFreshFor is how long a checked channel's pts stays trusted across
// reconnects.
const bootstrapFreshFor = 24 * time.Hour

// channelBootstrap remembers when each channel's pts was last checked, so
// that a reconnect does not repeat the sweep over every channel.
type channelBootstrap struct {
	mu      sync.Mutex
	checked map[int64]time.Time
}

// due reports whether the channel's pts needs checking.
func (b *channelBootstrap) due(channelID int64, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	checked, ok := b.checked[channelID]
	return !ok || now.Sub(checked) >= bootstrapFreshFor
}

// verified records a check that Telegram answered.
func (b *channelBootstrap) verified(channelID int64, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.checked == nil {
		b.checked = make(map[int64]time.Time)
	}
	b.checked[channelID] = now
}

// liveDownloader serves control-socket downloads from the current
// connection. The socket outlives connections, so between two of them a
// request fails with its reason instead of hanging.
type liveDownloader struct {
	mu      sync.RWMutex
	fetcher mediaDownloader
}

func (d *liveDownloader) set(fetcher mediaDownloader) {
	d.mu.Lock()
	d.fetcher = fetcher
	d.mu.Unlock()
}

func (d *liveDownloader) FetchChatMedia(ctx context.Context, chatID string, msgIDs []int, opts DownloadOptions) ([]MediaDownload, error) {
	d.mu.RLock()
	fetcher := d.fetcher
	d.mu.RUnlock()
	if fetcher == nil {
		return nil, errNoConnection
	}
	return fetcher.FetchChatMedia(ctx, chatID, msgIDs, opts)
}
