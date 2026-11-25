package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// Connection timing. gotd reconnects by itself, with a 100ms..5s backoff and
// no limit; the daemon paces that loop and takes over from it when the
// connection is gone for longer than gotd can mend.
const (
	// DefaultReconnectMaxWait caps the wait between two supervisor
	// attempts; --reconnect-max-wait overrides it. The cap is what bounds
	// the lag after a network returns without the daemon noticing.
	DefaultReconnectMaxWait = 60 * time.Second

	// innerReconnectMin and innerReconnectMax pace gotd's own loop.
	innerReconnectMin = time.Second
	innerReconnectMax = 15 * time.Second
	// dialTimeout bounds one connect attempt; gotd's 35s default holds
	// every attempt that long when the tunnel is up but dead.
	dialTimeout = 10 * time.Second

	// stalledAfter is how long a connection may go without an answer from
	// Telegram before the supervisor replaces it. gotd's ping declares a
	// half-open connection dead within its 60s interval plus 15s timeout,
	// so a connection that is merely reconnecting keeps its chance.
	stalledAfter = 2 * time.Minute
	// A connection quiet for probeEvery gets one cheap request: "no answer"
	// is measured, not assumed.
	probeEvery   = time.Minute
	probeTimeout = 15 * time.Second

	// reconnectBase is the first supervisor wait, once gotd has failed for
	// stalledAfter; it doubles up to the cap.
	reconnectBase = 15 * time.Second
	// An attempt that answered for stableAfter resets the backoff, so a
	// flapping network never gets the short early waits back.
	stableAfter = 2 * time.Minute
	// sessionDeadRetry spaces the attempts with a session Telegram rejected:
	// only `telemcp login` repairs one.
	sessionDeadRetry = 30 * time.Minute
	// authKeyConflictWait is the least wait after AUTH_KEY_DUPLICATED: the
	// key was in use from two places at once. A key that really died
	// answers 401 on the next attempt; a false alarm is then just waited out.
	authKeyConflictWait = 5 * time.Minute

	// networkPollEvery is how often the supervisor compares the network
	// while it waits, to retry at once when the network returns.
	networkPollEvery = 2 * time.Second
	// networkRetryMin spaces two retries that a network change brought on.
	networkRetryMin = 5 * time.Second
	// routeGateSkips bounds how many attempts the route check may suppress:
	// a routing-table quirk must never keep the daemon from dialing.
	routeGateSkips = 3
)

// telegramRouteTarget is DC 2, the data center the client connects to. The
// supervisor only asks the routing table about it.
const telegramRouteTarget = "149.154.167.41:443"

// The daemon's connection states, as `telemcp status` reports them.
const (
	WatchStateConnecting  = "connecting"
	WatchStateLive        = "live"
	WatchStateWaiting     = "waiting"
	WatchStateSessionDead = "session_dead"
)

var (
	// errConnectionStalled is why the supervisor replaced a connection.
	errConnectionStalled = errors.New("Telegram connection stalled")
	// errConnectionClosed covers a client run that ended without an error.
	errConnectionClosed = errors.New("Telegram connection closed")
	// errNoConnection answers a control-socket download while the daemon is
	// between two connections.
	errNoConnection = errors.New("Telegram connection is down; the daemon reconnects on its own, retry shortly")
)

// WatchState is the daemon's connection state, written next to the archive
// for `telemcp status`. Since is when the current state began; Attempts
// is how many connections the daemon has tried since it started.
type WatchState struct {
	PID         int       `json:"pid"`
	State       string    `json:"state"`
	Started     time.Time `json:"started"`
	Since       time.Time `json:"since"`
	Attempts    int       `json:"attempts"`
	LastError   string    `json:"last_error,omitempty"`
	LastDrop    time.Time `json:"last_drop,omitzero"`
	NextAttempt time.Time `json:"next_attempt,omitzero"`
}

func watchStatePath(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), "watch.state")
}

// ReadWatchState returns the state the daemon published, or nil when there
// is none.
func ReadWatchState(dbPath string) (*WatchState, error) {
	data, err := os.ReadFile(watchStatePath(dbPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var st WatchState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("parse %s: %w", watchStatePath(dbPath), err)
	}
	return &st, nil
}

// watchStatePublisher keeps the state file current for `telemcp status`.
// Every change rewrites it whole through a rename, so a reader never sees
// half a file.
type watchStatePublisher struct {
	path  string
	log   watchStateLog
	mu    sync.Mutex
	st    WatchState
	fails int
}

func newWatchStatePublisher(dbPath string, log watchStateLog) *watchStatePublisher {
	return &watchStatePublisher{path: watchStatePath(dbPath), log: log}
}

// update applies f and republishes the state.
func (p *watchStatePublisher) update(f func(*WatchState)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f(&p.st)
	data, err := json.Marshal(&p.st)
	if err == nil {
		err = replaceFile(p.path, data)
	}
	if err != nil && p.fails == 0 {
		p.log.printf("cannot publish the connection state to %s: %v", p.path, err)
	}
	if err != nil {
		p.fails++
	}
}

func (p *watchStatePublisher) remove() {
	_ = os.Remove(p.path)
}

// replaceFile writes data to path through a temporary file and a rename.
func replaceFile(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err == nil {
		err = tmp.Close()
	}
	if err == nil {
		err = os.Chmod(tmpPath, 0o600)
	}
	if err == nil {
		err = os.Rename(tmpPath, path)
	}
	if err != nil {
		_ = os.Remove(tmpPath)
	}
	return err
}

// watchStateLog writes the connection's life to the daemon log with a
// timestamp, which the other daemon lines lack: reconnects are only readable
// afterwards if their order and timing are on record.
type watchStateLog struct {
	w io.Writer
}

func (l watchStateLog) printf(format string, a ...any) {
	if l.w == nil {
		return
	}
	_, _ = fmt.Fprintf(l.w, "watch: %s %s\n", time.Now().Format(time.RFC3339), fmt.Sprintf(format, a...))
}

// innerReconnectBackoff paces gotd's own reconnection loop.
func innerReconnectBackoff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = innerReconnectMin
	b.MaxInterval = innerReconnectMax
	b.MaxElapsedTime = 0
	return b
}

// reconnectBackoff is the supervisor's wait between attempts: it doubles
// from base to max, and at least half of every step stays random, so that
// drops of different daemons never line up.
type reconnectBackoff struct {
	base, max time.Duration
	jitter    func(time.Duration) time.Duration
	n         int
}

func (b *reconnectBackoff) next() time.Duration {
	step := b.max
	if b.n < 20 {
		if next := b.base << b.n; next > 0 && next < b.max {
			step = next
			b.n++
		}
	}
	return step - b.jitter(step/2)
}

func (b *reconnectBackoff) reset() {
	b.n = 0
}

func randomJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(max)))
}

// isSessionDead reports whether Telegram rejected the session itself: the
// auth key was revoked, expired or unregistered. No reconnection recovers
// that; only `telemcp login` does.
func isSessionDead(err error) bool {
	return tgerr.IsCode(err, 401)
}

// watchSupervisor owns the daemon's connection lifetime. It starts a fresh
// connection after every loss, waits between attempts instead of hammering,
// and never ends the process because the network failed.
type watchSupervisor struct {
	maxWait time.Duration
	log     watchStateLog
	state   *watchStatePublisher
	// Seams for tests.
	now    func() time.Time
	jitter func(time.Duration) time.Duration
	routed func() bool
	wait   func(context.Context, time.Duration) error

	lastWake time.Time
}

// run drives attempts until ctx is cancelled. attempt is one connection
// lifetime and reports how long the connection answered.
func (s *watchSupervisor) run(ctx context.Context, attempt func(context.Context) (time.Duration, error)) error {
	now := s.now()
	s.state.update(func(st *WatchState) {
		st.PID, st.Started, st.Since, st.State = os.Getpid(), now, now, WatchStateConnecting
	})

	backoff := reconnectBackoff{base: min(reconnectBase, s.maxWait), max: s.maxWait, jitter: s.jitter}
	gated := 0
	for reconnects := 0; ; reconnects++ {
		if s.routed() {
			gated = 0
		} else if gated < routeGateSkips {
			// No route to Telegram: a dial would only fail, so wait for the
			// network instead. Bounded, in case the routing table lies.
			gated++
			d := backoff.next()
			s.log.printf("no route to Telegram; waiting for the network (rechecked in %s)", d)
			s.publish(WatchStateWaiting, "no route to Telegram", s.now().Add(d))
			if err := s.wait(ctx, d); err != nil {
				return nil
			}
			continue
		}
		if reconnects > 0 {
			s.log.printf("reconnecting (attempt %d)", reconnects+1)
		}
		s.publish(WatchStateConnecting, "", time.Time{})
		live, err := attempt(ctx)
		if ctx.Err() != nil {
			return nil // shutting down
		}
		now = s.now()
		if live >= stableAfter {
			backoff.reset()
		}
		d := backoff.next()
		next, lost := WatchStateWaiting, "connection lost"
		switch {
		case isSessionDead(err):
			next, lost = WatchStateSessionDead, "Telegram rejected the session"
			d = sessionDeadRetry + s.jitter(time.Minute)
			s.log.printf("%s: %v; next attempt in %s — run 'telemcp login' to authorize again", lost, err, d.Round(time.Second))
		case tgerr.Is(err, "AUTH_KEY_DUPLICATED"):
			d = max(d, authKeyConflictWait)
			s.log.printf("connection lost after %s: the session was in use from two places at once; next attempt in %s", live.Round(time.Second), d.Round(time.Second))
		default:
			s.log.printf("connection lost after %s: %v; next attempt in %s", live.Round(time.Second), err, d.Round(time.Second))
		}
		s.publish(next, fmt.Sprintf("%s: %v", lost, err), now.Add(d))
		if err := s.wait(ctx, d); err != nil {
			return nil
		}
	}
}

// live records the first answer of the current connection: the daemon is
// syncing again.
func (s *watchSupervisor) live() {
	now := s.now()
	s.log.printf("connected")
	s.state.update(func(st *WatchState) { st.State, st.Since = WatchStateLive, now })
}

// publish records a state change with its cause and, for a waiting state,
// when the daemon tries again.
func (s *watchSupervisor) publish(state, cause string, next time.Time) {
	now := s.now()
	s.state.update(func(st *WatchState) {
		if st.State == WatchStateLive && state != WatchStateConnecting {
			st.LastDrop = now
		}
		st.State, st.Since, st.NextAttempt = state, now, next
		if cause != "" {
			st.LastError = cause
		}
		if state == WatchStateConnecting {
			st.Attempts++
		}
	})
}

// waitFor waits d, or less when the network changes: the next attempt is
// what tells whether the network is back.
func (s *watchSupervisor) waitFor(ctx context.Context, d time.Duration) error {
	changes := make(chan struct{}, 1)
	stop := watchNetwork(ctx, changes)
	defer stop()
	timer := time.NewTimer(d)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		case <-changes:
			if time.Since(s.lastWake) < networkRetryMin {
				continue
			}
			s.lastWake = time.Now()
			s.log.printf("network changed; retrying sooner")
			return nil
		}
	}
}

// watchNetwork reports network changes on changes, at most one per poll,
// until stop is called. Waking from sleep counts: the network very likely
// moved meanwhile.
func watchNetwork(ctx context.Context, changes chan<- struct{}) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		base, lastTick := networkFingerprint(), time.Now().Round(0)
		ticker := time.NewTicker(networkPollEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case tick := <-ticker.C:
				now := tick.Round(0)
				changed := now.Sub(lastTick) > 10*networkPollEvery
				lastTick = now
				if fp := networkFingerprint(); fp != base {
					base, changed = fp, true
				}
				if changed {
					select {
					case changes <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
	return cancel
}

// networkFingerprint summarizes the interfaces — addresses appearing or
// vanishing, an interface going up or down. Reading it sends no packets.
func networkFingerprint() string {
	addrs, _ := net.InterfaceAddrs()
	ifaces, _ := net.Interfaces()
	parts := make([]string, 0, len(addrs)+len(ifaces))
	for _, a := range addrs {
		parts = append(parts, a.String())
	}
	for _, i := range ifaces {
		parts = append(parts, fmt.Sprintf("%s %s %d", i.Name, i.Flags, i.MTU))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\n")
}

// routeToTelegram reports whether the routing table has a path to Telegram.
// A UDP socket to that address only consults the table; nothing is sent.
func routeToTelegram() bool {
	conn, err := net.DialTimeout("udp4", telegramRouteTarget, 2*time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// connHealthConfig is the timing of connHealth; tests shrink it.
type connHealthConfig struct {
	probeEvery   time.Duration
	probeTimeout time.Duration
	stalledAfter time.Duration
}

func defaultConnHealthConfig() connHealthConfig {
	return connHealthConfig{probeEvery: probeEvery, probeTimeout: probeTimeout, stalledAfter: stalledAfter}
}

// connHealth measures one connection: every answer from Telegram renews it,
// and a connection that stops answering is cancelled so the supervisor
// replaces it. It takes the place of a health check that ended the process
// on the first failed request.
type connHealth struct {
	cfg    connHealthConfig
	log    watchStateLog
	cancel context.CancelFunc

	mu         sync.Mutex
	probe      func(context.Context) error
	lastAlive  time.Time // the last answer from Telegram
	firstAlive time.Time // zero until the first answer
	lastWall   time.Time // wall clock at the previous tick: a jump is sleep
	onLive     func()
	ready      bool
	everReady  bool
	stallErr   error
}

// newConnHealth starts watching a connection. The returned context scopes
// it: cancelling it ends the connection.
func newConnHealth(parent context.Context, cfg connHealthConfig, log watchStateLog, onLive func()) (*connHealth, context.Context) {
	ctx, cancel := context.WithCancel(parent)
	now := time.Now()
	h := &connHealth{cfg: cfg, log: log, cancel: cancel, lastAlive: now, lastWall: now.Round(0), onLive: onLive}
	go h.loop(ctx)
	return h, ctx
}

// Handle implements telegram.Middleware: it counts every answer.
func (h *connHealth) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		err := next.Invoke(ctx, input, output)
		if err == nil || serverAnswered(err) {
			h.answered()
		}
		return err
	}
}

// serverAnswered reports whether err is Telegram's reply to the request —
// a flood wait, an access error: the connection carried it, so it is alive.
func serverAnswered(err error) bool {
	var rpcErr *tgerr.Error
	return errors.As(err, &rpcErr)
}

// setProbe installs the request a quiet connection is probed with.
func (h *connHealth) setProbe(probe func(context.Context) error) {
	h.mu.Lock()
	h.probe = probe
	h.mu.Unlock()
}

func (h *connHealth) answered() {
	h.mu.Lock()
	now := time.Now()
	h.lastAlive = now
	first := h.firstAlive.IsZero()
	if first {
		h.firstAlive = now
	}
	onLive := h.onLive
	h.onLive = nil
	h.mu.Unlock()
	if first && onLive != nil {
		onLive()
	}
}

// connectionState is telegram.Options.OnConnectionState.
func (h *connHealth) connectionState(state telegram.ConnectionState) {
	h.mu.Lock()
	var lost, restored bool
	switch state {
	case telegram.ConnectionStateReady:
		restored = h.everReady
		h.ready, h.everReady = true, true
	case telegram.ConnectionStateDisconnected:
		lost = h.everReady
		h.ready = false
	}
	h.mu.Unlock()
	if lost {
		h.log.printf("connection lost; gotd is reconnecting")
	}
	if restored {
		h.log.printf("connection restored")
	}
}

func (h *connHealth) loop(ctx context.Context) {
	// Waking on a deadline rather than on a ticker makes the reset land
	// stalledAfter after the last answer, not on some later tick.
	timer := time.NewTimer(h.cfg.probeEvery)
	defer timer.Stop()
	var lastProbe time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-timer.C:
			idle, ready := h.measure(now)
			if idle >= h.cfg.stalledAfter {
				h.stall(idle)
				return
			}
			if ready && idle >= h.cfg.probeEvery && now.Sub(lastProbe) >= h.cfg.probeEvery {
				lastProbe = now
				if h.probeConnection(ctx) {
					return
				}
			}
			timer.Reset(h.nextWake(ready, lastProbe))
		}
	}
}

// nextWake is how long the loop may sleep: until the connection would count
// as stalled, or until a quiet connection is due for its next probe,
// whichever comes first.
func (h *connHealth) nextWake(ready bool, lastProbe time.Time) time.Duration {
	h.mu.Lock()
	wake, alive := h.lastAlive.Add(h.cfg.stalledAfter), h.lastAlive
	h.mu.Unlock()
	if ready {
		probe := lastProbe
		if alive.After(probe) {
			probe = alive
		}
		if next := probe.Add(h.cfg.probeEvery); next.Before(wake) {
			wake = next
		}
	}
	if d := time.Until(wake); d > time.Millisecond {
		return d
	}
	return time.Millisecond
}

// measure returns how long the connection went without an answer and
// whether gotd considers it ready. A machine that slept wakes to a stale
// window: it starts over, so gotd gets its own chance to replace the
// connection before the supervisor pays for a new one.
func (h *connHealth) measure(tick time.Time) (idle time.Duration, ready bool) {
	wall := tick.Round(0)
	h.mu.Lock()
	defer h.mu.Unlock()
	if wall.Sub(h.lastWall) > 2*h.cfg.probeEvery+10*time.Second {
		h.lastAlive = wall
	}
	h.lastWall = wall
	return wall.Sub(h.lastAlive), h.ready
}

func (h *connHealth) stall(idle time.Duration) {
	h.mu.Lock()
	h.stallErr = fmt.Errorf("%w: no answer for %s", errConnectionStalled, idle.Round(time.Second))
	h.mu.Unlock()
	h.log.printf("no answer from Telegram for %s; resetting the connection", idle.Round(time.Second))
	h.cancel()
}

// probeConnection asks a quiet connection one cheap request. It reports that
// the connection is dead in a way gotd does not notice: gotd still calls it
// ready, yet the request got no answer. Once gotd has reported a loss, the
// supervisor leaves the reconnection to it until the stall deadline.
func (h *connHealth) probeConnection(ctx context.Context) (dead bool) {
	h.mu.Lock()
	probe := h.probe
	h.mu.Unlock()
	if probe == nil {
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, h.cfg.probeTimeout)
	defer cancel()
	err := probe(probeCtx)
	if err == nil || ctx.Err() != nil {
		return false
	}
	if !serverAnswered(err) && h.isReady() {
		h.stall(h.idle(time.Now()))
		return true
	}
	h.log.printf("no answer to the liveness probe: %v", err)
	return false
}

func (h *connHealth) isReady() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ready
}

// idle is how long the connection went without an answer, at now.
func (h *connHealth) idle(now time.Time) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	return now.Sub(h.lastAlive)
}

// liveFor is how long the connection answered, up to now.
func (h *connHealth) liveFor(now time.Time) time.Duration {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.firstAlive.IsZero() {
		return 0
	}
	return now.Sub(h.firstAlive)
}

// stalled reports the supervisor's reason for ending the connection, if it did.
func (h *connHealth) stalled() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stallErr
}

func (h *connHealth) stop() {
	h.cancel()
}
