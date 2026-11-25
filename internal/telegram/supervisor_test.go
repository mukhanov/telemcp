package telegram

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tgerr"
)

func TestReconnectBackoff(t *testing.T) {
	const (
		base = 15 * time.Second
		cap  = 60 * time.Second
	)
	noJitter := func(time.Duration) time.Duration { return 0 }

	b := reconnectBackoff{base: base, max: cap, jitter: noJitter}
	var got []time.Duration
	for range 4 {
		got = append(got, b.next())
	}
	// Every loss doubles the wait up to the cap, where it stays.
	if want := []time.Duration{base, 2 * base, cap, cap}; !slices.Equal(got, want) {
		t.Fatalf("waits = %v, want %v", got, want)
	}
	b.reset()
	if got := b.next(); got != base {
		t.Fatalf("wait after a stable connection = %s, want the base %s", got, base)
	}

	// The jitter is what keeps the second half of a step.
	b = reconnectBackoff{base: base, max: cap, jitter: func(half time.Duration) time.Duration { return half }}
	for i, want := range []time.Duration{base / 2, base, cap / 2, cap / 2} {
		if got := b.next(); got != want {
			t.Fatalf("wait %d = %s, want %s", i, got, want)
		}
	}

	// A cap below the base leaves nothing to grow into.
	b = reconnectBackoff{base: base, max: 2 * time.Second, jitter: noJitter}
	if got := b.next(); got != 2*time.Second {
		t.Fatalf("wait under a low cap = %s, want the cap", got)
	}
}

// scriptedAttempt is one connection lifetime in a supervisor test.
type scriptedAttempt struct {
	live time.Duration
	err  error
}

// runSupervisor drives the supervisor through the scripted connections.
// Waits are recorded instead of slept and advance a fake clock; the wait
// after the last scripted connection shuts the daemon down.
func runSupervisor(t *testing.T, routed bool, script ...scriptedAttempt) (waits []time.Duration, state *WatchState, log string) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "telemcp.db")
	var logs strings.Builder
	clock := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &watchSupervisor{
		maxWait: DefaultReconnectMaxWait,
		log:     watchStateLog{w: &logs},
		state:   newWatchStatePublisher(dbPath, watchStateLog{w: &logs}),
		now:     func() time.Time { return clock },
		jitter:  func(time.Duration) time.Duration { return 0 },
		routed:  func() bool { return routed },
	}
	next := 0
	s.wait = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		clock = clock.Add(d)
		if next == len(script) {
			cancel()
			return ctx.Err()
		}
		return nil
	}
	if err := s.run(ctx, func(context.Context) (time.Duration, error) {
		if next >= len(script) {
			return 0, errors.New("attempt beyond the script")
		}
		a := script[next]
		if a.live > 0 {
			s.live() // the connection answered
		}
		next++
		clock = clock.Add(a.live)
		return a.live, a.err
	}); err != nil {
		t.Fatalf("run: %v", err)
	}
	published, err := ReadWatchState(dbPath)
	if err != nil {
		t.Fatalf("read published state: %v", err)
	}
	return waits, published, logs.String()
}

func TestWatchSupervisorBacksOffAndResetsAfterStableConnection(t *testing.T) {
	brokenPipe := errors.New("write tcp: broken pipe")
	waits, state, log := runSupervisor(t, true,
		scriptedAttempt{live: 5 * time.Minute, err: brokenPipe},
		scriptedAttempt{err: errors.New("dial: timeout")},
		scriptedAttempt{err: errors.New("dial: timeout")},
		scriptedAttempt{live: 3 * time.Minute, err: errors.New("connection reset")},
	)
	// A stable connection starts the ladder over; losses without one climb it.
	want := []time.Duration{15 * time.Second, 30 * time.Second, 60 * time.Second, 15 * time.Second}
	if !slices.Equal(waits, want) {
		t.Fatalf("waits = %v, want %v", waits, want)
	}
	switch {
	case state.Attempts != 4:
		t.Fatalf("attempts = %d, want 4", state.Attempts)
	case state.State != WatchStateWaiting:
		t.Fatalf("state = %q, want waiting", state.State)
	case !strings.Contains(state.LastError, "connection reset"):
		t.Fatalf("last error = %q, want the last loss", state.LastError)
	case state.LastDrop.IsZero():
		t.Fatal("last drop not recorded for a connection that was live")
	case !state.NextAttempt.After(state.Since):
		t.Fatalf("next attempt %s not after the drop at %s", state.NextAttempt, state.Since)
	}
	for _, want := range []string{"connection lost after 5m0s", "reconnecting (attempt 4)"} {
		if !strings.Contains(log, want) {
			t.Fatalf("log lacks %q:\n%s", want, log)
		}
	}
}

func TestWatchSupervisorWaitsOutDeadSession(t *testing.T) {
	rejected := fmt.Errorf("telegram session is not authorized: %w", &tgerr.Error{Code: 401, Type: "AUTH_KEY_UNREGISTERED"})
	waits, state, log := runSupervisor(t, true, scriptedAttempt{err: rejected})
	// Nothing but a new login repairs a rejected session: no fast retries.
	if !slices.Equal(waits, []time.Duration{sessionDeadRetry}) {
		t.Fatalf("waits = %v, want one wait of %s", waits, sessionDeadRetry)
	}
	if state.State != WatchStateSessionDead {
		t.Fatalf("state = %q, want session_dead", state.State)
	}
	if !strings.Contains(log, "telemcp login") {
		t.Fatalf("log does not tell how to repair the session:\n%s", log)
	}
}

func TestWatchSupervisorSlowsDownOnKeyConflict(t *testing.T) {
	duplicated := &tgerr.Error{Code: 406, Type: "AUTH_KEY_DUPLICATED", Message: "auth key duplicated"}
	waits, state, _ := runSupervisor(t, true, scriptedAttempt{err: duplicated})
	if !slices.Equal(waits, []time.Duration{authKeyConflictWait}) {
		t.Fatalf("waits = %v, want one wait of %s", waits, authKeyConflictWait)
	}
	// A conflict is not a rejected session: the next attempt may just work.
	if state.State != WatchStateWaiting {
		t.Fatalf("state = %q, want waiting", state.State)
	}
}

func TestWatchSupervisorSkipsDialWithoutRoute(t *testing.T) {
	waits, state, log := runSupervisor(t, false, scriptedAttempt{err: errors.New("write tcp: broken pipe")})
	// Without a route the daemon waits instead of dialing, but not forever:
	// the routing table may be wrong.
	if !slices.Equal(waits, []time.Duration{15 * time.Second, 30 * time.Second, 60 * time.Second, 60 * time.Second}) {
		t.Fatalf("waits = %v, want %d gated waits, one attempt, one more wait", waits, routeGateSkips)
	}
	if state.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1: the gate must not keep the daemon from dialing", state.Attempts)
	}
	if got := strings.Count(log, "no route to Telegram"); got != routeGateSkips {
		t.Fatalf("log names %d gated waits, want %d:\n%s", got, routeGateSkips, log)
	}
}

func TestWatchStatePublisher(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "telemcp.db")
	if st, err := ReadWatchState(dbPath); st != nil || err != nil {
		t.Fatalf("absent state: st = %+v, err = %v, want none", st, err)
	}

	now := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)
	s := &watchSupervisor{state: newWatchStatePublisher(dbPath, watchStateLog{}), now: func() time.Time { return now }}
	s.publish(WatchStateConnecting, "", time.Time{})
	s.live()
	s.publish(WatchStateWaiting, "connection lost: broken pipe", now.Add(time.Minute))
	st, err := ReadWatchState(dbPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	switch {
	case st.State != WatchStateWaiting || st.Attempts != 1:
		t.Fatalf("state = %+v, want waiting after one connection", *st)
	case st.LastError != "connection lost: broken pipe":
		t.Fatalf("last error = %q, want the loss", st.LastError)
	case !st.LastDrop.Equal(now) || !st.NextAttempt.Equal(now.Add(time.Minute)) || !st.Since.Equal(now):
		t.Fatalf("drop %s, next attempt %s, since %s: want %s, %s, %s", st.LastDrop, st.NextAttempt, st.Since, now, now.Add(time.Minute), now)
	}

	// Reconnecting starts a new state but keeps what the last connection died of.
	s.publish(WatchStateConnecting, "", time.Time{})
	if st, err := ReadWatchState(dbPath); err != nil || st.State != WatchStateConnecting || st.Attempts != 2 || st.LastError != "connection lost: broken pipe" {
		t.Fatalf("reconnecting state = %+v (%v)", st, err)
	}
	// A reader may only ever see a whole file, and no other user may read it.
	entries, err := os.ReadDir(filepath.Dir(dbPath))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 1 || entries[0].Name() != "watch.state" {
		t.Fatalf("directory holds %v, want only watch.state", entries)
	}
	if info, err := entries[0].Info(); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v (%v), want 0600", info.Mode().Perm(), err)
	}

	s.state.remove()
	if st, err := ReadWatchState(dbPath); st != nil || err != nil {
		t.Fatalf("removed state: st = %+v, err = %v, want none", st, err)
	}
}

func TestReadWatchStateRejectsGarbage(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "telemcp.db")
	if err := os.WriteFile(watchStatePath(dbPath), []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := ReadWatchState(dbPath); err == nil {
		t.Fatal("err = nil for an unparsable state file")
	}
}

// fastConnHealth shrinks the connection timings for tests.
func fastConnHealth() connHealthConfig {
	return connHealthConfig{probeEvery: 5 * time.Millisecond, probeTimeout: 50 * time.Millisecond, stalledAfter: 300 * time.Millisecond}
}

func TestConnHealthResetsSilentConnection(t *testing.T) {
	h, ctx := newConnHealth(context.Background(), fastConnHealth(), watchStateLog{}, nil)
	defer h.stop()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a connection that never answers was not reset")
	}
	if err := h.stalled(); !errors.Is(err, errConnectionStalled) {
		t.Fatalf("stalled() = %v, want errConnectionStalled", err)
	}
	if live := h.liveFor(time.Now()); live != 0 {
		t.Fatalf("liveFor = %s for a connection that never answered", live)
	}
}

func TestConnHealthResetsOnTheDeadline(t *testing.T) {
	cfg := connHealthConfig{probeEvery: 200 * time.Millisecond, probeTimeout: 50 * time.Millisecond, stalledAfter: 600 * time.Millisecond}
	h, ctx := newConnHealth(context.Background(), cfg, watchStateLog{}, nil)
	defer h.stop()
	time.Sleep(10 * time.Millisecond)
	h.answered()
	answered := time.Now()
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("a connection that stopped answering was not reset")
	}
	// Not on some later tick: the reset comes stalledAfter after the last answer.
	if elapsed := time.Since(answered); elapsed < cfg.stalledAfter || elapsed > cfg.stalledAfter+100*time.Millisecond {
		t.Fatalf("reset %s after the last answer, want %s", elapsed, cfg.stalledAfter)
	}
}

func TestConnHealthKeepsAnsweringConnection(t *testing.T) {
	h, ctx := newConnHealth(context.Background(), fastConnHealth(), watchStateLog{}, nil)
	defer h.stop()
	for end := time.Now().Add(900 * time.Millisecond); time.Now().Before(end); {
		h.answered()
		time.Sleep(25 * time.Millisecond)
	}
	if ctx.Err() != nil || h.stalled() != nil {
		t.Fatalf("an answering connection was reset: ctx err %v, stalled %v", ctx.Err(), h.stalled())
	}
}

func TestConnHealthProbesQuietConnection(t *testing.T) {
	var probes atomic.Int32
	h, ctx := newConnHealth(context.Background(), fastConnHealth(), watchStateLog{}, nil)
	defer h.stop()
	// A real probe is a request, so it counts as an answer itself.
	h.setProbe(func(context.Context) error {
		probes.Add(1)
		h.answered()
		return nil
	})
	h.connectionState(telegram.ConnectionStateReady)
	h.answered()

	deadline := time.Now().Add(2 * time.Second)
	for probes.Load() < 2 && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if got := probes.Load(); got < 2 || ctx.Err() != nil {
		t.Fatalf("probes = %d (ctx err %v): a quiet connection must be probed repeatedly and never reset", got, ctx.Err())
	}
}

type invokerFunc func(context.Context, bin.Encoder, bin.Decoder) error

func (f invokerFunc) Invoke(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
	return f(ctx, in, out)
}

// noopEncoder stands in for the request the middleware is called with.
type noopEncoder struct{}

func (noopEncoder) Encode(*bin.Buffer) error { return nil }

// noopDecoder stands in for the response.
type noopDecoder struct{}

func (noopDecoder) Decode(*bin.Buffer) error { return nil }

func TestConnHealthCountsServerRepliesAsAnswers(t *testing.T) {
	var live atomic.Int32
	h, _ := newConnHealth(context.Background(), fastConnHealth(), watchStateLog{}, func() { live.Add(1) })
	defer h.stop()
	invoke := h.Handle(invokerFunc(func(context.Context, bin.Encoder, bin.Decoder) error {
		return &tgerr.Error{Code: 420, Type: "FLOOD_WAIT", Argument: 5}
	}))
	for range 2 {
		if err := invoke(context.Background(), noopEncoder{}, noopDecoder{}); err == nil {
			t.Fatal("the middleware swallowed the server error")
		}
	}
	if live.Load() != 1 {
		t.Fatalf("live reported %d times, want once: a flood wait is Telegram answering", live.Load())
	}

	// A dead socket is not an answer.
	h2, _ := newConnHealth(context.Background(), fastConnHealth(), watchStateLog{}, nil)
	defer h2.stop()
	invoke = h2.Handle(invokerFunc(func(context.Context, bin.Encoder, bin.Decoder) error { return errors.New("broken pipe") }))
	_ = invoke(context.Background(), noopEncoder{}, noopDecoder{})
	if live := h2.liveFor(time.Now()); live != 0 {
		t.Fatalf("liveFor = %s after a failed write", live)
	}
}

func TestReconnectSchedulerPassesOnItsOwnSchedule(t *testing.T) {
	rec := &passRecorder{runs: make(chan struct{}, 16)}
	s := newTestScheduler(t, 30*time.Minute, rec)

	conn, cancelConn := context.WithCancel(context.Background())
	s.attach(&watchHandler{}, conn)
	rec.ran(t) // the startup pass
	s.requestPass()
	rec.idle(t, 50*time.Millisecond) // right after a finished pass: too soon
	s.ageLastDone(31 * time.Minute)
	s.requestPass()
	rec.ran(t)

	// A reconnect alone starts no pass; one after the schedule went stale does.
	s.attach(&watchHandler{}, conn)
	rec.idle(t, 50*time.Millisecond)
	s.ageLastDone(31 * time.Minute)
	s.attach(&watchHandler{}, conn)
	rec.ran(t)

	// Offline there is nothing to pass with.
	cancelConn()
	s.detach()
	s.ageLastDone(31 * time.Minute)
	s.requestPass()
	rec.idle(t, 50*time.Millisecond)
}

func TestReconnectSchedulerRunsPeriodically(t *testing.T) {
	rec := &passRecorder{runs: make(chan struct{}, 16)}
	s := newTestScheduler(t, 20*time.Millisecond, rec)
	s.attach(&watchHandler{}, context.Background())
	rec.ran(t) // startup
	// Every periodic tick is followed by another: rearming must not stall.
	rec.ran(t)
	rec.ran(t)
	rec.ran(t)
	// A requested pass still runs after the periodic ones.
	s.ageLastDone(31 * time.Minute)
	s.requestPass()
	rec.ran(t)
}

func TestReconnectSchedulerPassEndsWithConnection(t *testing.T) {
	started := make(chan struct{}, 16)
	rec := &passRecorder{runs: started}
	s := newTestScheduler(t, 0, rec)
	s.pass = func(ctx context.Context, _ *watchHandler) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	conn, cancelConn := context.WithCancel(context.Background())
	s.attach(&watchHandler{}, conn)
	<-started
	cancelConn() // the connection is lost mid-pass

	// The cut pass counts as neither done nor failed: the next connection starts one.
	s.attach(&watchHandler{}, context.Background())
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("no pass for the connection after an interrupted one")
	}
}

// passRecorder reports the reconcile passes a test scheduler runs.
type passRecorder struct {
	runs chan struct{}
}

func (p *passRecorder) ran(t *testing.T) {
	t.Helper()
	select {
	case <-p.runs:
	case <-time.After(5 * time.Second):
		t.Fatal("the reconcile pass did not run")
	}
}

func (p *passRecorder) idle(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-p.runs:
		t.Fatal("an unexpected reconcile pass ran")
	case <-time.After(d):
	}
}

func newTestScheduler(t *testing.T, every time.Duration, rec *passRecorder) *reconcileScheduler {
	t.Helper()
	s := newReconcileScheduler(WatchOptions{ReconcileEvery: every}, filepath.Join(t.TempDir(), "telemcp.db"))
	s.pass = func(context.Context, *watchHandler) error {
		rec.runs <- struct{}{}
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	s.start(ctx)
	return &s
}

// ageLastDone moves the last completed pass into the past.
func (s *reconcileScheduler) ageLastDone(d time.Duration) {
	s.mu.Lock()
	s.lastDone = time.Now().Add(-d)
	s.mu.Unlock()
}

func TestChannelBootstrapFreshness(t *testing.T) {
	var b channelBootstrap
	now := time.Date(2026, 11, 25, 16, 0, 0, 0, time.UTC)
	if !b.due(1, now) {
		t.Fatal("a channel that was never checked must be checked")
	}
	b.verified(1, now)
	if b.due(1, now.Add(bootstrapFreshFor-time.Second)) {
		t.Fatal("a channel checked a moment ago must be skipped on reconnect")
	}
	if !b.due(1, now.Add(bootstrapFreshFor)) {
		t.Fatal("a channel checked before the freshness window must be checked again")
	}
	if !b.due(2, now) {
		t.Fatal("a verified channel must not vouch for another one")
	}
}

type stubFetcher struct {
	calls atomic.Int32
}

func (f *stubFetcher) FetchChatMedia(context.Context, string, []int, DownloadOptions) ([]MediaDownload, error) {
	f.calls.Add(1)
	return nil, nil
}

func TestLiveDownloaderFollowsConnection(t *testing.T) {
	var d liveDownloader
	if _, err := d.FetchChatMedia(context.Background(), "chat", nil, DownloadOptions{}); !errors.Is(err, errNoConnection) {
		t.Fatalf("err = %v between connections, want errNoConnection", err)
	}
	f := &stubFetcher{}
	d.set(f)
	if _, err := d.FetchChatMedia(context.Background(), "chat", nil, DownloadOptions{}); err != nil || f.calls.Load() != 1 {
		t.Fatalf("err = %v, calls = %d with a live connection", err, f.calls.Load())
	}
	d.set(nil)
	if _, err := d.FetchChatMedia(context.Background(), "chat", nil, DownloadOptions{}); !errors.Is(err, errNoConnection) {
		t.Fatalf("err = %v after the connection ended, want errNoConnection", err)
	}
}

func TestIsSessionDead(t *testing.T) {
	rejected := fmt.Errorf("telegram session is not authorized: %w", &tgerr.Error{Code: 401, Type: "AUTH_KEY_UNREGISTERED"})
	if !isSessionDead(rejected) {
		t.Fatal("a wrapped 401 is a dead session")
	}
	if isSessionDead(errors.New("write tcp: broken pipe")) {
		t.Fatal("a transport error is not a dead session")
	}
}

func TestNetworkFingerprintIsStable(t *testing.T) {
	if a, b := networkFingerprint(), networkFingerprint(); a != b || a == "" {
		t.Fatalf("two readings of an unchanged network differ or are empty:\n%q\n%q", a, b)
	}
}

func TestWaitForEndsOnTimerAndShutdown(t *testing.T) {
	var logs strings.Builder
	s := &watchSupervisor{log: watchStateLog{w: &logs}}
	if err := s.waitFor(context.Background(), 10*time.Millisecond); err != nil {
		t.Fatalf("waitFor = %v once the wait elapsed, want nil", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.waitFor(ctx, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitFor = %v after a shutdown, want context.Canceled", err)
	}
}
