package cli

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mukhanov/telemcp/internal/store"
	"github.com/mukhanov/telemcp/internal/telegram"
)

// stubSupervisor keeps status tests off the machine's real service manager
// and feeds them the given service manager view instead.
func stubSupervisor(t *testing.T, s *supervisorState) {
	t.Helper()
	orig := supervisorProbe
	supervisorProbe = func(string) *supervisorState { return s }
	t.Cleanup(func() { supervisorProbe = orig })
}

// openArchive creates an empty archive the status command can read.
func openArchive(t *testing.T) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "telemcp.db")
	st, err := store.Open(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	return dbPath
}

// listenControl serves the archive's control socket the way a live daemon does.
func listenControl(t *testing.T, dbPath string) {
	t.Helper()
	ln, err := net.Listen("unix", telegram.ControlSocketPath(dbPath))
	if err != nil {
		t.Fatalf("listen on control socket: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
}

func TestProbeDaemon(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "telemcp.db")

	if got := probeDaemon(dbPath); got.State != daemonStopped || got.SocketUp || got.LockHeld {
		t.Fatalf("idle archive: probe = %+v, want stopped with nothing held", got)
	}

	release, err := telegram.AcquireConnectionLock(dbPath)
	if err != nil {
		t.Fatalf("acquire archive lock: %v", err)
	}
	if got := probeDaemon(dbPath); got.State != daemonStarting || !got.LockHeld || got.SocketUp {
		t.Fatalf("locked archive: probe = %+v, want starting", got)
	}
	if err := release(); err != nil {
		t.Fatalf("release archive lock: %v", err)
	}

	listenControl(t, dbPath)
	if got := probeDaemon(dbPath); got.State != daemonRunning || !got.SocketUp || got.LockHeld {
		t.Fatalf("listening archive: probe = %+v, want running", got)
	}
}

func TestReadWatchLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), watchLogName)
	const exitLine = "telemcp: callback: watch: Telegram connection health check: broken pipe"

	if lastWrite, tail := readWatchLog(path); lastWrite != "" || len(tail) != 0 {
		t.Fatalf("missing log: last write %q, tail %q; want neither", lastWrite, tail)
	}

	// On a dead connection the updates manager floods the log around the one
	// line that explains the exit; only the last records without it are kept.
	written := []string{
		"watch: source: session /Users/u/.telemcp/session.json",
		"watch: updates: ERROR: Handle updates error error=context canceled",
		"watch: reconcile pass: 200 chats, 10 messages",
		"",
		"telemcp: Telegram rate limit; waiting 5s before retry 1/3",
		exitLine,
	}
	before := time.Now().Add(-time.Minute)
	if err := os.WriteFile(path, []byte(strings.Join(written, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
	lastWrite, tail := readWatchLog(path)
	want := []string{"watch: reconcile pass: 200 chats, 10 messages", "telemcp: Telegram rate limit; waiting 5s before retry 1/3", exitLine}
	if strings.Join(tail, "\n") != strings.Join(want, "\n") {
		t.Fatalf("tail = %q, want %q", tail, want)
	}
	if got, err := time.Parse(time.RFC3339, lastWrite); err != nil || got.Before(before) {
		t.Fatalf("last write = %q (%v), want a time after %v", lastWrite, err, before)
	}

	// Only the end of a large log is read, and the half line that end starts
	// with is not a record.
	big := strings.Repeat("a", watchLogTailBytes+10) + "\n" + exitLine + "\n"
	if err := os.WriteFile(path, []byte(big), 0o644); err != nil {
		t.Fatalf("write large log: %v", err)
	}
	if _, tail := readWatchLog(path); len(tail) != 1 || tail[0] != exitLine {
		t.Fatalf("large log: tail = %q, want only %q", tail, exitLine)
	}
}

func TestLaunchdState(t *testing.T) {
	const target = "gui/502/com.u.telemcp-watch"
	// The environment block comes first and carries its own "state" line one
	// level deeper: it must not be taken for the job's state.
	const head = target + ` = {
	active count = 0
	path = /Users/u/Library/LaunchAgents/com.u.telemcp-watch.plist
	environment = {
		XPC_SERVICE_NAME => com.u.telemcp-watch
		state = active
	}
	minimum runtime = 30
	exit timeout = 5
`
	tests := []struct {
		name    string
		dump    string
		want    supervisorState
		wantErr bool
	}{
		{
			name: "dead job with pended respawn",
			dump: head + "\tstate = not running\n\truns = 4\n\tpended nondemand spawn = inefficient\n\tlast exit code = 1\n}\n",
			want: supervisorState{Kind: "launchd", Target: target, Loaded: true, State: "not running", Runs: 4, LastExit: "1", RespawnPended: "inefficient"},
		},
		{
			name: "running job",
			dump: head + "\tstate = running\n\truns = 1\n\tpid = 13588\n\tlast exit code = (never exited)\n}\n",
			want: supervisorState{Kind: "launchd", Target: target, Loaded: true, State: "running", PID: 13588, Runs: 1, LastExit: "(never exited)"},
		},
		{
			name: "job killed by a signal has no exit code",
			dump: head + "\tstate = not running\n\truns = 3\n\tlast terminating signal = Terminated: 15\n}\n",
			want: supervisorState{Kind: "launchd", Target: target, Loaded: true, State: "not running", Runs: 3, LastExit: "Terminated: 15"},
		},
		{
			name:    "dump without a state line",
			dump:    head + "\truns = 1\n}\n",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := launchdState(tt.dump, target)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("err = nil, want an error for a dump without a state line")
				}
				return
			}
			if err != nil {
				t.Fatalf("launchdState: %v", err)
			}
			if !reflect.DeepEqual(*got, tt.want) {
				t.Fatalf("launchdState = %+v, want %+v", *got, tt.want)
			}
		})
	}
}

func TestRunStatus(t *testing.T) {
	ctx := context.Background()
	const target = "gui/502/com.u.telemcp-watch"

	// A stopped daemon still gets the whole report; only the exit status says
	// it is down.
	stubSupervisor(t, nil)
	stopped := openArchive(t)
	var out strings.Builder
	// Global flags (--db, --json) precede the subcommand, as for every subcommand.
	err := Run(ctx, []string{"--db", stopped, "--json", "status"}, &out, &strings.Builder{})
	if !errors.Is(err, errDaemonDown) || ExitCode(err) != 1 {
		t.Fatalf("stopped daemon: err = %v (exit status %d), want errDaemonDown with exit status 1", err, ExitCode(err))
	}
	var rep statusReport
	if err := json.Unmarshal([]byte(out.String()), &rep); err != nil {
		t.Fatalf("decode report %s: %v", out.String(), err)
	}
	switch {
	case rep.Daemon.State != daemonStopped || rep.Supervisor != nil:
		t.Fatalf("stopped daemon: report = %+v, want a stopped daemon and no supervisor", rep)
	case rep.Archive == nil || rep.Archive.DBPath != stopped:
		t.Fatalf("stopped daemon: archive = %+v, want the archive at %s", rep.Archive, stopped)
	case rep.Log.Path != watchLogPath(stopped):
		t.Fatalf("stopped daemon: log path = %q, want %q", rep.Log.Path, watchLogPath(stopped))
	}
	if !strings.Contains(out.String(), `"tail": []`) {
		t.Fatalf("report without a log must carry an empty tail: %s", out.String())
	}

	out.Reset()
	if err := Run(ctx, []string{"--db", stopped, "status"}, &out, &strings.Builder{}); !errors.Is(err, errDaemonDown) {
		t.Fatalf("stopped daemon, text: err = %v, want errDaemonDown", err)
	}
	for _, want := range []string{"daemon:  stopped\n", "start:   telemcp watch\n", "archive:  " + stopped + "\n", "log:      " + watchLogPath(stopped) + " (not found)\n"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("text report lacks %q:\n%s", want, out.String())
		}
	}

	// A daemon that answers its control socket is up, with or without a
	// service manager.
	listenControl(t, stopped)
	out.Reset()
	if err := Run(ctx, []string{"--db", stopped, "--json", "status"}, &out, &strings.Builder{}); err != nil {
		t.Fatalf("running daemon: err = %v, want nil", err)
	}
	rep = statusReport{}
	if err := json.Unmarshal([]byte(out.String()), &rep); err != nil {
		t.Fatalf("decode report %s: %v", out.String(), err)
	}
	if rep.Daemon.State != daemonRunning || !rep.Daemon.SocketUp {
		t.Fatalf("running daemon: daemon = %+v, want running", rep.Daemon)
	}

	// launchd names the pid of the daemon it runs.
	stubSupervisor(t, &supervisorState{Kind: "launchd", Target: target, Loaded: true, State: "running", PID: 4242})
	out.Reset()
	if err := Run(ctx, []string{"--db", stopped, "--json", "status"}, &out, &strings.Builder{}); err != nil {
		t.Fatalf("running daemon under launchd: err = %v, want nil", err)
	}
	rep = statusReport{}
	if err := json.Unmarshal([]byte(out.String()), &rep); err != nil {
		t.Fatalf("decode report %s: %v", out.String(), err)
	}
	if rep.Daemon.State != daemonRunning || rep.Daemon.PID != 4242 {
		t.Fatalf("running daemon under launchd: daemon = %+v, want running with the launchd pid", rep.Daemon)
	}

	// launchd runs a process that does not answer yet: the daemon is starting.
	stubSupervisor(t, &supervisorState{Kind: "launchd", Target: target, Loaded: true, State: "running", PID: 4242})
	starting := openArchive(t)
	out.Reset()
	if err := Run(ctx, []string{"--db", starting, "--json", "status"}, &out, &strings.Builder{}); !errors.Is(err, errDaemonDown) {
		t.Fatalf("starting daemon: err = %v, want errDaemonDown", err)
	}
	rep = statusReport{}
	if err := json.Unmarshal([]byte(out.String()), &rep); err != nil {
		t.Fatalf("decode report %s: %v", out.String(), err)
	}
	if rep.Daemon.State != daemonStarting || rep.Daemon.PID != 4242 {
		t.Fatalf("starting daemon: daemon = %+v, want starting with the launchd pid", rep.Daemon)
	}

	// launchd holds a dead job with its respawn parked: the report says so and
	// names the command that starts it.
	stubSupervisor(t, &supervisorState{Kind: "launchd", Target: target, Loaded: true, State: "not running", Runs: 4, LastExit: "1", RespawnPended: "inefficient"})
	dead := openArchive(t)
	out.Reset()
	if err := Run(ctx, []string{"--db", dead, "status"}, &out, &strings.Builder{}); !errors.Is(err, errDaemonDown) {
		t.Fatalf("pended respawn: err = %v, want errDaemonDown", err)
	}
	for _, want := range []string{
		"launchd:  com.u.telemcp-watch: not running, runs 4, last exit 1, respawn pended (inefficient)\n",
		"start:   launchctl kickstart " + target + "\n",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("pended respawn report lacks %q:\n%s", want, out.String())
		}
	}

	// A wrong --label points at the loaded telemcp jobs instead of leaving the
	// reader with "nothing is loaded".
	stubSupervisor(t, &supervisorState{Kind: "launchd", Target: target, Candidates: []string{"com.example.telemcp-watch"}})
	out.Reset()
	if err := Run(ctx, []string{"--db", dead, "status"}, &out, &strings.Builder{}); !errors.Is(err, errDaemonDown) {
		t.Fatalf("unknown label: err = %v, want errDaemonDown", err)
	}
	if want := "launchd:  no job named com.u.telemcp-watch is loaded (loaded telemcp jobs: com.example.telemcp-watch; pass --label)\n"; !strings.Contains(out.String(), want) {
		t.Fatalf("unknown label report lacks %q:\n%s", want, out.String())
	}
}
