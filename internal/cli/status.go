package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/mukhanov/telemcp/internal/archive"
	"github.com/mukhanov/telemcp/internal/telegram"
)

// errDaemonDown is what `telemcp status` returns after printing a report in
// which the watch daemon is not running; main turns it into exit status 1, so
// `telemcp status || launchctl kickstart …` works as a watchdog line.
var errDaemonDown = errors.New("daemon is not running")

const (
	// watchLogName is the file the contrib/ launchd template points the
	// daemon's output at, next to the archive.
	watchLogName = "watch.log"
	// watchLogTailBytes bounds how much of the log end is read for the tail.
	watchLogTailBytes = 64 << 10
	// watchLogTailLines is how many lines of the tail the report carries.
	watchLogTailLines = 3
	// controlProbeTimeout bounds the control socket connection attempt.
	controlProbeTimeout = time.Second
)

// daemonState is what the archive's own files say about its watch daemon, in
// the order a start goes through.
type daemonState string

const (
	// daemonStopped: nothing holds the archive connection lock.
	daemonStopped daemonState = "stopped"
	// daemonStarting: a process holds the lock but the control socket does
	// not answer. The daemon listens only once Telegram has authorized it;
	// one that stays in this state is stuck before that point.
	daemonStarting daemonState = "starting"
	// daemonRunning: the control socket answered a connection.
	daemonRunning daemonState = "running"
)

type daemonStatus struct {
	State daemonState `json:"state"`
	PID   int         `json:"pid,omitempty"`
	// SocketUp reports whether the control socket accepted a connection.
	SocketUp bool `json:"socket_up"`
	// LockHeld reports whether some telemcp process holds the archive
	// connection lock.
	LockHeld bool `json:"lock_held"`
}

// supervisorState is the service manager's view of the daemon job.
type supervisorState struct {
	Kind     string `json:"kind"`   // launchd
	Target   string `json:"target"` // the name the service manager knows the job by
	Loaded   bool   `json:"loaded"`
	State    string `json:"state,omitempty"` // running, not running, spawn scheduled
	PID      int    `json:"pid,omitempty"`
	Runs     int    `json:"runs"`
	LastExit string `json:"last_exit,omitempty"` // exit code or terminating signal of the last run
	// RespawnPended is launchd's reason for parking a scheduled non-demand
	// spawn (a KeepAlive respawn or an interval run) instead of firing it.
	RespawnPended string `json:"respawn_pended,omitempty"`
	// Candidates lists the loaded jobs that mention telemcp when Target is
	// not one of them.
	Candidates []string `json:"candidates,omitempty"`
	Error      string   `json:"error,omitempty"`
}

// jobLabel is the job name without the service manager's domain prefix.
func (s *supervisorState) jobLabel() string {
	return s.Target[strings.LastIndex(s.Target, "/")+1:]
}

// logStatus is the daemon log next to the archive.
type logStatus struct {
	Path      string   `json:"path"`
	LastWrite string   `json:"last_write,omitempty"` // RFC3339; empty when there is no log
	Tail      []string `json:"tail"`
}

// connectionStatus is what a running daemon publishes about its Telegram
// connection; absent for a daemon that does not report one.
type connectionStatus struct {
	State       string `json:"state"`    // live, connecting, waiting, session_dead
	Since       string `json:"since"`    // RFC3339
	Attempts    int    `json:"attempts"` // connections tried since the daemon started
	LastError   string `json:"last_error,omitempty"`
	LastDrop    string `json:"last_drop,omitempty"`
	NextAttempt string `json:"next_attempt,omitempty"`
}

// statusReport is one `telemcp status` report.
type statusReport struct {
	Daemon       daemonStatus      `json:"daemon"`
	Connection   *connectionStatus `json:"connection,omitempty"`
	Supervisor   *supervisorState  `json:"supervisor,omitempty"`
	Archive      *archive.Status   `json:"archive,omitempty"`
	ArchiveError string            `json:"archive_error,omitempty"`
	Log          logStatus         `json:"log"`
}

// runStatus reports whether the archive's watch daemon is alive, what the
// service manager knows about it, and how fresh the archive is.
func (r *runtime) runStatus(args []string) error {
	fs := flag.NewFlagSet("telemcp status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	label := fs.String("label", defaultWatchLabel(), "")
	if err := parseFlagsOnly(fs, args); err != nil {
		return err
	}
	rep := collectStatus(r.ctx, r.dbPath, *label)
	if err := r.printStatus(rep); err != nil {
		return err
	}
	if rep.Daemon.State != daemonRunning {
		return errDaemonDown
	}
	return nil
}

// defaultWatchLabel is the launchd label the README's recipe derives from the
// contrib/ template: com.<user>.telemcp-watch.
func defaultWatchLabel() string {
	name := os.Getenv("USER")
	if u, err := user.Current(); err == nil {
		name = u.Username
	}
	return "com." + name + ".telemcp-watch"
}

// collectStatus gathers the report. Nothing in it fails the command: an
// archive that cannot be read, or a service manager that cannot be asked, is
// itself part of the status.
func collectStatus(ctx context.Context, dbPath, label string) *statusReport {
	rep := &statusReport{Daemon: probeDaemon(dbPath), Log: logStatus{Path: watchLogPath(dbPath)}}
	if s := supervisorProbe(label); s != nil {
		rep.Supervisor = s
		if s.PID != 0 {
			rep.Daemon.PID = s.PID
			// A supervisor process that does not answer yet is starting.
			if rep.Daemon.State != daemonRunning {
				rep.Daemon.State = daemonStarting
			}
		}
	}
	if rep.Daemon.State != daemonStopped {
		rep.Connection = readConnection(dbPath, rep.Daemon.PID)
	}
	db, err := archive.Open(dbPath)
	if err != nil {
		rep.ArchiveError = err.Error()
	} else {
		st, err := db.Status(ctx)
		_ = db.Close()
		if err != nil {
			rep.ArchiveError = err.Error()
		} else {
			rep.Archive = st
		}
	}
	rep.Log.LastWrite, rep.Log.Tail = readWatchLog(rep.Log.Path)
	return rep
}

// readConnection returns the connection state a live daemon published. The
// file outlives a killed daemon, so it only counts for the process at pid.
func readConnection(dbPath string, pid int) *connectionStatus {
	st, err := telegram.ReadWatchState(dbPath)
	if err != nil {
		return &connectionStatus{State: "unknown", LastError: err.Error()}
	}
	if st == nil || pid != 0 && st.PID != 0 && st.PID != pid {
		return nil // a stale file of an earlier daemon
	}
	c := &connectionStatus{State: st.State, Since: st.Since.Format(time.RFC3339), Attempts: st.Attempts, LastError: st.LastError}
	if !st.LastDrop.IsZero() {
		c.LastDrop = st.LastDrop.Format(time.RFC3339)
	}
	if !st.NextAttempt.IsZero() {
		c.NextAttempt = st.NextAttempt.Format(time.RFC3339)
	}
	return c
}

// probeDaemon classifies the daemon from the archive's own files. The control
// socket is the positive signal: the daemon listens on it once Telegram has
// authorized it and removes it on every exit, so an accepted connection
// proves a live daemon. The lock is only probed when the socket is down —
// the probe holds the lock for a moment, and a daemon acquiring it in exactly
// that moment would wrongly find the archive taken.
func probeDaemon(dbPath string) daemonStatus {
	if conn, err := net.DialTimeout("unix", telegram.ControlSocketPath(dbPath), controlProbeTimeout); err == nil {
		_ = conn.Close()
		return daemonStatus{State: daemonRunning, SocketUp: true}
	}
	d := daemonStatus{State: daemonStopped}
	if telegram.WatchLockHeld(dbPath) {
		d.State, d.LockHeld = daemonStarting, true
	}
	return d
}

func watchLogPath(dbPath string) string {
	return filepath.Join(filepath.Dir(dbPath), watchLogName)
}

// readWatchLog returns the log's last write time and its last lines, without
// the updates manager's own `watch: updates:` chatter: on a dead connection
// it logs thousands of context-canceled errors around the one line that
// explains the exit. A missing log is an empty tail, not an error.
func readWatchLog(path string) (lastWrite string, tail []string) {
	f, err := os.Open(path)
	if err != nil {
		return "", []string{}
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", []string{}
	}
	var offset int64
	if info.Size() > watchLogTailBytes {
		offset = info.Size() - watchLogTailBytes
	}
	buf, err := io.ReadAll(io.NewSectionReader(f, offset, watchLogTailBytes))
	if err != nil {
		return "", []string{}
	}
	text := string(buf)
	if offset > 0 {
		// The buffer starts inside a line; its remaining half is not a record.
		_, text, _ = strings.Cut(text, "\n")
	}
	for line := range strings.SplitSeq(text, "\n") {
		if line == "" || strings.HasPrefix(line, "watch: updates:") {
			continue
		}
		tail = append(tail, line)
		if len(tail) > watchLogTailLines {
			tail = tail[1:]
		}
	}
	return info.ModTime().UTC().Format(time.RFC3339), tail
}

// launchdState reads one `launchctl print` dump.
func launchdState(dump, target string) (*supervisorState, error) {
	fields := parseLaunchctlPrint(dump)
	s := &supervisorState{
		Kind:          "launchd",
		Target:        target,
		Loaded:        true,
		State:         fields["state"],
		RespawnPended: fields["pended nondemand spawn"],
	}
	if s.State == "" {
		return nil, fmt.Errorf("launchctl print %s: no state line in the output", target)
	}
	s.PID, _ = strconv.Atoi(fields["pid"])
	s.Runs, _ = strconv.Atoi(fields["runs"])
	// A run that ended by signal has no exit code line; launchd names the signal instead.
	if s.LastExit = fields["last exit code"]; s.LastExit == "" {
		s.LastExit = fields["last terminating signal"]
	}
	return s, nil
}

// parseLaunchctlPrint reads the top-level `key = value` lines of a
// `launchctl print` dump. Nested blocks (environments, triggers) are indented
// deeper and ignored. The format is not an API, so unknown lines are skipped
// rather than guessed at.
func parseLaunchctlPrint(dump string) map[string]string {
	fields := make(map[string]string)
	for line := range strings.SplitSeq(dump, "\n") {
		if len(line) < 2 || line[0] != '\t' || line[1] == '\t' {
			continue
		}
		key, value, ok := strings.Cut(line[1:], " = ")
		if !ok {
			continue
		}
		if _, seen := fields[key]; !seen {
			fields[key] = value
		}
	}
	return fields
}

// startHint names the command that brings a daemon which is not running back
// under its supervisor; empty when there is nothing useful to say.
func startHint(rep *statusReport) string {
	switch s := rep.Supervisor; {
	case rep.Daemon.State == daemonRunning:
		return ""
	case s == nil, !s.Loaded:
		return "telemcp watch"
	case s.State == "spawn scheduled", s.PID != 0:
		return "" // the supervisor is starting it
	default:
		return "launchctl kickstart " + s.Target
	}
}

func (r *runtime) printStatus(rep *statusReport) error {
	if r.json {
		enc := json.NewEncoder(r.stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(rep)
	}
	now := time.Now()
	var err error
	line := func(format string, a ...any) {
		if err == nil {
			_, err = fmt.Fprintf(r.stdout, format, a...)
		}
	}
	pidNote := func(pid int) string {
		if pid == 0 {
			return ""
		}
		return fmt.Sprintf("pid %d, ", pid)
	}

	line("daemon:  %s", rep.Daemon.State)
	switch rep.Daemon.State {
	case daemonRunning:
		line(" (%swatch.sock answers)", pidNote(rep.Daemon.PID))
	case daemonStarting:
		line(" (%swatch.sock does not answer yet", pidNote(rep.Daemon.PID))
		if rep.Daemon.LockHeld {
			line(", watch.lock is held")
		}
		line(")")
	}
	line("\n")
	if c := rep.Connection; c != nil {
		line("connection: %s", c.State)
		if c.Since != "" {
			line(" since %s", localTime(c.Since))
		}
		switch c.State {
		case telegram.WatchStateWaiting:
			line(", next attempt %s", until(c.NextAttempt, now))
		case telegram.WatchStateSessionDead:
			line(", next attempt %s — run 'telemcp login'", until(c.NextAttempt, now))
		}
		if c.Attempts > 1 {
			line(", %d reconnects", c.Attempts-1)
		}
		if c.LastDrop != "" {
			line(", last drop %s", localTime(c.LastDrop))
		}
		line("\n")
		if c.LastError != "" {
			line("            last error: %s\n", c.LastError)
		}
	}

	if s := rep.Supervisor; s != nil {
		line("%-9s", s.Kind+":")
		switch {
		case s.Error != "":
			line(" %s", s.Error)
		case !s.Loaded:
			line(" no job named %s is loaded", s.jobLabel())
			if len(s.Candidates) != 0 {
				line(" (loaded telemcp jobs: %s; pass --label)", strings.Join(s.Candidates, ", "))
			}
		default:
			line(" %s: %s, runs %d", s.jobLabel(), s.State, s.Runs)
			if s.LastExit != "" {
				line(", last exit %s", s.LastExit)
			}
			if s.RespawnPended != "" {
				line(", respawn pended (%s)", s.RespawnPended)
			}
		}
		line("\n")
	}

	if hint := startHint(rep); hint != "" {
		line("start:   %s\n", hint)
	}

	if rep.ArchiveError != "" {
		line("archive:  %s: %s\n", r.dbPath, rep.ArchiveError)
	} else {
		a := rep.Archive
		line("archive:  %s\n          %d messages in %d chats", a.DBPath, a.Messages, a.Chats)
		if t, err := time.Parse(time.RFC3339, a.NewestMessage); err == nil {
			line(", newest %s (%s)", t.Local().Format(time.DateTime), ago(t, now))
		}
		if a.LastImportAt != "" {
			line(", last import %s", localTime(a.LastImportAt))
		}
		line("\n")
	}

	line("log:      %s", rep.Log.Path)
	if rep.Log.LastWrite == "" {
		line(" (not found)\n")
	} else {
		line(" (last write %s)\n", localTime(rep.Log.LastWrite))
		for _, l := range rep.Log.Tail {
			line("          %s\n", l)
		}
	}
	return err
}

// localTime renders an RFC3339 timestamp in the local zone; anything that does
// not parse is shown as it is.
func localTime(s string) string {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return s
	}
	return t.Local().Format(time.DateTime)
}

// until renders how far a timestamp is from now.
func until(s string, now time.Time) string {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return s
	}
	if d := t.Sub(now); d > 0 {
		return "in " + d.Round(time.Second).String()
	}
	return "now"
}

// ago renders an age the way it is read off a status line.
func ago(t, now time.Time) string {
	d := now.Sub(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%02dm ago", int(d.Hours()), int(d.Minutes())%60)
	default:
		return fmt.Sprintf("%dd%dh ago", int(d.Hours()/24), int(d.Hours())%24)
	}
}
