//go:build darwin

package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// supervisorProbe is the service manager probe: launchd on macOS.
var supervisorProbe = launchdProbe

const (
	// launchctlPath is where macOS keeps launchctl; the archive's tools run
	// under launchd with PATH=/usr/bin:/bin:/usr/sbin:/sbin, and an absolute
	// path needs no lookup at all.
	launchctlPath = "/bin/launchctl"
	// launchctlTimeout bounds each launchctl call.
	launchctlTimeout = 5 * time.Second
)

// errNoService is launchctl reporting a job it does not know.
var errNoService = errors.New("no such service")

// launchdProbe is what launchd knows about the daemon job. A job launchd
// does not know is reported as not loaded, not as an error: that is a state
// `telemcp status` exists to show.
func launchdProbe(label string) *supervisorState {
	target := "gui/" + strconv.Itoa(os.Getuid()) + "/" + label
	ctx, cancel := context.WithTimeout(context.Background(), launchctlTimeout)
	defer cancel()
	dump, err := launchctlOutput(ctx, "print", target)
	switch {
	case errors.Is(err, errNoService):
		return &supervisorState{Kind: "launchd", Target: target, Candidates: launchdTelemcpJobs(ctx)}
	case err != nil:
		return &supervisorState{Kind: "launchd", Target: target, Error: err.Error()}
	}
	s, err := launchdState(string(dump), target)
	if err != nil {
		return &supervisorState{Kind: "launchd", Target: target, Error: err.Error()}
	}
	return s
}

// launchctlOutput runs launchctl and returns its stdout. A job launchd does
// not know fails with "Could not find service" on stderr.
func launchctlOutput(ctx context.Context, args ...string) ([]byte, error) {
	out, err := exec.CommandContext(ctx, launchctlPath, args...).Output()
	if err == nil {
		return out, nil
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("launchctl %s: %w", strings.Join(args, " "), ctx.Err())
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && strings.Contains(string(exitErr.Stderr), "Could not find service") {
		return nil, errNoService
	}
	return nil, fmt.Errorf("launchctl %s: %w", strings.Join(args, " "), err)
}

// launchdTelemcpJobs lists the loaded labels that mention telemcp, so a run
// with the wrong --label can point at the real job.
func launchdTelemcpJobs(ctx context.Context) []string {
	out, err := launchctlOutput(ctx, "list")
	if err != nil {
		return nil
	}
	var jobs []string
	for line := range strings.SplitSeq(string(out), "\n") {
		// launchctl list prints "PID<TAB>Status<TAB>Label" per job.
		_, rest, ok := strings.Cut(line, "\t")
		if !ok {
			continue
		}
		_, label, ok := strings.Cut(rest, "\t")
		if ok && strings.Contains(strings.ToLower(label), "telemcp") {
			jobs = append(jobs, label)
		}
	}
	return jobs
}
