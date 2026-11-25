//go:build darwin

package cli

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
)

// guiDomainReachable reports whether launchctl can be asked about the user's
// session at all; a CI runner without a GUI session cannot.
func guiDomainReachable() bool {
	_, err := launchctlOutput(context.Background(), "print", "gui/"+strconv.Itoa(os.Getuid()))
	return err == nil || errors.Is(err, errNoService)
}

func TestLaunchdProbeUnknownJob(t *testing.T) {
	if !guiDomainReachable() {
		t.Skip("launchd has no gui session for this user")
	}
	label := "com.example.telemcp.absent-" + strconv.Itoa(os.Getpid())
	s := launchdProbe(label)
	if s.Error != "" {
		t.Fatalf("probe: %s", s.Error)
	}
	if s.Loaded || s.State != "" || s.Target != "gui/"+strconv.Itoa(os.Getuid())+"/"+label {
		t.Fatalf("unknown job: probe = %+v, want an unloaded job", *s)
	}
	for _, job := range s.Candidates {
		if job == label || !strings.Contains(job, "telemcp") {
			t.Fatalf("candidates = %q, want only loaded telemcp jobs other than %s", s.Candidates, label)
		}
	}
}
