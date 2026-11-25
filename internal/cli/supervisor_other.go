//go:build !darwin

package cli

// supervisorProbe reports no service manager on platforms without launchd:
// the daemon's own lock and control socket remain the status signals.
var supervisorProbe = func(string) *supervisorState { return nil }
