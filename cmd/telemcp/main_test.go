package main

import "testing"

func TestDetectSubcommand(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "bare login", args: []string{"login"}, want: "login"},
		{name: "login with flags", args: []string{"login", "--session", "/tmp/session.json"}, want: "login"},
		{name: "login after global", args: []string{"--db", "x.db", "login"}, want: "login"},
		{name: "import", args: []string{"import"}, want: "import"},
		{name: "sync", args: []string{"sync"}, want: "sync"},
		{name: "prune", args: []string{"prune"}, want: "prune"},
		{name: "mcp mode", args: nil, want: ""},
		{name: "mcp mode with dbpath", args: []string{"archive.db"}, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := detectSubcommand(tt.args); got != tt.want {
				t.Fatalf("detectSubcommand(%q) = %q, want %q", tt.args, got, tt.want)
			}
		})
	}
}
