//go:build !windows

package telegram

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/mukhanov/telemcp/internal/store"
)

func TestAuditMediaReadsRequireSelectedRegularSource(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "private")
	if err := os.WriteFile(outside, []byte("outside sentinel"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "cache-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(root, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{outside, link, fifo} {
		destination := filepath.Join(t.TempDir(), "media")
		messages := []store.Message{{MediaPath: source}}
		if err := copyImportedMedia(messages, destination, nil, root); err == nil || isMediaSourceUnavailable(err) {
			t.Fatalf("invalid source treated as success/cache miss: %q %v", source, err)
		}
		if _, err := os.Stat(destination); !os.IsNotExist(err) {
			t.Fatal("invalid source created archive media")
		}
	}
}
