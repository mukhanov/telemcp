package telegram

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// DefaultPath returns the default Telegram Desktop tdata directory for the
// running platform.
func DefaultPath() string {
	home, _ := os.UserHomeDir()
	switch runtime.GOOS {
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", "Telegram Desktop", "tdata")
	case "windows":
		if appData := strings.TrimSpace(os.Getenv("APPDATA")); appData != "" {
			return filepath.Join(appData, "Telegram Desktop", "tdata")
		}
		return filepath.Join(home, "AppData", "Roaming", "Telegram Desktop", "tdata")
	default:
		if dataHome := strings.TrimSpace(os.Getenv("XDG_DATA_HOME")); dataHome != "" {
			return filepath.Join(dataHome, "TelegramDesktop", "tdata")
		}
		return filepath.Join(home, ".local", "share", "TelegramDesktop", "tdata")
	}
}
