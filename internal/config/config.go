package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// ChatExclusion records a chat excluded from synchronization. Excluded chats
// are hidden from all telemcp tools and pruned from the archive.
type ChatExclusion struct {
	ID         string `json:"id"`
	Name       string `json:"name,omitempty"`
	Reason     string `json:"reason,omitempty"`
	ExcludedAt string `json:"excluded_at,omitempty"`
}

// Config is telemcp's mutable configuration, managed via MCP tools.
type Config struct {
	ExcludeChats []ChatExclusion `json:"exclude_chats,omitempty"`
}

// DefaultPath returns the config location: $TELEMCP_CONFIG, else
// <os.UserConfigDir>/telemcp/config.json.
func DefaultPath() (string, error) {
	if p := os.Getenv("TELEMCP_CONFIG"); p != "" {
		return p, nil
	}
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "telemcp", "config.json"), nil
}

// Load reads the config; a missing file is an empty config.
func Load(path string) (Config, error) {
	var c Config
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(data, &c); err != nil {
		return c, fmt.Errorf("parse %s: %w", path, err)
	}
	return c, nil
}

// Save writes the config atomically.
func Save(path string, c Config) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ExcludedIDs returns the chat ids to keep out of queries and the archive.
func (c Config) ExcludedIDs() []string {
	ids := make([]string, 0, len(c.ExcludeChats))
	for _, e := range c.ExcludeChats {
		ids = append(ids, e.ID)
	}
	return ids
}

// isExcluded reports whether the chat id is excluded.
func (c Config) isExcluded(id string) bool {
	for _, e := range c.ExcludeChats {
		if e.ID == id {
			return true
		}
	}
	return false
}

// Exclude adds a chat to the exclusion list and returns the created entry.
func (c *Config) Exclude(id, name, reason string) (ChatExclusion, error) {
	if c.isExcluded(id) {
		return ChatExclusion{}, fmt.Errorf("chat %s (%s) is already excluded", id, name)
	}
	entry := ChatExclusion{
		ID:         id,
		Name:       name,
		Reason:     reason,
		ExcludedAt: time.Now().UTC().Format(time.RFC3339),
	}
	c.ExcludeChats = append(c.ExcludeChats, entry)
	return entry, nil
}

// Include removes a chat (matched by id or name) from the exclusion list.
func (c *Config) Include(idOrName string) (ChatExclusion, error) {
	for i, e := range c.ExcludeChats {
		if e.ID == idOrName || e.Name == idOrName {
			c.ExcludeChats = append(c.ExcludeChats[:i], c.ExcludeChats[i+1:]...)
			return e, nil
		}
	}
	return ChatExclusion{}, fmt.Errorf("chat %q is not excluded; see get_sync_config", idOrName)
}
