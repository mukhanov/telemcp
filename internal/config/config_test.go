package config

import (
	"path/filepath"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.ExcludeChats) != 0 {
		t.Fatalf("missing config not empty: %+v", cfg)
	}
	if _, err := cfg.Exclude("-100123456", "Work Chat", "archived"); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.Exclude("-100123456", "Work Chat", "dup"); err == nil {
		t.Fatal("duplicate exclusion must error")
	}
	if err := Save(path, cfg); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded.ExcludeChats) != 1 || loaded.ExcludeChats[0].ID != "-100123456" || loaded.ExcludeChats[0].ExcludedAt == "" {
		t.Fatalf("loaded = %+v", loaded)
	}
	entry, err := loaded.Include("Work Chat") // by name
	if err != nil {
		t.Fatal(err)
	}
	if entry.ID != "-100123456" {
		t.Fatalf("entry = %+v", entry)
	}
	if _, err := loaded.Include("Work Chat"); err == nil {
		t.Fatal("including a non-excluded chat must error")
	}
}
