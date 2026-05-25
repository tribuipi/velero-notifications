package config

import (
	"os"
	"testing"
)

func TestLoadConfig_NotifyOnStartupTrue(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(f.Name())

	if _, err := f.WriteString("notifications:\n  notify_on_startup: true\n"); err != nil {
		t.Fatalf("write config: %v", err)
	}
	f.Close()

	cfg, err := LoadConfig(f.Name())
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if !cfg.Notifications.NotifyOnStartup {
		t.Fatal("expected NotifyOnStartup to be true")
	}
}

func TestLoadConfig_NotifyOnStartupDefaultFalse(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(f.Name())

	if _, err := f.WriteString("notifications:\n  slack:\n    enabled: false\n"); err != nil {
		t.Fatalf("write config: %v", err)
	}
	f.Close()

	cfg, err := LoadConfig(f.Name())
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.Notifications.NotifyOnStartup {
		t.Fatal("expected NotifyOnStartup to default to false")
	}
}
