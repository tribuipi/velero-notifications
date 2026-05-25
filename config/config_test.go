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

func TestLoadConfig_FiltersLoaded(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(f.Name())

	content := "filters:\n  name_patterns:\n    - \"^daily-.*\"\n  annotation_key: \"velero-notifications.io/notify\"\n"
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write config: %v", err)
	}
	f.Close()

	cfg, err := LoadConfig(f.Name())
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if len(cfg.Filters.NamePatterns) != 1 || cfg.Filters.NamePatterns[0] != "^daily-.*" {
		t.Fatalf("expected name_patterns [\"^daily-.*\"], got %v", cfg.Filters.NamePatterns)
	}
	if cfg.Filters.AnnotationKey != "velero-notifications.io/notify" {
		t.Fatalf("expected annotation_key %q, got %q", "velero-notifications.io/notify", cfg.Filters.AnnotationKey)
	}
}

func TestLoadConfig_FiltersDefaultEmpty(t *testing.T) {
	t.Parallel()

	f, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	defer os.Remove(f.Name())

	if _, err := f.WriteString("namespace: velero\n"); err != nil {
		t.Fatalf("write config: %v", err)
	}
	f.Close()

	cfg, err := LoadConfig(f.Name())
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if len(cfg.Filters.NamePatterns) != 0 {
		t.Fatalf("expected empty name_patterns, got %v", cfg.Filters.NamePatterns)
	}
	if cfg.Filters.AnnotationKey != "" {
		t.Fatalf("expected empty annotation_key, got %q", cfg.Filters.AnnotationKey)
	}
}
