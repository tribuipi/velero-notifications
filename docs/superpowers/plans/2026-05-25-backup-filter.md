# Backup Filtering Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add configurable backup filtering so operators can select which Velero backups trigger notifications using regex name patterns and/or an opt-in annotation (OR logic); no filters configured means notify all (backward-compatible).

**Architecture:** A `FiltersConfig` struct in the config package captures name patterns and an annotation key from `config.yaml`. The controller defines its own `FilterConfig` type, compiles regexes once at startup in `NewVeleroController`, stores them on the struct, and gates every `handleEvent` call through a new `matchesFilter()` method before any notification or annotation patch.

**Tech Stack:** Go stdlib `regexp`, Kubernetes `unstructured.Unstructured`, YAML config, Helm

---

### Task 1: Add Filters struct to config package

**Files:**
- Modify: `config/config.go`
- Modify: `config/config_test.go`

- [ ] **Step 1: Write failing tests**

Add to `config/config_test.go` after the existing tests:

```go
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./config/... -run 'TestLoadConfig_Filters' -v
```

Expected: compile error — `cfg.Filters` field does not exist yet.

- [ ] **Step 3: Add FiltersConfig and Filters field to config.go**

Add the new type before the `Config` struct, and add a `Filters` field at the bottom of `Config`:

```go
type FiltersConfig struct {
	NamePatterns  []string `yaml:"name_patterns"`
	AnnotationKey string   `yaml:"annotation_key"`
}

type Config struct {
	Logging struct {
		Level   string `yaml:"level"`
		Verbose bool   `yaml:"verbose"`
	} `yaml:"logging"`
	Namespace     string `yaml:"namespace"`
	CheckInterval int    `yaml:"check_interval"`
	Notifications struct {
		NotificationPrefix string `yaml:"notification_prefix"`
		NotifyOnStartup    bool   `yaml:"notify_on_startup"`
		Slack              struct {
			Enabled      bool   `yaml:"enabled"`
			FailuresOnly bool   `yaml:"failures_only"`
			Webhook      string `yaml:"webhook_url"`
			Channel      string `yaml:"channel"`
			Username     string `yaml:"username"`
		} `yaml:"slack"`
		Email struct {
			Enabled      bool   `yaml:"enabled"`
			FailuresOnly bool   `yaml:"failures_only"`
			SMTPServer   string `yaml:"smtp_server"`
			SMTPPort     int    `yaml:"smtp_port"`
			Username     string `yaml:"username"`
			Password     string `yaml:"password"`
			From         string `yaml:"from"`
			To           string `yaml:"to"`
		} `yaml:"email"`
	} `yaml:"notifications"`
	Filters FiltersConfig `yaml:"filters"`
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./config/... -v
```

Expected: PASS — all existing tests plus the two new Filters tests.

- [ ] **Step 5: Commit**

```bash
git add config/config.go config/config_test.go
git commit -m "feat: add FiltersConfig struct to config package"
```

---

### Task 2: Add filter logic to controller

**Files:**
- Modify: `controllers/controller.go`
- Modify: `controllers/controller_test.go`

- [ ] **Step 1: Write failing tests**

Add `"regexp"` to the import block in `controllers/controller_test.go`.

Add the following helper and tests after the `createBackup` helper function:

```go
func newTestControllerWithFilter(mock *mockNotifier, notifyOnStartup bool, patterns []*regexp.Regexp, annotationKey string) (*VeleroController, *fake.FakeDynamicClient) {
	fakeClient := fake.NewSimpleDynamicClient(runtime.NewScheme())
	vc := &VeleroController{
		Namespace:        "velero",
		Verbose:          false,
		NotifyOnStartup:  notifyOnStartup,
		Notifiers:        []notifications.Notifier{mock},
		dynClient:        fakeClient,
		filterPatterns:   patterns,
		filterAnnotation: annotationKey,
	}
	return vc, fakeClient
}

func TestCompilePatterns_ValidRegex(t *testing.T) {
	t.Parallel()

	patterns, err := compilePatterns([]string{"^daily-.*", "^prod-.*"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(patterns) != 2 {
		t.Fatalf("expected 2 patterns, got %d", len(patterns))
	}
}

func TestCompilePatterns_InvalidRegex(t *testing.T) {
	t.Parallel()

	_, err := compilePatterns([]string{"[invalid"})
	if err == nil {
		t.Fatal("expected error for invalid regex, got nil")
	}
}

func TestCompilePatterns_Empty(t *testing.T) {
	t.Parallel()

	patterns, err := compilePatterns(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(patterns) != 0 {
		t.Fatalf("expected 0 patterns, got %d", len(patterns))
	}
}

func TestMatchesFilter(t *testing.T) {
	t.Parallel()

	daily := regexp.MustCompile("^daily-.*")

	tests := []struct {
		name          string
		patterns      []*regexp.Regexp
		annotationKey string
		backupName    string
		annotations   map[string]string
		want          bool
	}{
		{
			name:       "no filters: notify all",
			backupName: "any-backup",
			want:       true,
		},
		{
			name:       "name matches pattern",
			patterns:   []*regexp.Regexp{daily},
			backupName: "daily-backup",
			want:       true,
		},
		{
			name:       "name does not match pattern",
			patterns:   []*regexp.Regexp{daily},
			backupName: "weekly-backup",
			want:       false,
		},
		{
			name:          "annotation present",
			annotationKey: "velero-notifications.io/notify",
			backupName:    "any-backup",
			annotations:   map[string]string{"velero-notifications.io/notify": "true"},
			want:          true,
		},
		{
			name:          "annotation absent",
			annotationKey: "velero-notifications.io/notify",
			backupName:    "any-backup",
			want:          false,
		},
		{
			name:          "OR: name matches, annotation absent",
			patterns:      []*regexp.Regexp{daily},
			annotationKey: "velero-notifications.io/notify",
			backupName:    "daily-backup",
			want:          true,
		},
		{
			name:          "OR: annotation present, name no match",
			patterns:      []*regexp.Regexp{daily},
			annotationKey: "velero-notifications.io/notify",
			backupName:    "weekly-backup",
			annotations:   map[string]string{"velero-notifications.io/notify": "true"},
			want:          true,
		},
		{
			name:          "neither matches",
			patterns:      []*regexp.Regexp{daily},
			annotationKey: "velero-notifications.io/notify",
			backupName:    "weekly-backup",
			want:          false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			vc, _ := newTestControllerWithFilter(&mockNotifier{}, false, tc.patterns, tc.annotationKey)
			backup := newTestBackup(tc.backupName, "velero", "Completed", tc.annotations)
			got := vc.matchesFilter(backup)
			if got != tc.want {
				t.Fatalf("matchesFilter() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestHandleEvent_FilteredOut(t *testing.T) {
	t.Parallel()

	daily := regexp.MustCompile("^daily-.*")
	mock := &mockNotifier{}
	vc, _ := newTestControllerWithFilter(mock, false, []*regexp.Regexp{daily}, "")
	backup := newTestBackup("weekly-backup", "velero", "Completed", nil)

	vc.handleEvent(backup, backupsGVR, "Backup", false)

	if len(mock.calls) != 0 {
		t.Fatalf("expected 0 notify calls for filtered-out backup, got %d", len(mock.calls))
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./controllers/... -run 'TestCompilePatterns|TestMatchesFilter|TestHandleEvent_FilteredOut' -v
```

Expected: compile error — `filterPatterns`, `filterAnnotation`, `compilePatterns`, and `matchesFilter` are not defined yet.

- [ ] **Step 3: Add FilterConfig, filter fields, compilePatterns, and matchesFilter to controller.go**

Add `"regexp"` to the imports in `controllers/controller.go`.

Add the `FilterConfig` struct immediately before the `VeleroController` struct:

```go
type FilterConfig struct {
	NamePatterns  []string
	AnnotationKey string
}
```

Replace the `VeleroController` struct with:

```go
type VeleroController struct {
	Namespace        string
	ResyncPeriod     time.Duration
	Verbose          bool
	NotifyOnStartup  bool
	Notifiers        []notifications.Notifier
	dynClient        dynamic.Interface
	hasSynced        atomic.Bool
	filterPatterns   []*regexp.Regexp
	filterAnnotation string
}
```

Add `compilePatterns` after the `formatTime` function:

```go
func compilePatterns(patterns []string) ([]*regexp.Regexp, error) {
	result := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return nil, fmt.Errorf("invalid name_pattern %q: %w", p, err)
		}
		result = append(result, re)
	}
	return result, nil
}
```

Add `matchesFilter` after `compilePatterns`:

```go
func (vc *VeleroController) matchesFilter(u *unstructured.Unstructured) bool {
	if len(vc.filterPatterns) == 0 && vc.filterAnnotation == "" {
		return true
	}
	if vc.filterAnnotation != "" {
		if _, ok := u.GetAnnotations()[vc.filterAnnotation]; ok {
			return true
		}
	}
	name := u.GetName()
	for _, re := range vc.filterPatterns {
		if re.MatchString(name) {
			return true
		}
	}
	return false
}
```

Replace `handleEvent` with the version that calls `matchesFilter` right after the terminal phase check:

```go
func (vc *VeleroController) handleEvent(obj interface{}, gvr schema.GroupVersionResource, kind string, isInitialSync bool) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}

	name := u.GetName()
	namespace := u.GetNamespace()

	phase, found, err := unstructured.NestedString(u.Object, "status", "phase")
	if err != nil || !found {
		return
	}

	if !isTerminalPhase(phase) {
		if vc.Verbose && isInProgressPhase(phase) {
			log.Printf("%s %s is in %s.", kind, name, phase)
		}
		return
	}

	if !vc.matchesFilter(u) {
		return
	}

	if _, notified := u.GetAnnotations()[notifiedAnnotation]; notified {
		return
	}

	if isInitialSync && !vc.NotifyOnStartup {
		if err := vc.patchNotifiedAnnotation(gvr, namespace, name, phase); err != nil {
			log.Printf("Warning: failed to mark %s %s as seen: %v", kind, name, err)
		}
		return
	}

	message := buildMessage(u.Object, kind, name, phase)
	log.Println(message)
	vc.notifyAll(phase, message)

	if err := vc.patchNotifiedAnnotation(gvr, namespace, name, phase); err != nil {
		log.Printf("Warning: failed to mark %s %s as notified: %v", kind, name, err)
	}
}
```

- [ ] **Step 4: Run all controller tests to verify they pass**

```bash
go test ./controllers/... -v
```

Expected: PASS — all existing tests plus the 3 compilePatterns tests, 8 matchesFilter subtests, and TestHandleEvent_FilteredOut.

- [ ] **Step 5: Commit**

```bash
git add controllers/controller.go controllers/controller_test.go
git commit -m "feat: add FilterConfig, compilePatterns, and matchesFilter to controller"
```

---

### Task 3: Wire FilterConfig through NewVeleroController and main.go

**Files:**
- Modify: `controllers/controller.go`
- Modify: `main.go`

- [ ] **Step 1: Update NewVeleroController to accept FilterConfig**

Replace the `NewVeleroController` function signature and body. Add a `filters FilterConfig` parameter, compile patterns before building the struct, and store the results:

```go
func NewVeleroController(namespace string, checkInterval int, verbose bool, notifyOnStartup bool, notifiers []notifications.Notifier, filters FilterConfig) (*VeleroController, error) {
	var kubeconfig *string
	var config *rest.Config
	var err error

	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(Optional) Absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "Absolute path to the kubeconfig file")
	}

	flag.Parse()

	if *kubeconfig != "" {
		if _, err := os.Stat(*kubeconfig); err == nil {
			config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
			if err != nil {
				log.Fatalf("Failed to build kubeconfig from flag: %v", err)
			}
			log.Println("Using local kubeconfig to connect to the cluster.")
		} else {
			config, err = rest.InClusterConfig()
			if err != nil {
				log.Fatalf("Failed to retrieve in-cluster kubeconfig: %v", err)
			}
			log.Println("Kubeconfig file not found. Using in-cluster configuration to connect to the Kubernetes API server.")
		}
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			log.Fatalf("Failed to retrieve in-cluster kubeconfig: %v", err)
		}
		log.Println("Using in-cluster configuration to connect to the Kubernetes API server.")
	}

	dynClient, err := dynamic.NewForConfig(config)
	if err != nil {
		log.Fatalf("Error creating dynamic client: %v", err)
	}

	if verbose {
		log.Printf("Successfully connected to the Kubernetes API server in namespace '%s'.", namespace)
	}

	patterns, err := compilePatterns(filters.NamePatterns)
	if err != nil {
		return nil, err
	}

	resync := time.Duration(checkInterval) * time.Second
	if resync > 0 && resync < 30*time.Second {
		resync = 30 * time.Second
	}

	return &VeleroController{
		Namespace:        namespace,
		ResyncPeriod:     resync,
		Verbose:          verbose,
		NotifyOnStartup:  notifyOnStartup,
		Notifiers:        notifiers,
		dynClient:        dynClient,
		filterPatterns:   patterns,
		filterAnnotation: filters.AnnotationKey,
	}, nil
}
```

- [ ] **Step 2: Update main.go to pass FilterConfig**

Replace the `NewVeleroController` call in `main.go`:

```go
veleroController, err := controller.NewVeleroController(
	cfg.Namespace,
	cfg.CheckInterval,
	cfg.Logging.Verbose,
	cfg.Notifications.NotifyOnStartup,
	notifiers,
	controller.FilterConfig{
		NamePatterns:  cfg.Filters.NamePatterns,
		AnnotationKey: cfg.Filters.AnnotationKey,
	},
)
```

- [ ] **Step 3: Verify the build compiles**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 4: Run all tests**

```bash
go test ./...
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add controllers/controller.go main.go
git commit -m "feat: wire FilterConfig through NewVeleroController and main"
```

---

### Task 4: Update config.yaml example and Helm chart

**Files:**
- Modify: `config/config.yaml`
- Modify: `charts/velero-notifications/values.yaml`
- Modify: `charts/velero-notifications/templates/configmap.yaml`

- [ ] **Step 1: Add filters block to config/config.yaml**

Append to the end of `config/config.yaml`:

```yaml
filters:
  name_patterns: []
  #   - "^daily-.*"
  #   - "^prod-backup-.*"
  annotation_key: ""
  # annotation_key: "velero-notifications.io/notify"
```

- [ ] **Step 2: Add filters block to charts/velero-notifications/values.yaml**

Append to `charts/velero-notifications/values.yaml` before the `resources:` block:

```yaml
filters:
  # -- List of Go regex patterns matched against backup names. A backup matching any pattern is notified.
  # Leave empty to notify all backups (default).
  name_patterns: []
  # -- Annotation key for opt-in filtering. Any backup carrying this annotation key (any value) is notified.
  # Leave empty to notify all backups (default).
  annotation_key: ""
```

- [ ] **Step 3: Render filters in charts/velero-notifications/templates/configmap.yaml**

Append to the `config.yaml: |-` section (indented to match the existing content — 4 spaces):

```yaml
    filters:
      annotation_key: {{ .Values.filters.annotation_key | default "" | quote }}
      name_patterns:
      {{- range .Values.filters.name_patterns }}
        - {{ . | quote }}
      {{- end }}
```

- [ ] **Step 4: Verify the full build and tests still pass**

```bash
go build ./... && go test ./...
```

Expected: no errors, all tests pass.

- [ ] **Step 5: Commit**

```bash
git add config/config.yaml charts/velero-notifications/values.yaml charts/velero-notifications/templates/configmap.yaml
git commit -m "feat: add filters block to config.yaml and Helm chart"
```
