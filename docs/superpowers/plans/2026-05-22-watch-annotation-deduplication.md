# Watch + Annotation Deduplication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the broken polling+in-memory-map controller with an event-driven Kubernetes Watch that uses resource annotations to durably track which backups have been notified, surviving pod restarts and correctly handling all phase transitions.

**Architecture:** A `DynamicSharedInformerFactory` watches `velero.io/v1/backups`. `OnAdd`/`OnUpdate` events drive `handleEvent()`, which checks a `velero-notifications.io/notified` annotation before notifying and patches it after. The `check_interval` config value becomes the informer resync period (floor: 30s). A `notify_on_startup` flag controls whether pre-existing completed backups fire on first run.

**Tech Stack:** Go 1.25, `k8s.io/client-go v0.32.2` (`dynamicinformer`, `cache`, `dynamic/fake`), `k8s.io/apimachinery v0.32.2` (`types.MergePatchType`), `encoding/json`, `sync/atomic`

---

## File Map

| File | Change |
|---|---|
| `config/config.go` | Add `NotifyOnStartup bool` to `Notifications` struct |
| `config/config_test.go` | New — tests for `notify_on_startup` loading |
| `controllers/controller.go` | Full rewrite: remove `checkBackups`/ticker/`processedBackups`, add informer-based watch |
| `controllers/controller_test.go` | New — unit tests for `handleEvent` and `patchNotifiedAnnotation` |
| `main.go` | Pass `cfg.Notifications.NotifyOnStartup` to `NewVeleroController` |
| `charts/velero-notifications/templates/rbac.yaml` | Add `"patch"` to `velero.io` backups verbs |
| `config/config.yaml` | Add `notify_on_startup: false` |

---

### Task 1: Add `notify_on_startup` to Config

**Files:**
- Modify: `config/config.go`
- Create: `config/config_test.go`

- [ ] **Step 1: Create failing config tests**

Create `config/config_test.go`:

```go
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
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./config/...
```

Expected: FAIL — `cfg.Notifications.NotifyOnStartup undefined`

- [ ] **Step 3: Add `NotifyOnStartup` to the Config struct**

In `config/config.go`, inside the `Notifications` struct (after `NotificationPrefix`):

```go
NotificationPrefix string `yaml:"notification_prefix"`
NotifyOnStartup    bool   `yaml:"notify_on_startup"`
```

- [ ] **Step 4: Run tests to confirm they pass**

```bash
go test ./config/...
```

Expected: `ok  github.com/zokeber/velero-notifications/config`

- [ ] **Step 5: Commit**

```bash
git add config/config.go config/config_test.go
git commit -m "feat(config): add notify_on_startup option"
```

---

### Task 2: Refactor VeleroController Struct

**Files:**
- Modify: `controllers/controller.go`

This is a structural change only — no logic added yet. The goal is to get the file compiling with the new shape before adding the new logic.

- [ ] **Step 1: Replace the import block in `controllers/controller.go`**

Replace the entire `import (...)` block with:

```go
import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	"github.com/zokeber/velero-notifications/notifications"
)
```

- [ ] **Step 2: Add the annotation constant and GVR variable after the imports**

Add after the import block, before `type VeleroController`:

```go
const notifiedAnnotation = "velero-notifications.io/notified"

var backupsGVR = schema.GroupVersionResource{
	Group:    "velero.io",
	Version:  "v1",
	Resource: "backups",
}
```

- [ ] **Step 3: Replace the `VeleroController` struct**

Replace:

```go
type VeleroController struct {
	Namespace        string
	Interval         time.Duration
	Verbose          bool
	Notifiers        []notifications.Notifier
	dynClient        dynamic.Interface
	processedBackups map[string]string
}
```

With:

```go
type VeleroController struct {
	Namespace       string
	ResyncPeriod    time.Duration
	Verbose         bool
	NotifyOnStartup bool
	Notifiers       []notifications.Notifier
	dynClient       dynamic.Interface
	hasSynced       atomic.Bool
}
```

- [ ] **Step 4: Update `NewVeleroController` signature and body**

Replace the function signature and return statement:

Old signature:
```go
func NewVeleroController(namespace string, checkInterval int, verbose bool, notifiers []notifications.Notifier) (*VeleroController, error) {
```

New signature:
```go
func NewVeleroController(namespace string, checkInterval int, verbose bool, notifyOnStartup bool, notifiers []notifications.Notifier) (*VeleroController, error) {
```

Replace the return statement at the end of `NewVeleroController`:

Old:
```go
	return &VeleroController{
		Namespace:        namespace,
		Interval:         time.Duration(checkInterval) * time.Second,
		Verbose:          verbose,
		Notifiers:        notifiers,
		dynClient:        dynClient,
		processedBackups: make(map[string]string),
	}, nil
```

New:
```go
	resync := time.Duration(checkInterval) * time.Second
	if resync > 0 && resync < 30*time.Second {
		resync = 30 * time.Second
	}

	return &VeleroController{
		Namespace:       namespace,
		ResyncPeriod:    resync,
		Verbose:         verbose,
		NotifyOnStartup: notifyOnStartup,
		Notifiers:       notifiers,
		dynClient:       dynClient,
	}, nil
```

- [ ] **Step 5: Replace `Run()` with a stub (keeps compilation)**

Replace the entire `Run()` method with:

```go
func (vc *VeleroController) Run(ctx context.Context) {
	// TODO: replaced in Task 5
	<-ctx.Done()
	log.Println("Shutting down Velero Controller.")
}
```

- [ ] **Step 6: Verify compilation**

```bash
go build ./...
```

Expected: compile error only from `main.go` (wrong number of args to `NewVeleroController`) — that's fine, it's fixed in Task 6.

---

### Task 3: Write Failing Controller Tests

**Files:**
- Create: `controllers/controller_test.go`

- [ ] **Step 1: Create the test file**

Create `controllers/controller_test.go`:

```go
package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"

	"github.com/zokeber/velero-notifications/notifications"
)

// mockNotifier records calls to Notify.
type mockNotifier struct {
	calls []struct{ status, message string }
}

func (m *mockNotifier) Notify(status, message string) error {
	m.calls = append(m.calls, struct{ status, message string }{status, message})
	return nil
}

var _ notifications.Notifier = (*mockNotifier)(nil)

// newTestBackup creates an unstructured Backup object for tests.
func newTestBackup(name, namespace, phase string, annotations map[string]string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "velero.io/v1",
			"kind":       "Backup",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"status": map[string]interface{}{
				"phase": phase,
			},
		},
	}
	if len(annotations) > 0 {
		meta := obj.Object["metadata"].(map[string]interface{})
		ann := make(map[string]interface{}, len(annotations))
		for k, v := range annotations {
			ann[k] = v
		}
		meta["annotations"] = ann
	}
	return obj
}

// newTestController returns a VeleroController wired to a fake dynamic client.
func newTestController(mock *mockNotifier, notifyOnStartup bool) (*VeleroController, *fake.FakeDynamicClient) {
	fakeClient := fake.NewSimpleDynamicClient(runtime.NewScheme())
	vc := &VeleroController{
		Namespace:       "velero",
		Verbose:         false,
		NotifyOnStartup: notifyOnStartup,
		Notifiers:       []notifications.Notifier{mock},
		dynClient:       fakeClient,
	}
	return vc, fakeClient
}

// createBackup pre-registers a backup in the fake client so PATCH can find it.
func createBackup(t *testing.T, fakeClient *fake.FakeDynamicClient, backup *unstructured.Unstructured) {
	t.Helper()
	_, err := fakeClient.Resource(backupsGVR).Namespace("velero").Create(
		context.Background(), backup, metav1.CreateOptions{},
	)
	if err != nil {
		t.Fatalf("create backup in fake client: %v", err)
	}
}

func TestPatchNotifiedAnnotation(t *testing.T) {
	t.Parallel()

	vc, fakeClient := newTestController(&mockNotifier{}, false)
	backup := newTestBackup("my-backup", "velero", "Completed", nil)
	createBackup(t, fakeClient, backup)

	if err := vc.patchNotifiedAnnotation(backupsGVR, "velero", "my-backup", "Completed"); err != nil {
		t.Fatalf("patchNotifiedAnnotation: %v", err)
	}

	got, err := fakeClient.Resource(backupsGVR).Namespace("velero").Get(
		context.Background(), "my-backup", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get backup: %v", err)
	}

	if got.GetAnnotations()[notifiedAnnotation] != "Completed" {
		t.Fatalf("expected annotation %q=%q, got map %v",
			notifiedAnnotation, "Completed", got.GetAnnotations())
	}
}

func TestHandleEvent_TerminalNoAnnotation(t *testing.T) {
	t.Parallel()

	mock := &mockNotifier{}
	vc, fakeClient := newTestController(mock, false)
	backup := newTestBackup("my-backup", "velero", "Completed", nil)
	createBackup(t, fakeClient, backup)

	vc.handleEvent(backup, backupsGVR, "Backup", false)

	if len(mock.calls) != 1 {
		t.Fatalf("expected 1 notify call, got %d", len(mock.calls))
	}
	if mock.calls[0].status != "Completed" {
		t.Fatalf("expected status %q, got %q", "Completed", mock.calls[0].status)
	}

	got, _ := fakeClient.Resource(backupsGVR).Namespace("velero").Get(
		context.Background(), "my-backup", metav1.GetOptions{},
	)
	if got.GetAnnotations()[notifiedAnnotation] != "Completed" {
		t.Fatal("expected notified annotation to be set after notification")
	}
}

func TestHandleEvent_AnnotationPresent(t *testing.T) {
	t.Parallel()

	mock := &mockNotifier{}
	vc, _ := newTestController(mock, false)
	backup := newTestBackup("my-backup", "velero", "Completed", map[string]string{
		notifiedAnnotation: "Completed",
	})

	vc.handleEvent(backup, backupsGVR, "Backup", false)

	if len(mock.calls) != 0 {
		t.Fatalf("expected 0 notify calls when annotation present, got %d", len(mock.calls))
	}
}

func TestHandleEvent_NonTerminal(t *testing.T) {
	t.Parallel()

	mock := &mockNotifier{}
	vc, _ := newTestController(mock, false)
	backup := newTestBackup("my-backup", "velero", "InProgress", nil)

	vc.handleEvent(backup, backupsGVR, "Backup", false)

	if len(mock.calls) != 0 {
		t.Fatalf("expected 0 notify calls for non-terminal phase, got %d", len(mock.calls))
	}
}

func TestHandleEvent_InitialSyncSkip(t *testing.T) {
	t.Parallel()

	mock := &mockNotifier{}
	vc, fakeClient := newTestController(mock, false) // notifyOnStartup=false
	backup := newTestBackup("old-backup", "velero", "Completed", nil)
	createBackup(t, fakeClient, backup)

	vc.handleEvent(backup, backupsGVR, "Backup", true) // isInitialSync=true

	if len(mock.calls) != 0 {
		t.Fatalf("expected 0 notify calls during initial sync with notifyOnStartup=false, got %d", len(mock.calls))
	}

	got, _ := fakeClient.Resource(backupsGVR).Namespace("velero").Get(
		context.Background(), "old-backup", metav1.GetOptions{},
	)
	if got.GetAnnotations()[notifiedAnnotation] != "Completed" {
		t.Fatal("expected annotation to be set silently during initial sync")
	}
}

func TestHandleEvent_InitialSyncNotify(t *testing.T) {
	t.Parallel()

	mock := &mockNotifier{}
	vc, fakeClient := newTestController(mock, true) // notifyOnStartup=true
	backup := newTestBackup("old-backup", "velero", "Failed", nil)
	createBackup(t, fakeClient, backup)

	vc.handleEvent(backup, backupsGVR, "Backup", true) // isInitialSync=true

	if len(mock.calls) != 1 {
		t.Fatalf("expected 1 notify call during initial sync with notifyOnStartup=true, got %d", len(mock.calls))
	}
	if mock.calls[0].status != "Failed" {
		t.Fatalf("expected status %q, got %q", "Failed", mock.calls[0].status)
	}
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
go test ./controllers/...
```

Expected: FAIL — `vc.patchNotifiedAnnotation undefined`, `vc.handleEvent undefined`

---

### Task 4: Implement `patchNotifiedAnnotation`, Helper Functions, and `handleEvent`

**Files:**
- Modify: `controllers/controller.go`

Add the following functions to `controllers/controller.go`. The existing `formatTime`, `extractWarnings`, `extractErrors`, `notifyAll` functions remain untouched.

- [ ] **Step 1: Add phase helper functions**

Add after `notifyAll`:

```go
func isTerminalPhase(phase string) bool {
	return phase == "Completed" || phase == "PartiallyFailed" || phase == "Failed"
}

func isInProgressPhase(phase string) bool {
	return phase == "InProgress" || phase == "Finalizing" || phase == "WaitingForPluginOperations"
}
```

- [ ] **Step 2: Add `buildMessage`**

Add after `isInProgressPhase`:

```go
func buildMessage(obj map[string]interface{}, kind, name, phase string) string {
	completionTimestamp, found, err := unstructured.NestedString(obj, "status", "completionTimestamp")
	if err != nil || !found {
		completionTimestamp = "Unknown"
	}
	startTimestamp, found, err := unstructured.NestedString(obj, "status", "startTimestamp")
	if err != nil || !found {
		startTimestamp = "Unknown"
	}

	progress, found, err := unstructured.NestedMap(obj, "status", "progress")
	itemsBackedUp := "Unknown"
	totalItems := "Unknown"
	if found && err == nil {
		if ib, ok := progress["itemsBackedUp"]; ok {
			itemsBackedUp = fmt.Sprintf("%v", ib)
		}
		if ti, ok := progress["totalItems"]; ok {
			totalItems = fmt.Sprintf("%v", ti)
		}
	}

	warnings := extractWarnings(obj)
	errorsCount := extractErrors(obj)

	var message string
	if phase == "Completed" {
		message = fmt.Sprintf("%s %s completed successfully.\n\nStart Time: %s, End Time: %s.\n\nProgress: %s/%s items processed",
			kind, name, formatTime(startTimestamp), formatTime(completionTimestamp), itemsBackedUp, totalItems)
	} else {
		message = fmt.Sprintf("%s %s finished with status: %s.\n\nStart Time: %s, End Time: %s.\n\nProgress: %s/%s items processed",
			kind, name, phase, formatTime(startTimestamp), formatTime(completionTimestamp), itemsBackedUp, totalItems)
		if phase == "Failed" {
			if fr, found2, err2 := unstructured.NestedString(obj, "status", "failureReason"); err2 == nil && found2 {
				message += fmt.Sprintf("\nFailure Reason: %s", fr)
			}
		}
	}

	if warnings > 0 {
		message += fmt.Sprintf(" (with %d warnings).", warnings)
	}
	if errorsCount > 0 {
		message += fmt.Sprintf(" (with %d errors).", errorsCount)
	}

	return message
}
```

- [ ] **Step 3: Add `patchNotifiedAnnotation`**

Add after `buildMessage`:

```go
func (vc *VeleroController) patchNotifiedAnnotation(gvr schema.GroupVersionResource, namespace, name, phase string) error {
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				notifiedAnnotation: phase,
			},
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}
	_, err = vc.dynClient.Resource(gvr).Namespace(namespace).Patch(
		context.TODO(),
		name,
		types.MergePatchType,
		data,
		metav1.PatchOptions{},
	)
	return err
}
```

- [ ] **Step 4: Add `handleEvent`**

Add after `patchNotifiedAnnotation`:

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

- [ ] **Step 5: Run tests to confirm they pass**

```bash
go test ./controllers/... -v
```

Expected:
```
=== RUN   TestPatchNotifiedAnnotation
--- PASS: TestPatchNotifiedAnnotation
=== RUN   TestHandleEvent_TerminalNoAnnotation
--- PASS: TestHandleEvent_TerminalNoAnnotation
=== RUN   TestHandleEvent_AnnotationPresent
--- PASS: TestHandleEvent_AnnotationPresent
=== RUN   TestHandleEvent_NonTerminal
--- PASS: TestHandleEvent_NonTerminal
=== RUN   TestHandleEvent_InitialSyncSkip
--- PASS: TestHandleEvent_InitialSyncSkip
=== RUN   TestHandleEvent_InitialSyncNotify
--- PASS: TestHandleEvent_InitialSyncNotify
ok  github.com/zokeber/velero-notifications/controllers
```

- [ ] **Step 6: Commit**

```bash
git add controllers/controller.go controllers/controller_test.go
git commit -m "feat(controller): add handleEvent, patchNotifiedAnnotation, and helpers"
```

---

### Task 5: Rewrite `Run()` with Informers and Add `watchResource`

**Files:**
- Modify: `controllers/controller.go`

- [ ] **Step 1: Replace the `Run()` stub**

Replace:

```go
func (vc *VeleroController) Run(ctx context.Context) {
	// TODO: replaced in Task 5
	<-ctx.Done()
	log.Println("Shutting down Velero Controller.")
}
```

With:

```go
func (vc *VeleroController) Run(ctx context.Context) {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(
		vc.dynClient,
		vc.ResyncPeriod,
		vc.Namespace,
		nil,
	)

	vc.watchResource(factory, backupsGVR, "Backup")

	factory.Start(ctx.Done())

	syncMap := factory.WaitForCacheSync(ctx.Done())
	for gvr, synced := range syncMap {
		if !synced {
			log.Printf("Informer for %s failed to sync.", gvr.Resource)
		}
	}
	vc.hasSynced.Store(true)

	if vc.Verbose {
		log.Printf("Controller synced. Watching for Velero events in namespace '%s'.", vc.Namespace)
	}

	<-ctx.Done()
	log.Println("Shutting down Velero Controller.")
}
```

- [ ] **Step 2: Add `watchResource` after `Run()`**

```go
func (vc *VeleroController) watchResource(factory dynamicinformer.DynamicSharedInformerFactory, gvr schema.GroupVersionResource, kind string) {
	informer := factory.ForResource(gvr)
	_, err := informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			vc.handleEvent(obj, gvr, kind, !vc.hasSynced.Load())
		},
		UpdateFunc: func(_, newObj interface{}) {
			vc.handleEvent(newObj, gvr, kind, false)
		},
	})
	if err != nil {
		log.Printf("Failed to add event handler for %s: %v", kind, err)
	}
}
```

- [ ] **Step 3: Delete the now-dead `checkBackups` function**

Remove the entire `checkBackups` method (lines 157–252 in the original file).

- [ ] **Step 4: Build to verify no unused imports**

```bash
go build ./controllers/...
```

Expected: no errors. If the compiler reports unused imports (`cache`, `dynamicinformer`, etc.), verify the functions added in Steps 1–2 reference them.

- [ ] **Step 5: Run all tests**

```bash
go test ./...
```

Expected: all packages pass. `main.go` won't compile yet (wrong arg count) — that's fixed next.

- [ ] **Step 6: Commit**

```bash
git add controllers/controller.go
git commit -m "feat(controller): replace polling ticker with informer-based Watch"
```

---

### Task 6: Update `main.go`

**Files:**
- Modify: `main.go`

- [ ] **Step 1: Pass `NotifyOnStartup` to `NewVeleroController`**

In `main.go`, replace:

```go
veleroController, err := controller.NewVeleroController(
		cfg.Namespace,
		cfg.CheckInterval,
		cfg.Logging.Verbose,
		notifiers,
	)
```

With:

```go
veleroController, err := controller.NewVeleroController(
		cfg.Namespace,
		cfg.CheckInterval,
		cfg.Logging.Verbose,
		cfg.Notifications.NotifyOnStartup,
		notifiers,
	)
```

- [ ] **Step 2: Build to verify**

```bash
go build ./...
```

Expected: no errors.

- [ ] **Step 3: Run all tests**

```bash
go test ./...
```

Expected: all packages pass.

- [ ] **Step 4: Commit**

```bash
git add main.go
git commit -m "feat(main): thread notify_on_startup into controller"
```

---

### Task 7: Update RBAC and `config.yaml`

**Files:**
- Modify: `charts/velero-notifications/templates/rbac.yaml`
- Modify: `config/config.yaml`

- [ ] **Step 1: Add `patch` verb to the ClusterRole**

In `charts/velero-notifications/templates/rbac.yaml`, replace:

```yaml
  - apiGroups: ["velero.io"]
    resources: ["backups"]
    verbs: ["get", "list", "watch"]
```

With:

```yaml
  - apiGroups: ["velero.io"]
    resources: ["backups"]
    verbs: ["get", "list", "watch", "patch"]
```

- [ ] **Step 2: Add `notify_on_startup` to `config/config.yaml`**

In `config/config.yaml`, replace:

```yaml
notifications:
  notification_prefix: "[Velero]"
```

With:

```yaml
notifications:
  notification_prefix: "[Velero]"
  notify_on_startup: false
```

- [ ] **Step 3: Run final full test suite**

```bash
go test ./...
```

Expected:
```
ok  github.com/zokeber/velero-notifications/config
ok  github.com/zokeber/velero-notifications/controllers
ok  github.com/zokeber/velero-notifications/notifications
```

- [ ] **Step 4: Commit**

```bash
git add charts/velero-notifications/templates/rbac.yaml config/config.yaml
git commit -m "feat: add patch RBAC for backup annotations and notify_on_startup config"
```
