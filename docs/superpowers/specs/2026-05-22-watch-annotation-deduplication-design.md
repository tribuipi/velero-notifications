# Watch + Annotation Deduplication Design

**Date:** 2026-05-22
**Status:** Approved

---

## Problem

`checkBackups()` has three bugs that cause missed and duplicate notifications:

1. **Backups tracked as `InProgress` are never notified.** Once a backup is added to `processedBackups`, the `continue` on the map-exists check fires on every future poll — including when the phase has since become `Completed`/`Failed`. The notification block is unreachable for those backups.

2. **Backups already in terminal state on first observation are re-notified every interval.** They are never added to `processedBackups`, so `notifyAll` fires on every tick.

3. **State is lost on restart.** `processedBackups` is in-memory only. Any completed backup becomes eligible for re-notification after a pod restart.

---

## Solution: Kubernetes Watch + Resource Annotation

Replace the polling ticker with a `DynamicSharedInformerFactory` that watches `velero.io/v1/backups`. React to `OnAdd` and `OnUpdate` events. Persist notification state as an annotation on the Backup resource itself so it survives pod restarts without any external state store.

**Annotation:**
```
velero-notifications.io/notified: "<phase>"
```
Value is the terminal phase string that triggered the notification (e.g. `Completed`, `Failed`, `PartiallyFailed`).

---

## Architecture

**Current:** ticker → `checkBackups()` → list all backups → diff against in-memory map → notify.

**New:** informer `OnAdd`/`OnUpdate` events → `handleEvent()` → check annotation → notify → patch annotation.

The `check_interval` config value is repurposed as the informer **resync period** (in seconds). A resync fires `OnUpdate` with identical old/new objects; because the annotation is already present on any notified resource, `handleEvent` exits early — no duplicate notification.

---

## Component Changes

### `config/config.go`
- Add `NotifyOnStartup bool` (yaml: `notify_on_startup`) under `Notifications`.
- `CheckInterval` stays; used as the informer resync period. A minimum of 30 seconds is enforced in code (informers with a sub-second resync are noisy and unnecessary); existing `cfg.CheckInterval < 2` guard already enforces a floor at the config layer.

### `controllers/controller.go`
- Remove `processedBackups map[string]string`.
- Add `notifyOnStartup bool` and `hasSynced atomic.Bool` to `VeleroController`.
- Remove `Run()` ticker loop. Replace with informer `Start()` + `WaitForCacheSync()` + `<-ctx.Done()` shutdown.
- Add `watchResource(gvr schema.GroupVersionResource, kind string)` — sets up one informer with shared event handlers. Called once for backups; called again for restores when that feature is added.
- Add `handleEvent(obj interface{}, kind string, isInitialSync bool)`:
  1. Extract phase from resource.
  2. If phase is not terminal → return.
  3. If annotation `velero-notifications.io/notified` is present → return.
  4. If `isInitialSync && !notifyOnStartup` → `patchNotifiedAnnotation` silently, return.
  5. Build message → `notifyAll()` → `patchNotifiedAnnotation()`.
- Add `patchNotifiedAnnotation(gvr, namespace, name, phase string) error` — issues a `types.MergePatchType` PATCH via `dynClient`. On failure: log warning, do not retry (accept rare re-notification on restart over silently missing a notification).

### `charts/.../rbac.yaml`
- Add `"patch"` to the `velero.io` backups verbs list. Add `"restores"` resource + `"patch"` when restore support is implemented.

### `config/config.yaml`
- Add `notify_on_startup: false` under `notifications:`.

---

## Event Flow

```
Kubernetes API ──► OnUpdate event fires
                      │
                      ▼
                 handleEvent(obj, "Backup", isInitialSync=false)
                      │
                      ├─ phase is terminal ✓
                      ├─ annotation absent ✓
                      ├─ build message
                      ├─ notifyAll(phase, message)       ← notify first
                      └─ patchNotifiedAnnotation(...)    ← then mark
                              │
                              └─ on PATCH failure: log warning, accept
                                 risk of re-notification on next restart
```

**`isInitialSync` detection:** `hasSynced` is an `atomic.Bool`, set to `true` after `cache.WaitForCacheSync` returns. Handlers pass `!vc.hasSynced.Load()` as `isInitialSync`. Single atomic read — no mutex needed.

**Startup behaviour (`notify_on_startup`):**
- `false` (default): pre-existing completed backups are silently annotated without notification.
- `true`: pre-existing completed backups without an annotation are notified normally.

**Terminal phases:** `Completed`, `PartiallyFailed`, `Failed`.

**Non-terminal phases** (`New`, `InProgress`, `Finalizing`, `WaitingForPluginOperations`): `handleEvent` returns immediately. Verbose "still in progress" logging is preserved via `OnUpdate` comparing old vs new phase.

---

## Testing

New file: `controllers/controller_test.go`

| Test | Scenario |
|---|---|
| `TestHandleEvent_TerminalNoAnnotation` | Terminal + no annotation → `notifyAll` called + annotation patched |
| `TestHandleEvent_AnnotationPresent` | Terminal + annotation present → nothing called |
| `TestHandleEvent_NonTerminal` | `InProgress` → nothing called |
| `TestHandleEvent_InitialSyncSkip` | Terminal + no annotation + `isInitialSync=true` + `notifyOnStartup=false` → annotation patched silently |
| `TestHandleEvent_InitialSyncNotify` | Terminal + no annotation + `isInitialSync=true` + `notifyOnStartup=true` → notification sent |
| `TestPatchNotifiedAnnotation` | MergePatch JSON is correctly formed |

Test doubles: Kubernetes `fake` dynamic client + mock `Notifier` that records calls. No real cluster required.

---

## RBAC Impact

The ClusterRole requires `patch` added to `velero.io` backups verbs. This is the only permission change. The controller does not need write access to any other resource type.

---

## Migration

No migration script needed. On first deployment:
- Backups without the annotation are treated as unseen.
- `notify_on_startup: false` (default) silently marks them, preventing a notification flood.
- Operators who want to backfill notifications for pre-existing completions can set `notify_on_startup: true` for one deployment, then revert.
