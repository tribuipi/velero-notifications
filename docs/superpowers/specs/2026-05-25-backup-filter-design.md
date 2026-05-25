# Backup Filter Design

**Date:** 2026-05-25
**Status:** Approved

## Overview

Add configurable filtering to `velero-notifications` so operators can choose which backups trigger notifications, based on backup name regex patterns or the presence of an opt-in annotation. A backup is notified if it matches **any** name pattern OR carries the opt-in annotation (OR logic). When no filters are configured, all backups are notified (fully backward-compatible).

## Config Schema

A new top-level `filters` block in `config.yaml`:

```yaml
filters:
  name_patterns:
    - "^daily-.*"
    - "^prod-backup-.*"
  annotation_key: "velero-notifications.io/notify"
```

- `name_patterns`: list of Go regex strings matched against the backup name; at least one must match to pass the filter
- `annotation_key`: if this annotation key is present on the backup (any value), the backup passes the filter
- Both fields are optional; omitting the entire `filters` block (or leaving both empty) means all backups are notified

`Config` struct addition in `config/config.go`:

```go
Filters struct {
    NamePatterns  []string `yaml:"name_patterns"`
    AnnotationKey string   `yaml:"annotation_key"`
} `yaml:"filters"`
```

Invalid regex patterns cause `NewVeleroController` to return an error, which `main.go` already handles via `log.Fatalf` — bad patterns kill the process at startup with a clear message.

## Controller Logic

Compiled regexes and the annotation key are stored on `VeleroController` — patterns are compiled once at construction, not per-event.

```go
type VeleroController struct {
    // existing fields ...
    filterPatterns   []*regexp.Regexp
    filterAnnotation string
}
```

A new `matchesFilter()` method is the single decision point:

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

`handleEvent` calls `matchesFilter()` early — before building the message or patching the annotation. Filtered-out backups are silently skipped (no annotation patch, no log noise unless verbose). `NewVeleroController` gains a `filters config.FiltersConfig` parameter; `main.go` passes `cfg.Filters` through.

## Helm Chart

`values.yaml` gains a `filters` block defaulting to empty (backward-compatible):

```yaml
filters:
  # name_patterns: []
  # annotation_key: ""
```

`configmap.yaml` renders the block:

```yaml
filters:
  annotation_key: {{ .Values.filters.annotation_key | default "" | quote }}
  name_patterns:
  {{- range .Values.filters.name_patterns }}
    - {{ . | quote }}
  {{- end }}
```

## Testing

Table-driven cases in `controller_test.go`:

| Case | name_patterns | annotation_key | backup name | backup annotations | Expected |
|---|---|---|---|---|---|
| No filters | `[]` | `""` | any | any | notify |
| Name match | `["^daily-.*"]` | `""` | `daily-backup` | none | notify |
| Name no match | `["^daily-.*"]` | `""` | `weekly-backup` | none | skip |
| Annotation match | `[]` | `notify` | any | `{notify: "true"}` | notify |
| Annotation absent | `[]` | `notify` | any | none | skip |
| OR: name matches, no annotation | `["^daily-.*"]` | `notify` | `daily-backup` | none | notify |
| OR: annotation present, name no match | `["^daily-.*"]` | `notify` | `weekly-backup` | `{notify: "true"}` | notify |
| Neither matches | `["^daily-.*"]` | `notify` | `weekly-backup` | none | skip |
| Invalid regex | `["[invalid"]` | `""` | — | — | error at construction |

## Files Changed

- `config/config.go` — add `Filters` struct
- `config/config.yaml` — add example `filters` block
- `controllers/controller.go` — add `filterPatterns`/`filterAnnotation` fields, `matchesFilter()`, update `NewVeleroController` signature, call filter in `handleEvent`
- `main.go` — pass `cfg.Filters` to `NewVeleroController`
- `charts/velero-notifications/values.yaml` — add `filters` block
- `charts/velero-notifications/templates/configmap.yaml` — render `filters` section
- `controllers/controller_test.go` — add table-driven filter tests
