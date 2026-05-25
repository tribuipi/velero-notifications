package controller

import (
	"context"
	"regexp"
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
