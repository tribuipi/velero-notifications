package controller

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
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"

	"github.com/zokeber/velero-notifications/notifications"
)

const notifiedAnnotation = "velero-notifications.io/notified"

var backupsGVR = schema.GroupVersionResource{
	Group:    "velero.io",
	Version:  "v1",
	Resource: "backups",
}

type VeleroController struct {
	Namespace       string
	ResyncPeriod    time.Duration
	Verbose         bool
	NotifyOnStartup bool
	Notifiers       []notifications.Notifier
	dynClient       dynamic.Interface
	hasSynced       atomic.Bool
}

func formatTime(tStr string) string {
	t, err := time.Parse(time.RFC3339, tStr)
	if err != nil {
		return tStr
	}
	return t.Format("01/02/06 at 3:04 PM MST")
}

func NewVeleroController(namespace string, checkInterval int, verbose bool, notifyOnStartup bool, notifiers []notifications.Notifier) (*VeleroController, error) {
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
}

func (vc *VeleroController) Run(ctx context.Context) {
	// replaced in Task 5
	<-ctx.Done()
	log.Println("Shutting down Velero Controller.")
}

func (vc *VeleroController) notifyAll(status, message string) {
	for _, notifier := range vc.Notifiers {
		if err := notifier.Notify(status, message); err != nil {
			log.Printf("Error sending notifications: %v", err)
		}
	}
}

func extractWarnings(obj map[string]interface{}) int {
	warnings := 0
	if w, found, err := unstructured.NestedFieldCopy(obj, "status", "warnings"); err == nil && found {
		switch v := w.(type) {
		case int:
			warnings = v
		case int64:
			warnings = int(v)
		case float64:
			warnings = int(v)
		case string:
			if val, err := strconv.Atoi(v); err == nil {
				warnings = val
			}
		}
	}
	return warnings
}

func extractErrors(obj map[string]interface{}) int {
	errorsCount := 0
	if e, found, err := unstructured.NestedFieldCopy(obj, "status", "errors"); err == nil && found {
		switch v := e.(type) {
		case int:
			errorsCount = v
		case int64:
			errorsCount = int(v)
		case float64:
			errorsCount = int(v)
		case string:
			if val, err := strconv.Atoi(v); err == nil {
				errorsCount = val
			}
		}
	}
	return errorsCount
}

func isTerminalPhase(phase string) bool {
	return phase == "Completed" || phase == "PartiallyFailed" || phase == "Failed"
}

func isInProgressPhase(phase string) bool {
	return phase == "InProgress" || phase == "Finalizing" || phase == "WaitingForPluginOperations"
}

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

// checkBackups is kept for reference; will be deleted in Task 5.
func (vc *VeleroController) checkBackups() {
	processedBackups := make(map[string]string) // TODO(Task5): remove with checkBackups
	list, err := vc.dynClient.Resource(backupsGVR).Namespace(vc.Namespace).List(context.TODO(), metav1.ListOptions{})

	if err != nil {
		log.Printf("Failed to retrieving backups from Velero: %v", err)
		vc.notifyAll("Error", fmt.Sprintf("Failed to retrieving backups from Velero: %v", err))
		return
	}

	if vc.Verbose {
		log.Printf("Found %d backups in namespace '%s'.", len(list.Items), vc.Namespace)
	}

	for _, item := range list.Items {
		backupName, _, _ := unstructured.NestedString(item.Object, "metadata", "name")
		phase, found, err := unstructured.NestedString(item.Object, "status", "phase")
		if err != nil || !found {
			log.Printf("Backup %s is not supported.", backupName)
			continue
		}

		if _, exists := processedBackups[backupName]; exists {
			continue
		} else if phase == "InProgress" || phase == "Finalizing" || phase == "WaitingForPluginOperations" {
			processedBackups[backupName] = phase
			if vc.Verbose {
				log.Printf("New backup detected in %s: %s.", phase, backupName)
			}
		}

		if phase == "Completed" || phase == "PartiallyFailed" || phase == "Failed" {
			completionTimestamp, found, err := unstructured.NestedString(item.Object, "status", "completionTimestamp")
			if err != nil || !found {
				completionTimestamp = "Unknown"
			}
			startTimestamp, found, err := unstructured.NestedString(item.Object, "status", "startTimestamp")
			if err != nil || !found {
				startTimestamp = "Unknown"
			}
			progress, found, err := unstructured.NestedMap(item.Object, "status", "progress")
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

			warnings := extractWarnings(item.Object)
			errorsCount := extractErrors(item.Object)
			failureReason := ""

			if phase == "Failed" {
				if fr, found, err := unstructured.NestedString(item.Object, "status", "failureReason"); err == nil && found {
					failureReason = fr
				}
			}

			var message string
			if phase == "Completed" {
				message = fmt.Sprintf("Backup %s completed successfully.\n\nStart Time: %s, End Time: %s.\n\nProgress: %s/%s items processed", backupName, formatTime(startTimestamp), formatTime(completionTimestamp), itemsBackedUp, totalItems)
			} else {
				message = fmt.Sprintf("Backup %s finished with status: %s.\n\nStart Time: %s, End Time: %s.\n\nProgress: %s/%s items processed", backupName, phase, formatTime(startTimestamp), formatTime(completionTimestamp), itemsBackedUp, totalItems)
				if failureReason != "" {
					message += fmt.Sprintf("\nFailure Reason: %s", failureReason)
				}
			}

			if warnings > 0 {
				message += fmt.Sprintf(" (with %d warnings).", warnings)
			}

			if errorsCount > 0 {
				message += fmt.Sprintf(" (with %d errors).", errorsCount)
			}

			log.Println(message)
			vc.notifyAll(phase, message)

			delete(processedBackups, backupName)
		}

		if vc.Verbose && (phase == "InProgress" || phase == "Finalizing" || phase == "WaitingForPluginOperations") {
			log.Printf("Backup %s is still in %s.", backupName, phase)
		}
	}
}
