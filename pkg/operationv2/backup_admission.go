package operationv2

import (
	"context"
	"encoding/json"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"

	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/operations/validation"
)

const (
	BackupPlanAnnotation    = "kubeclipper.io/backup-operation-plan"
	BackupRequestAnnotation = "kubeclipper.io/backup-request-digest"
	BackupOwnerAnnotation   = "kubeclipper.io/backup-owner-uid"
)

type BackupClusters interface {
	GetClusterEx(context.Context, string, string) (*corev1.Cluster, error)
	UpdateCluster(context.Context, *corev1.Cluster) (*corev1.Cluster, error)
}
type BackupOperations interface {
	ListOperations(context.Context, types.UID, string) (*operations.OperationList, error)
	CreateOperation(context.Context, *operations.Operation) (*operations.Operation, error)
}

// AttachBackupPlan makes the Backup the durable admission record. No runnable
// operation exists until this record has been stored and assigned a UID.
func AttachBackupPlan(b *corev1.Backup, op *operations.Operation) error {
	if errs := validation.ValidateOperation(op); len(errs) > 0 {
		return errs.ToAggregate()
	}
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	if len(data) > 128*1024 {
		return fmt.Errorf("backup operation plan exceeds 128 KiB")
	}
	if b.Annotations == nil {
		b.Annotations = map[string]string{}
	}
	b.Annotations[BackupPlanAnnotation] = string(data)
	b.Status.ClusterBackupStatus = corev1.ClusterBackupCreating
	return nil
}

func admittedBackupPlan(b *corev1.Backup) (*operations.Operation, error) {
	var plan operations.Operation
	if err := json.Unmarshal([]byte(b.Annotations[BackupPlanAnnotation]), &plan); err != nil {
		return nil, fmt.Errorf("decode admitted backup %q: %w", b.Name, err)
	}
	if b.UID == "" || plan.Name != b.Labels[common.LabelOperationName] ||
		plan.Spec.Action != corev1.OperationBackupCluster || plan.Spec.TargetRef.Name != b.Labels[common.LabelClusterName] ||
		plan.Labels[common.LabelBackupName] != b.Name {
		return nil, fmt.Errorf("backup %q has inconsistent admission identity", b.Name)
	}
	if errs := validation.ValidateOperation(&plan); len(errs) > 0 {
		return nil, errs.ToAggregate()
	}
	if plan.Annotations == nil {
		plan.Annotations = map[string]string{}
	}
	plan.Annotations[BackupOwnerAnnotation] = string(b.UID)
	return &plan, nil
}

// ListOperations bypasses the watch cache, including after a write times out.
func findAdmittedBackupOperation(ctx context.Context, store BackupOperations, plan *operations.Operation) (*operations.Operation, error) {
	list, err := store.ListOperations(ctx, plan.Spec.TargetRef.UID, "")
	if err != nil {
		return nil, err
	}
	for i := range list.Items {
		op := &list.Items[i]
		if op.Name != plan.Name {
			continue
		}
		if op.Spec.TargetRef != plan.Spec.TargetRef || op.Spec.Action != plan.Spec.Action ||
			op.Labels[common.LabelBackupName] != plan.Labels[common.LabelBackupName] ||
			op.Annotations[BackupOwnerAnnotation] != plan.Annotations[BackupOwnerAnnotation] {
			return nil, fmt.Errorf("operation %q belongs to another backup", plan.Name)
		}
		return op, nil
	}
	return nil, nil
}

func backupAdmissionTarget(c *corev1.Cluster, plan *operations.Operation) error {
	if c.UID != plan.Spec.TargetRef.UID || !c.DeletionTimestamp.IsZero() {
		return apierrors.NewGone("admitted backup target Cluster identity changed or is deleting")
	}
	return nil
}

// The owner marker survives terminal projection. A delayed publisher must not
// put a completed backup back into BackingUp, even after a CAS conflict.
func reserveBackupCluster(ctx context.Context, clusters BackupClusters, store BackupOperations, plan *operations.Operation) (bool, error) {
	ready := false
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		c, readErr := clusters.GetClusterEx(ctx, plan.Spec.TargetRef.Name, "")
		if readErr != nil {
			return readErr
		}
		if identityErr := backupAdmissionTarget(c, plan); identityErr != nil {
			return identityErr
		}
		if c.Annotations[BackupOwnerAnnotation] == plan.Annotations[BackupOwnerAnnotation] {
			if c.Status.Phase != corev1.ClusterBackingUp {
				return apierrors.NewGone("backup no longer owns backing-up state")
			}
			ready = true
			return nil
		}
		// Recheck after reading Cluster: the operation could have completed since
		// the caller's first read. No write is needed for an already published plan.
		existing, findErr := findAdmittedBackupOperation(ctx, store, plan)
		if existing != nil || findErr != nil {
			return findErr
		}
		if c.Status.Phase != corev1.ClusterRunning {
			return nil
		}
		c = c.DeepCopy()
		if c.Annotations == nil {
			c.Annotations = map[string]string{}
		}
		c.Annotations[BackupOwnerAnnotation] = plan.Annotations[BackupOwnerAnnotation]
		c.Status.Phase = corev1.ClusterBackingUp
		_, writeErr := clusters.UpdateCluster(ctx, c)
		ready = writeErr == nil
		return writeErr
	})
	return ready, err
}

// EnsureBackupOperation is safe to repeat after every persistence boundary,
// including a committed write whose response was lost. Only a stored Backup
// can reserve Cluster state and publish its stable Operation identity.
func EnsureBackupOperation(
	ctx context.Context, b *corev1.Backup, clusters BackupClusters, store BackupOperations,
) (*operations.Operation, error) {
	plan, err := admittedBackupPlan(b)
	if err != nil {
		return nil, err
	}
	current, err := clusters.GetClusterEx(ctx, plan.Spec.TargetRef.Name, "")
	if err != nil {
		return nil, err
	}
	if identityErr := backupAdmissionTarget(current, plan); identityErr != nil {
		return nil, identityErr
	}
	existing, err := findAdmittedBackupOperation(ctx, store, plan)
	if existing != nil || err != nil {
		return existing, err
	}
	ready, err := reserveBackupCluster(ctx, clusters, store, plan)
	if err != nil {
		return nil, err
	}
	if !ready {
		return findAdmittedBackupOperation(ctx, store, plan)
	}
	op, err := store.CreateOperation(ctx, plan)
	if err == nil {
		return op, nil
	}
	// A readback never turns a failed write into success unless it finds the
	// identity-checked committed result. Preserve both errors if readback fails.
	existing, readErr := findAdmittedBackupOperation(ctx, store, plan)
	if existing != nil && readErr == nil {
		return existing, nil
	}
	if readErr != nil {
		return nil, fmt.Errorf("publish backup operation: %w; readback: %w", err, readErr)
	}
	if apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("operation %q exists outside admitted target", plan.Name)
	}
	return nil, err
}
