package operationv2

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"

	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/operations/validation"
)

const (
	ComponentOwnerAnnotation = "kubeclipper.io/component-operation-owner"
	ComponentPlanAnnotation  = "kubeclipper.io/component-operation-plan"
	ComponentErrorAnnotation = "kubeclipper.io/component-operation-error"
)

type ComponentOperations interface {
	GetOperation(context.Context, string, string) (*operations.Operation, error)
	CreateOperation(context.Context, *operations.Operation) (*operations.Operation, error)
}

// AttachComponentPlan records the executable plan and its owner in the same
// Cluster update as the desired component configuration and Updating phase.
func AttachComponentPlan(cluster *corev1.Cluster, op *operations.Operation) error {
	if !IsComponentAction(op.Spec.Action) || op.Spec.TargetRef.Name != cluster.Name || op.Spec.TargetRef.UID != cluster.UID {
		return fmt.Errorf("component operation target or action is invalid")
	}
	if op.Annotations == nil {
		op.Annotations = map[string]string{}
	}
	op.Annotations[ComponentOwnerAnnotation] = string(cluster.UID)
	if errs := validation.ValidateOperation(op); len(errs) > 0 {
		return errs.ToAggregate()
	}
	data, err := json.Marshal(op)
	if err != nil {
		return err
	}
	if len(data) > 128*1024 {
		return fmt.Errorf("component operation plan exceeds 128 KiB")
	}
	if cluster.Annotations == nil {
		cluster.Annotations = map[string]string{}
	}
	cluster.Annotations[ComponentOwnerAnnotation] = op.Name
	cluster.Annotations[ComponentPlanAnnotation] = string(data)
	delete(cluster.Annotations, ComponentErrorAnnotation)
	cluster.Status.Phase = corev1.ClusterUpdating
	return nil
}

type ComponentClusters interface {
	GetClusterEx(context.Context, string, string) (*corev1.Cluster, error)
	UpdateCluster(context.Context, *corev1.Cluster) (*corev1.Cluster, error)
}

// RecordComponentPublication preserves the error on the admission record while
// the same validated plan is retried. It never rolls back an uncertain write.
func RecordComponentPublication(ctx context.Context, clusters ComponentClusters, admitted *corev1.Cluster, publishErr error) error {
	message := ""
	if publishErr != nil {
		message = publishErr.Error()
		const maxErrorBytes = 4096
		if len(message) > maxErrorBytes {
			message = strings.ToValidUTF8(message[:maxErrorBytes], "")
		}
	}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := clusters.GetClusterEx(ctx, admitted.Name, "")
		if err != nil {
			return err
		}
		if current.UID != admitted.UID || current.Status.Phase != corev1.ClusterUpdating ||
			current.Annotations[ComponentOwnerAnnotation] != admitted.Annotations[ComponentOwnerAnnotation] ||
			current.Annotations[ComponentPlanAnnotation] != admitted.Annotations[ComponentPlanAnnotation] {
			return nil
		}
		if current.Annotations[ComponentErrorAnnotation] == message {
			return nil
		}
		current = current.DeepCopy()
		if message == "" {
			delete(current.Annotations, ComponentErrorAnnotation)
		} else {
			current.Annotations[ComponentErrorAnnotation] = message
		}
		_, err = clusters.UpdateCluster(ctx, current)
		return err
	})
}

func IsComponentAction(action string) bool {
	return action == corev1.OperationUpgradeExtension || action == corev1.OperationInstallComponents ||
		action == corev1.OperationUninstallComponents
}

func ComponentOwner(name string, generation int64) string {
	if generation == 0 {
		return name
	}
	return fmt.Sprintf("%s/%d", name, generation)
}

// PublishComponentPlan is idempotent across lost create responses and server
// restarts. It must be called with a strongly read Cluster, never an informer
// object that may contain an already-completed reservation.
func PublishComponentPlan(ctx context.Context, cluster *corev1.Cluster, store ComponentOperations) error {
	data := cluster.Annotations[ComponentPlanAnnotation]
	if data == "" {
		return nil
	}
	plan, err := componentPlan(cluster, data)
	if err != nil {
		return err
	}
	existing, err := store.GetOperation(ctx, plan.Name, "")
	if apierrors.IsNotFound(err) {
		_, err = store.CreateOperation(ctx, plan)
		if err == nil {
			return nil
		}
		// A subsequent reconciliation performs a fresh read before retrying.
		return err
	}
	if err != nil {
		return err
	}
	if !SameComponentPlan(existing, plan) {
		return fmt.Errorf("operation %q belongs to a different reservation", plan.Name)
	}
	return nil
}

func SameComponentPlan(existing, plan *operations.Operation) bool {
	return existing.Name == plan.Name && existing.Spec.TargetRef == plan.Spec.TargetRef &&
		existing.Spec.Action == plan.Spec.Action && existing.Spec.Timeout == plan.Spec.Timeout &&
		apiequality.Semantic.DeepEqual(existing.Spec.Steps, plan.Spec.Steps) &&
		existing.Annotations[ComponentOwnerAnnotation] == plan.Annotations[ComponentOwnerAnnotation]
}

func componentPlan(cluster *corev1.Cluster, data string) (*operations.Operation, error) {
	var plan operations.Operation
	if err := json.Unmarshal([]byte(data), &plan); err != nil {
		return nil, fmt.Errorf("decode component operation plan: %w", err)
	}
	if cluster.Status.Phase != corev1.ClusterUpdating || !cluster.DeletionTimestamp.IsZero() ||
		plan.Name != cluster.Annotations[ComponentOwnerAnnotation] || plan.Spec.TargetRef.Name != cluster.Name ||
		plan.Spec.TargetRef.UID != cluster.UID || plan.Annotations[ComponentOwnerAnnotation] != string(cluster.UID) ||
		!IsComponentAction(plan.Spec.Action) {
		return nil, fmt.Errorf("component operation reservation identity or phase changed")
	}
	if errs := validation.ValidateOperation(&plan); len(errs) != 0 {
		return nil, errs.ToAggregate()
	}
	return &plan, nil
}
