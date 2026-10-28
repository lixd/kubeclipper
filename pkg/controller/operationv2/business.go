package operationv2

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/util/retry"

	"github.com/kubeclipper/kubeclipper/pkg/client/informers"
	operationslister "github.com/kubeclipper/kubeclipper/pkg/client/lister/operations/v1alpha1"
	ctrl "github.com/kubeclipper/kubeclipper/pkg/controller-runtime"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/controller"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/handler"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/manager"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/source"
	"github.com/kubeclipper/kubeclipper/pkg/models/cluster"
	operationbuilder "github.com/kubeclipper/kubeclipper/pkg/operationv2"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

type BusinessReconciler struct {
	Operations operationslister.OperationLister
	Clusters   cluster.Operator
	Store      interface {
		GetOperation(context.Context, string, string) (*operations.Operation, error)
	}
}

func (r *BusinessReconciler) Reconcile(ctx context.Context, request ctrl.Request) (ctrl.Result, error) {
	op, err := r.Operations.Get(request.Name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !op.Status.Phase.IsTerminal() {
		return ctrl.Result{}, nil
	}
	component := operationbuilder.IsComponentAction(op.Spec.Action) &&
		op.Annotations[operationbuilder.ComponentOwnerAnnotation] == string(op.Spec.TargetRef.UID)
	if !projectsClusterState(op.Spec.Action) && !component {
		return ctrl.Result{}, nil
	}
	all, err := r.Operations.List(labels.Everything())
	if err != nil {
		return ctrl.Result{}, err
	}
	projectPhase := true
	projectVersion := op.Spec.Action == corev1.OperationUpgradeCluster && op.Status.Phase == operations.OperationSucceeded
	for _, newer := range all {
		if newer.Spec.TargetRef.UID != op.Spec.TargetRef.UID || !operationBefore(op, newer) {
			continue
		}
		if !component && (projectsClusterState(newer.Spec.Action) || (operationbuilder.IsComponentAction(newer.Spec.Action) &&
			newer.Annotations[operationbuilder.ComponentOwnerAnnotation] != "")) {
			projectPhase = false
		}
		if newer.Spec.Action == corev1.OperationUpgradeCluster && newer.Status.Phase == operations.OperationSucceeded {
			projectVersion = false
		}
	}
	if !projectPhase && !projectVersion {
		return ctrl.Result{}, nil
	}
	if projectVersion && op.Labels[common.LabelUpgradeVersion] == "" {
		return ctrl.Result{}, fmt.Errorf("operation %q has no upgrade version", op.Name)
	}
	clusterObject, err := r.Clusters.GetClusterEx(ctx, op.Spec.TargetRef.Name, "")
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if clusterObject.UID != op.Spec.TargetRef.UID {
		return ctrl.Result{}, fmt.Errorf("operation %q target Cluster UID changed", op.Name)
	}
	owner := clusterObject.Annotations[operationbuilder.ComponentOwnerAnnotation]
	if clusterObject.Status.Phase == corev1.ClusterUpdating && owner != "" &&
		owner != operationbuilder.ComponentOwner(op.Name, op.Spec.RetryGeneration) {
		return ctrl.Result{}, nil
	}
	if projectPhase && op.Spec.Action == corev1.OperationDeleteCluster && op.Status.Phase == operations.OperationSucceeded {
		return ctrl.Result{}, r.Clusters.DeleteCluster(ctx, clusterObject.Name)
	}
	desired := failedClusterPhase(op.Spec.Action)
	if op.Status.Phase == operations.OperationSucceeded {
		desired = corev1.ClusterRunning
	}
	if err := r.updateCluster(ctx, op, desired, projectPhase, projectVersion); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func projectsClusterState(action string) bool {
	switch action {
	case corev1.OperationCreateCluster, corev1.OperationDeleteCluster, corev1.OperationUpgradeCluster,
		corev1.OperationRecoverCluster, corev1.OperationAddNodes, corev1.OperationRemoveNodes,
		corev1.OperationConvertNodes, corev1.OperationBackupCluster, corev1.OperationUpdateCertification:
		return true
	default:
		return false
	}
}

// updateCluster retries the complete read/modify/write cycle. A retry must
// re-read the object because the previous resourceVersion is no longer valid
// after a concurrent controller update.
func (r *BusinessReconciler) updateCluster(ctx context.Context, op *operations.Operation, desired corev1.ClusterPhase, projectPhase, projectVersion bool) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		clusterObject, err := r.Clusters.GetClusterEx(ctx, op.Spec.TargetRef.Name, "")
		if err != nil {
			return err
		}
		if clusterObject.UID != op.Spec.TargetRef.UID {
			return fmt.Errorf("operation %q target Cluster UID changed", op.Name)
		}
		owner := clusterObject.Annotations[operationbuilder.ComponentOwnerAnnotation]
		component := operationbuilder.IsComponentAction(op.Spec.Action) &&
			op.Annotations[operationbuilder.ComponentOwnerAnnotation] == string(clusterObject.UID)
		expectedOwner := operationbuilder.ComponentOwner(op.Name, op.Spec.RetryGeneration)
		if owner != "" && clusterObject.Status.Phase == corev1.ClusterUpdating && (!component || owner != expectedOwner) {
			return nil
		}
		if component {
			if owner != expectedOwner || clusterObject.Status.Phase != corev1.ClusterUpdating {
				return nil
			}
			if r.Store == nil {
				return fmt.Errorf("operation store is required for component projection")
			}
			current, readErr := r.Store.GetOperation(ctx, op.Name, "")
			if readErr != nil {
				return readErr
			}
			if current.UID != op.UID || current.Spec.RetryGeneration != op.Spec.RetryGeneration ||
				current.Status.ObservedRetryGeneration != op.Status.ObservedRetryGeneration ||
				current.Spec.RetryGeneration != current.Status.ObservedRetryGeneration || current.Status.Phase != op.Status.Phase {
				return nil
			}
		}
		// A durable Backup can reserve the phase before its Operation exists.
		// Historical terminal events must respect that reservation even when
		// the operation cache cannot yet see a newer Operation.
		backupOwner := clusterObject.Annotations[operationbuilder.BackupOwnerAnnotation]
		if clusterObject.Status.Phase == corev1.ClusterBackingUp && backupOwner != "" &&
			(op.Spec.Action != corev1.OperationBackupCluster || op.Annotations[operationbuilder.BackupOwnerAnnotation] != backupOwner) {
			return nil
		}
		if op.Spec.Action == corev1.OperationBackupCluster && op.Annotations[operationbuilder.BackupOwnerAnnotation] != "" {
			// Only the admission owning BackingUp may finish it. This check is
			// repeated inside CAS retries, after each fresh Cluster read.
			if clusterObject.Annotations[operationbuilder.BackupOwnerAnnotation] != op.Annotations[operationbuilder.BackupOwnerAnnotation] ||
				clusterObject.Status.Phase != corev1.ClusterBackingUp {
				return nil
			}
		}

		if (!projectPhase || clusterObject.Status.Phase == desired) &&
			(!projectVersion || clusterObject.KubernetesVersion == op.Labels[common.LabelUpgradeVersion]) {
			return nil
		}
		clusterObject = clusterObject.DeepCopy()
		if projectPhase {
			clusterObject.Status.Phase = desired
			if component {
				delete(clusterObject.Annotations, operationbuilder.ComponentOwnerAnnotation)
				delete(clusterObject.Annotations, operationbuilder.ComponentPlanAnnotation)
				delete(clusterObject.Annotations, operationbuilder.ComponentErrorAnnotation)
			}
		}
		if projectVersion {
			clusterObject.KubernetesVersion = op.Labels[common.LabelUpgradeVersion]
		}
		_, err = r.Clusters.UpdateCluster(ctx, clusterObject)
		return err
	})
}

func failedClusterPhase(action string) corev1.ClusterPhase {
	switch action {
	case corev1.OperationCreateCluster:
		return corev1.ClusterInstallFailed
	case corev1.OperationDeleteCluster:
		return corev1.ClusterTerminateFailed
	case corev1.OperationUpgradeCluster:
		return corev1.ClusterUpgradeFailed
	case corev1.OperationRecoverCluster:
		return corev1.ClusterRestoreFailed
	case corev1.OperationAddNodes, corev1.OperationRemoveNodes:
		// Node membership changes can leave a partially applied cluster state.
		// Keep the cluster usable and expose the failure on the Operation itself;
		// forcing UpdateFailed would unnecessarily block unrelated workflows.
		return corev1.ClusterRunning
	case corev1.OperationBackupCluster, corev1.OperationUpdateCertification:
		// Backup and certificate rotation do not change cluster structure, so
		// a failed attempt leaves the cluster exactly as it was (R7: a failed
		// backup used to wedge the cluster in UpdateFailed and block every
		// subsequent backup until a manual status reset).
		return corev1.ClusterRunning
	default:
		return corev1.ClusterUpdateFailed
	}
}

func (r *BusinessReconciler) SetupWithManager(mgr manager.Manager, factory informers.SharedInformerFactory) error {
	informer := factory.Operations().V1alpha1().Operations()
	informer.Informer()
	managedController, err := controller.NewUnmanaged("operation-v2-business", controller.Options{
		MaxConcurrentReconciles: 2, Reconciler: r,
		Log: mgr.GetLogger().WithName("operation-v2-business-controller"), RecoverPanic: true,
	})
	if err != nil {
		return err
	}
	operationSource := source.NewKindWithCache(&operations.Operation{}, factory)
	if err := managedController.Watch(operationSource, &handler.EnqueueRequestForObject{}); err != nil {
		return err
	}
	mgr.AddRunnable(managedController)
	return nil
}
