package operationv2

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	operationslister "github.com/kubeclipper/kubeclipper/pkg/client/lister/operations/v1alpha1"
	ctrl "github.com/kubeclipper/kubeclipper/pkg/controller-runtime"
	clustermock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	operationbuilder "github.com/kubeclipper/kubeclipper/pkg/operationv2"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

type componentOperationReader struct{ op *operations.Operation }

func (r componentOperationReader) GetOperation(_ context.Context, _, _ string) (*operations.Operation, error) {
	return r.op.DeepCopy(), nil
}

func TestComponentTerminalSuccessProjectsRunning(t *testing.T) {
	for _, action := range []string{corev1.OperationUpgradeExtension, corev1.OperationInstallComponents, corev1.OperationUninstallComponents} {
		t.Run(action, func(t *testing.T) {
			op := &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "operation", UID: "operation-uid", Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: "cluster-uid"}}, Spec: operations.OperationSpec{Action: action, TargetRef: operations.ObjectReference{Name: "cluster", UID: "cluster-uid"}}, Status: operations.OperationStatus{Phase: operations.OperationSucceeded}}
			index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			if err := index.Add(op); err != nil {
				t.Fatal(err)
			}
			cluster := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid", Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: op.Name}}, Status: corev1.ClusterStatus{Phase: corev1.ClusterUpdating}}
			mock := clustermock.NewMockOperator(gomock.NewController(t))
			mock.EXPECT().GetClusterEx(gomock.Any(), "cluster", "").DoAndReturn(func(context.Context, string, string) (*corev1.Cluster, error) { return cluster.DeepCopy(), nil }).AnyTimes()
			updates := 0
			mock.EXPECT().UpdateCluster(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, c *corev1.Cluster) (*corev1.Cluster, error) {
				updates++
				cluster = c.DeepCopy()
				return cluster, nil
			}).AnyTimes()
			r := &BusinessReconciler{Operations: operationslister.NewOperationLister(index), Clusters: mock, Store: componentOperationReader{op}}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: op.Name}}); err != nil {
				t.Fatal(err)
			}
			if updates != 1 || cluster.Status.Phase != corev1.ClusterRunning {
				t.Fatalf("%s remains %s (writes=%d)", action, cluster.Status.Phase, updates)
			}
		})
	}
}

func TestComponentTerminalFailuresAndOwnership(t *testing.T) {
	for _, action := range []string{corev1.OperationUpgradeExtension, corev1.OperationInstallComponents, corev1.OperationUninstallComponents} {
		for _, phase := range []operations.OperationPhase{operations.OperationFailed, operations.OperationTimedOut, operations.OperationCancelled} {
			t.Run(action+"/"+string(phase), func(t *testing.T) {
				op := &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "op", UID: "op-uid", Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: "cluster-uid"}}, Spec: operations.OperationSpec{Action: action, TargetRef: operations.ObjectReference{Name: "cluster", UID: "cluster-uid"}}, Status: operations.OperationStatus{Phase: phase}}
				index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
				if err := index.Add(op); err != nil {
					t.Fatal(err)
				}
				cluster := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid", Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: op.Name, operationbuilder.ComponentPlanAnnotation: "plan", "other": "preserved"}}, Status: corev1.ClusterStatus{Phase: corev1.ClusterUpdating}}
				mock := clustermock.NewMockOperator(gomock.NewController(t))
				mock.EXPECT().GetClusterEx(gomock.Any(), "cluster", "").Return(cluster, nil).AnyTimes()
				mock.EXPECT().UpdateCluster(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, c *corev1.Cluster) (*corev1.Cluster, error) {
					cluster = c.DeepCopy()
					return cluster, nil
				})
				r := &BusinessReconciler{Operations: operationslister.NewOperationLister(index), Clusters: mock, Store: componentOperationReader{op}}
				if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: op.Name}}); err != nil {
					t.Fatal(err)
				}
				if cluster.Status.Phase != corev1.ClusterUpdateFailed || cluster.Annotations["other"] != "preserved" || cluster.Annotations[operationbuilder.ComponentOwnerAnnotation] != "" || cluster.Annotations[operationbuilder.ComponentPlanAnnotation] != "" {
					t.Fatalf("incorrect terminal cluster: %#v", cluster)
				}
			})
		}
	}
}

func TestComponentStaleGenerationAndBackgroundRegistryDoNotProject(t *testing.T) {
	for _, scenario := range []string{"stale-generation", "new-owner", "background-registry"} {
		t.Run(scenario, func(t *testing.T) {
			op := &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "op", UID: "op-uid", Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: "cluster-uid"}}, Spec: operations.OperationSpec{Action: corev1.OperationInstallComponents, TargetRef: operations.ObjectReference{Name: "cluster", UID: "cluster-uid"}}, Status: operations.OperationStatus{Phase: operations.OperationFailed}}
			current := op.DeepCopy()
			owner := op.Name
			switch scenario {
			case "stale-generation":
				current.Spec.RetryGeneration = 1
			case "new-owner":
				owner = "new-op"
			case "background-registry":
				op.Annotations = nil
			}
			index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			if err := index.Add(op); err != nil {
				t.Fatal(err)
			}
			cluster := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid", Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: owner}}, Status: corev1.ClusterStatus{Phase: corev1.ClusterUpdating}}
			mock := clustermock.NewMockOperator(gomock.NewController(t))
			mock.EXPECT().GetClusterEx(gomock.Any(), "cluster", "").Return(cluster, nil).AnyTimes()
			r := &BusinessReconciler{Operations: operationslister.NewOperationLister(index), Clusters: mock, Store: componentOperationReader{current}}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: op.Name}}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestComponentRetryReservesNewGeneration(t *testing.T) {
	op := &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "op", UID: "op-uid", Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: "cluster-uid"}}, Spec: operations.OperationSpec{Action: corev1.OperationInstallComponents, TargetRef: operations.ObjectReference{Name: "cluster", UID: "cluster-uid"}, RetryGeneration: 1}, Status: operations.OperationStatus{Phase: operations.OperationFailed}}
	for _, start := range []corev1.ClusterPhase{corev1.ClusterUpdateFailed, corev1.ClusterUpdating} {
		t.Run(string(start), func(t *testing.T) {
			cluster := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid", Annotations: map[string]string{"other": "keep"}}, Status: corev1.ClusterStatus{Phase: start}}
			if start == corev1.ClusterUpdating {
				cluster.Annotations[operationbuilder.ComponentOwnerAnnotation] = op.Name
				cluster.Annotations[operationbuilder.ComponentPlanAnnotation] = "stale plan"
			}
			mock := clustermock.NewMockOperator(gomock.NewController(t))
			mock.EXPECT().GetClusterEx(gomock.Any(), "cluster", "").DoAndReturn(func(context.Context, string, string) (*corev1.Cluster, error) { return cluster.DeepCopy(), nil }).AnyTimes()
			mock.EXPECT().UpdateCluster(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, c *corev1.Cluster) (*corev1.Cluster, error) {
				cluster = c.DeepCopy()
				return cluster, nil
			})
			r := &OperationReconciler{Clusters: mock}
			if err := r.reserveComponentRetry(context.Background(), op); err != nil {
				t.Fatal(err)
			}
			if cluster.Status.Phase != corev1.ClusterUpdating || cluster.Annotations[operationbuilder.ComponentOwnerAnnotation] != "op/1" || cluster.Annotations["other"] != "keep" || cluster.Annotations[operationbuilder.ComponentPlanAnnotation] != "" {
				t.Fatalf("bad reservation: %#v", cluster)
			}
			if err := r.reserveComponentRetry(context.Background(), op); err != nil {
				t.Fatalf("retry reservation was not idempotent: %v", err)
			}
		})
	}
}

func TestComponentRetryCannotTakeAnotherReservation(t *testing.T) {
	op := &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "op"}, Spec: operations.OperationSpec{TargetRef: operations.ObjectReference{Name: "cluster", UID: "cluster-uid"}, RetryGeneration: 1}}
	for _, cluster := range []*corev1.Cluster{
		{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid", Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: "new-op"}}, Status: corev1.ClusterStatus{Phase: corev1.ClusterUpdating}},
		{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid"}, Status: corev1.ClusterStatus{Phase: corev1.ClusterBackingUp}},
		{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "recreated"}, Status: corev1.ClusterStatus{Phase: corev1.ClusterUpdateFailed}},
	} {
		mock := clustermock.NewMockOperator(gomock.NewController(t))
		mock.EXPECT().GetClusterEx(gomock.Any(), "cluster", "").Return(cluster, nil)
		if err := (&OperationReconciler{Clusters: mock}).reserveComponentRetry(context.Background(), op); err == nil {
			t.Fatalf("accepted conflicting reservation: %#v", cluster)
		}
	}
}

func TestRejectedComponentRetryReleasesLockAndRetainsFailure(t *testing.T) {
	store := newFakeStore()
	op := runningOperation("component", oneStep("install", 0, "node-1"))
	op.Spec.Action = corev1.OperationInstallComponents
	op.Spec.RetryGeneration = 1
	op.Annotations = map[string]string{operationbuilder.ComponentOwnerAnnotation: string(op.Spec.TargetRef.UID)}
	op.Status.Phase = operations.OperationFailed
	op.Status.Message = "original failure"
	store.addOperation(op)
	cluster := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: op.Spec.TargetRef.Name, UID: op.Spec.TargetRef.UID, Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: "other-op"}}, Status: corev1.ClusterStatus{Phase: corev1.ClusterUpdating}}
	mock := clustermock.NewMockOperator(gomock.NewController(t))
	mock.EXPECT().GetClusterEx(gomock.Any(), cluster.Name, "").Return(cluster, nil)
	r := &OperationReconciler{Store: store, Clusters: mock}
	reconcileOK(t, r, op.Name)
	got := store.operation(op.Name)
	if got.Status.Phase != operations.OperationFailed || got.Status.ObservedRetryGeneration != 1 || !strings.Contains(got.Status.Message, "original failure") || !strings.Contains(got.Status.Message, "retry rejected") || len(store.locks) != 0 {
		t.Fatalf("rejected retry did not release lock and retain failure: status=%#v locks=%d", got.Status, len(store.locks))
	}
}

func TestLateComponentOperationCannotExecuteWithoutReservation(t *testing.T) {
	store := newFakeStore()
	op := testOperation("component", "op-uid", testNow, oneStep("install", 0, "node-1"))
	op.Spec.Action = corev1.OperationInstallComponents
	op.Annotations = map[string]string{operationbuilder.ComponentOwnerAnnotation: string(op.Spec.TargetRef.UID)}
	store.addOperation(op)
	cluster := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: op.Spec.TargetRef.Name, UID: op.Spec.TargetRef.UID}, Status: corev1.ClusterStatus{Phase: corev1.ClusterUpdateFailed}}
	mock := clustermock.NewMockOperator(gomock.NewController(t))
	mock.EXPECT().GetClusterEx(gomock.Any(), cluster.Name, "").Return(cluster, nil)
	r := &OperationReconciler{Store: store, Clusters: mock}
	reconcileOK(t, r, op.Name)
	got := store.operation(op.Name)
	if got.Status.Phase != operations.OperationFailed || got.Status.Reason != operations.OperationReasonInvalidExecutionFacts || len(store.tasks) != 0 || len(store.locks) != 0 {
		t.Fatalf("unowned operation executed: status=%#v tasks=%d locks=%d", got.Status, len(store.tasks), len(store.locks))
	}
}

func TestComponentAdmissionProtectsAgainstHistoricalTerminalOperation(t *testing.T) {
	op := &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "old-backup", UID: "old-op"}, Spec: operations.OperationSpec{Action: corev1.OperationBackupCluster, TargetRef: operations.ObjectReference{Name: "cluster", UID: "cluster-uid"}}, Status: operations.OperationStatus{Phase: operations.OperationFailed}}
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := index.Add(op); err != nil {
		t.Fatal(err)
	}
	cluster := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid", Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: "new-operation"}}, Status: corev1.ClusterStatus{Phase: corev1.ClusterUpdating}}
	mock := clustermock.NewMockOperator(gomock.NewController(t))
	mock.EXPECT().GetClusterEx(gomock.Any(), "cluster", "").Return(cluster, nil).AnyTimes()
	r := &BusinessReconciler{Operations: operationslister.NewOperationLister(index), Clusters: mock}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: op.Name}}); err != nil {
		t.Fatal(err)
	}
}

func TestComponentTerminalConflictRereadsOwnership(t *testing.T) {
	op := &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "op", UID: "op-uid", Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: "cluster-uid"}}, Spec: operations.OperationSpec{Action: corev1.OperationInstallComponents, TargetRef: operations.ObjectReference{Name: "cluster", UID: "cluster-uid"}}, Status: operations.OperationStatus{Phase: operations.OperationSucceeded}}
	cluster := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid", Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: op.Name}}, Status: corev1.ClusterStatus{Phase: corev1.ClusterUpdating}}
	mock := clustermock.NewMockOperator(gomock.NewController(t))
	mock.EXPECT().GetClusterEx(gomock.Any(), "cluster", "").DoAndReturn(func(context.Context, string, string) (*corev1.Cluster, error) { return cluster.DeepCopy(), nil }).AnyTimes()
	mock.EXPECT().UpdateCluster(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, *corev1.Cluster) (*corev1.Cluster, error) {
		cluster.Annotations[operationbuilder.ComponentOwnerAnnotation] = "new-op"
		return nil, apierrors.NewConflict(schema.GroupResource{Resource: "clusters"}, cluster.Name, errors.New("concurrent update"))
	})
	r := &BusinessReconciler{Clusters: mock, Store: componentOperationReader{op}}
	if err := r.updateCluster(context.Background(), op, corev1.ClusterRunning, true, false); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Phase != corev1.ClusterUpdating || cluster.Annotations[operationbuilder.ComponentOwnerAnnotation] != "new-op" {
		t.Fatal("old terminal event overwrote a newer owner")
	}
}

func TestReservedComponentRetrySurvivesNewerQueuedOperation(t *testing.T) {
	store := newFakeStore()
	op := runningOperation("component", oneStep("install", 0, "node-1"))
	op.Spec.Action = corev1.OperationInstallComponents
	op.Spec.RetryGeneration = 1
	op.Annotations = map[string]string{operationbuilder.ComponentOwnerAnnotation: string(op.Spec.TargetRef.UID)}
	op.Status.Phase = operations.OperationFailed
	store.addOperation(op)
	newer := testOperation("later", "later-uid", op.CreationTimestamp.Add(time.Second), oneStep("other", 0, "node-1"))
	store.addOperation(newer)
	cluster := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: op.Spec.TargetRef.Name, UID: op.Spec.TargetRef.UID, Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: operationbuilder.ComponentOwner(op.Name, 1)}}, Status: corev1.ClusterStatus{Phase: corev1.ClusterUpdating}}
	mock := clustermock.NewMockOperator(gomock.NewController(t))
	mock.EXPECT().GetClusterEx(gomock.Any(), cluster.Name, "").Return(cluster, nil).AnyTimes()
	r := &OperationReconciler{Store: store, Clusters: mock}
	reconcileOK(t, r, op.Name)
	if got := store.operation(op.Name); got.Status.Phase != operations.OperationPending || got.Status.ObservedRetryGeneration != 1 {
		t.Fatalf("already-reserved retry was abandoned: %#v", got.Status)
	}
}

func TestOwnedComponentTerminalProjectsWithNewerQueuedOperation(t *testing.T) {
	op := testOperation("component", "op-uid", testNow, oneStep("install", 0, "node-1"))
	op.Spec.Action = corev1.OperationInstallComponents
	op.Annotations = map[string]string{operationbuilder.ComponentOwnerAnnotation: string(op.Spec.TargetRef.UID)}
	op.Status.Phase = operations.OperationSucceeded
	newer := testOperation("newer", "newer-uid", testNow.Add(time.Second), oneStep("update", 0, "node-1"))
	newer.Spec.Action = corev1.OperationUpdateCertification
	index := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	if err := index.Add(op); err != nil {
		t.Fatal(err)
	}
	if err := index.Add(newer); err != nil {
		t.Fatal(err)
	}
	cluster := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: op.Spec.TargetRef.Name, UID: op.Spec.TargetRef.UID, Annotations: map[string]string{operationbuilder.ComponentOwnerAnnotation: op.Name}}, Status: corev1.ClusterStatus{Phase: corev1.ClusterUpdating}}
	mock := clustermock.NewMockOperator(gomock.NewController(t))
	mock.EXPECT().GetClusterEx(gomock.Any(), cluster.Name, "").Return(cluster, nil).AnyTimes()
	mock.EXPECT().UpdateCluster(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, c *corev1.Cluster) (*corev1.Cluster, error) {
		cluster = c.DeepCopy()
		return cluster, nil
	})
	r := &BusinessReconciler{Operations: operationslister.NewOperationLister(index), Clusters: mock, Store: componentOperationReader{op}}
	if _, err := r.Reconcile(context.Background(), requestFor(op.Name)); err != nil {
		t.Fatal(err)
	}
	if cluster.Status.Phase != corev1.ClusterRunning {
		t.Fatal("owning operation failed to converge before newer queued operation")
	}
}
