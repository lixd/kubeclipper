package backupcontroller

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	corelister "github.com/kubeclipper/kubeclipper/pkg/client/lister/core/v1"
	ctrl "github.com/kubeclipper/kubeclipper/pkg/controller-runtime"
	clustermock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	operationstore "github.com/kubeclipper/kubeclipper/pkg/models/operationv2"
	operationbuilder "github.com/kubeclipper/kubeclipper/pkg/operationv2"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

type admissionOperationFacts struct {
	operationstore.Store
	op *operations.Operation
}

func (s *admissionOperationFacts) ListOperations(context.Context, types.UID, string) (*operations.OperationList, error) {
	return &operations.OperationList{Items: []operations.Operation{*s.op.DeepCopy()}}, nil
}
func (*admissionOperationFacts) ListTasksByOperationUID(context.Context, types.UID, string) (*operations.OperationTaskList, error) {
	return &operations.OperationTaskList{Items: []operations.OperationTask{{Status: operations.OperationTaskStatus{Result: &operations.TaskResult{Outputs: map[string]string{"response": `{"backupFileSize":42,"backupFileMD5":"snapshot-checksum"}`}}}}}}, nil
}

func TestAdmittedBackupConvergesWithoutObservingRunningInCache(t *testing.T) {
	for _, phase := range []operations.OperationPhase{operations.OperationSucceeded, operations.OperationFailed, operations.OperationTimedOut, operations.OperationCancelled, operations.OperationRunning, operations.OperationPending} {
		t.Run(string(phase), func(t *testing.T) {
			b := &corev1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "snapshot", UID: "backup-uid", Labels: map[string]string{common.LabelClusterName: "cluster", common.LabelOperationName: "op"}}}
			op := &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "op", Labels: map[string]string{common.LabelBackupName: "snapshot"}}, Spec: operations.OperationSpec{TargetRef: operations.ObjectReference{Kind: corev1.KindCluster, Name: "cluster", UID: "cluster-uid"}, Action: corev1.OperationBackupCluster, DesiredState: operations.OperationDesiredStateActive, Timeout: metav1.Duration{Duration: time.Minute}, Steps: []operations.OperationStep{{ID: "snapshot", Executor: "custom", Targets: []operations.NodeReference{{Name: "node", UID: "node-uid"}}}}}}
			if err := operationbuilder.AttachBackupPlan(b, op); err != nil {
				t.Fatal(err)
			}
			op.UID = "op-uid"
			op.Annotations = map[string]string{operationbuilder.BackupOwnerAnnotation: string(b.UID)}
			op.Status.Phase = phase
			c := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid"}, Status: corev1.ClusterStatus{Phase: corev1.ClusterRunning}}
			clusters := clustermock.NewMockOperator(gomock.NewController(t))
			clusters.EXPECT().GetClusterEx(gomock.Any(), "cluster", "").Return(c.DeepCopy(), nil).AnyTimes()
			ci := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			bi := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
			if err := ci.Add(c); err != nil {
				t.Fatal(err)
			}
			if err := bi.Add(b); err != nil {
				t.Fatal(err)
			}
			if phase.IsTerminal() {
				clusters.EXPECT().UpdateBackup(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, got *corev1.Backup) (*corev1.Backup, error) {
					want := corev1.ClusterBackupError
					if phase == operations.OperationSucceeded {
						want = corev1.ClusterBackupAvailable
						if got.Status.BackupFileSize != 42 || got.Status.BackupFileMD5 != "snapshot-checksum" {
							t.Fatal("available without real task result fields")
						}
					}
					if got.Status.ClusterBackupStatus != want {
						t.Fatalf("got %s want %s", got.Status.ClusterBackupStatus, want)
					}
					if err := bi.Update(got.DeepCopy()); err != nil {
						t.Fatal(err)
					}
					return got, nil
				})
			}
			r := &BackupReconciler{AdmissionClusters: clusters, ClusterLister: corelister.NewClusterLister(ci), BackupLister: corelister.NewBackupLister(bi), BackupWriter: clusters, OperationStore: &admissionOperationFacts{op: op}, OperationLister: &fakeOperationLister{ops: map[string]*operations.Operation{op.Name: op}}}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: b.Name}}); err != nil {
				t.Fatal(err)
			}
			// Replaying the same observed state must not write the same status again.
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: b.Name}}); err != nil {
				t.Fatal(err)
			}

			if b.Status.ClusterBackupStatus != corev1.ClusterBackupCreating {
				t.Fatal("shared informer object was mutated")
			}
		})
	}
}

func TestFailedRestorePreservesExistingSnapshot(t *testing.T) {
	r := &BackupReconciler{}
	for _, phase := range []operations.OperationPhase{operations.OperationFailed, operations.OperationTimedOut, operations.OperationCancelled} {
		b := &corev1.Backup{Status: corev1.BackupStatus{ClusterBackupStatus: corev1.ClusterBackupRestoring, BackupFileMD5: "existing", BackupFileSize: 42}}
		op := &operations.Operation{Spec: operations.OperationSpec{Action: corev1.OperationRecoverCluster}, Status: operations.OperationStatus{Phase: phase}}
		if err := r.applyBackupOperationStatus(context.Background(), b, op); err != nil {
			t.Fatal(err)
		}
		if b.Status.ClusterBackupStatus != corev1.ClusterBackupAvailable || b.Status.BackupFileMD5 != "existing" {
			t.Fatalf("failed restore lost its snapshot: %+v", b.Status)
		}
	}
}
