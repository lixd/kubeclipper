/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package clustercontroller

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/golang/mock/gomock"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubeclipper/kubeclipper/pkg/logger"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"

	clustermock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	operationv2store "github.com/kubeclipper/kubeclipper/pkg/models/operationv2"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

type recordingOperationStore struct {
	operationv2store.Store
	cleanupErr   error
	cleanupUID   types.UID
	cleanupCalls int
	cleanupHook  func()
}

func (s *recordingOperationStore) CleanupByTargetUID(_ context.Context, targetUID types.UID) error {
	s.cleanupCalls++
	s.cleanupUID = targetUID
	if s.cleanupHook != nil {
		s.cleanupHook()
	}
	return s.cleanupErr
}

// fakeNodeLister implements the NodeLister interface and honors the given
// label selector like the real informer-backed lister would.
type fakeNodeLister struct {
	nodes []*v1.Node
}

func (l *fakeNodeLister) List(selector labels.Selector) ([]*v1.Node, error) {
	var matched []*v1.Node
	for _, node := range l.nodes {
		if selector.Matches(labels.Set(node.Labels)) {
			matched = append(matched, node)
		}
	}
	return matched, nil
}

func (l *fakeNodeLister) Get(name string) (*v1.Node, error) {
	for _, node := range l.nodes {
		if node.Name == name {
			return node, nil
		}
	}
	return nil, errors.New("not found")
}

func TestFindOperationCluster(t *testing.T) {
	requests := findOperationCluster(&operations.Operation{Spec: operations.OperationSpec{
		TargetRef: operations.ObjectReference{Kind: "Cluster", Name: "cluster-a"},
	}})
	if len(requests) != 1 || requests[0].Name != "cluster-a" {
		t.Fatalf("findOperationCluster() = %#v, want cluster-a", requests)
	}
	if requests := findOperationCluster(&operations.Operation{Spec: operations.OperationSpec{
		TargetRef: operations.ObjectReference{Kind: "Node", Name: "node-a"},
	}}); len(requests) != 0 {
		t.Fatalf("findOperationCluster() = %#v, want no requests", requests)
	}
}

func TestKubeConfigTokenFromTasks(t *testing.T) {
	tasks := []operations.OperationTask{
		{
			Spec:   operations.OperationTaskSpec{StepID: "another-step"},
			Status: operations.OperationTaskStatus{Phase: operations.TaskSucceeded},
		},
		{
			Spec: operations.OperationTaskSpec{StepID: "capture-cluster-access"},
			Status: operations.OperationTaskStatus{
				Phase:  operations.TaskSucceeded,
				Result: &operations.TaskResult{Outputs: map[string]string{"response": " cluster-token\n"}},
			},
		},
	}
	token, err := kubeConfigTokenFromTasks(tasks)
	if err != nil {
		t.Fatal(err)
	}
	if token != "cluster-token" {
		t.Fatalf("token = %q, want cluster-token", token)
	}
	if _, err := kubeConfigTokenFromTasks(nil); err == nil {
		t.Fatal("missing task output must fail")
	}
}

func TestKubeConfigSyncOperationName(t *testing.T) {
	uid := types.UID("f1a8e6a3-9b05-40a7-9ddc-f5580c33fe88")
	if got, want := kubeConfigSyncOperationName(uid, nil), "sync-kubeconfig-f1a8e6a3-9b05-40a7-9ddc-f5580c33fe88"; got != want {
		t.Fatalf("empty kubeconfig operation name = %q, want %q", got, want)
	}
	first := kubeConfigSyncOperationName(uid, []byte("old-kubeconfig"))
	second := kubeConfigSyncOperationName(uid, []byte("old-kubeconfig"))
	if first != second {
		t.Fatalf("operation name must be stable: %q != %q", first, second)
	}
	if first == kubeConfigSyncOperationName(uid, []byte("new-kubeconfig")) {
		t.Fatal("operation name must change when the stored kubeconfig changes")
	}
}

func TestFinalizeCluster(t *testing.T) {
	// Fresh object per subtest: finalizeCluster mutates the passed-in cluster.
	newCluster := func() *v1.Cluster {
		return &v1.Cluster{ObjectMeta: metav1.ObjectMeta{
			Name:       "cluster-a",
			UID:        "cluster-uid",
			Finalizers: []string{v1.ClusterFinalizer},
		}}
	}

	// releases node occupation before anything that could fail keeps the
	// nodes blocked, then removes the finalizer only after all cleanups.
	t.Run("releases node labels first and finalizer last", func(t *testing.T) {
		controller := gomock.NewController(t)
		cluster := newCluster()
		var order []string
		store := &recordingOperationStore{cleanupHook: func() { order = append(order, "history") }}
		nodeLister := &fakeNodeLister{nodes: []*v1.Node{labeledNode("node-a", "cluster-a"), labeledNode("node-b", "cluster-a")}}
		nodeWriter := clustermock.NewMockNodeWriter(controller)
		nodeWriter.EXPECT().UpdateNode(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, node *v1.Node) (*v1.Node, error) {
				order = append(order, "release-"+node.Name)
				return node, nil
			},
		).Times(2)
		cronBackups := clustermock.NewMockCronBackupWriter(controller)
		cronBackups.EXPECT().DeleteCronBackupCollection(gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, any) error {
				order = append(order, "cronbackup")
				return nil
			},
		)
		clusterWriter := clustermock.NewMockClusterWriter(controller)
		clusterWriter.EXPECT().UpdateCluster(gomock.Any(), cluster).DoAndReturn(
			func(context.Context, *v1.Cluster) (*v1.Cluster, error) {
				order = append(order, "finalizer")
				if sets.NewString(cluster.Finalizers...).Has(v1.ClusterFinalizer) {
					t.Fatal("cluster finalizer was not removed")
				}
				return cluster, nil
			},
		)

		reconciler := &ClusterReconciler{
			OperationStore:   store,
			NodeLister:       nodeLister,
			NodeWriter:       nodeWriter,
			CronBackupWriter: cronBackups,
			ClusterWriter:    clusterWriter,
		}
		if err := reconciler.finalizeCluster(context.Background(), cluster); err != nil {
			t.Fatalf("finalize cluster: %v", err)
		}
		if store.cleanupUID != cluster.UID {
			t.Fatalf("cleanup UID = %q, want %q", store.cleanupUID, cluster.UID)
		}
		wantOrder := []string{"release-node-a", "release-node-b", "cronbackup", "history", "finalizer"}
		if !reflect.DeepEqual(order, wantOrder) {
			t.Fatalf("cleanup order = %v, want %v", order, wantOrder)
		}
	})

	t.Run("releases finalizer even when history cleanup fails", func(t *testing.T) {
		controller := gomock.NewController(t)
		cluster := newCluster()
		store := &recordingOperationStore{cleanupErr: errors.New("operation still active")}
		nodeWriter := clustermock.NewMockNodeWriter(controller)
		nodeWriter.EXPECT().UpdateNode(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		cronBackups := clustermock.NewMockCronBackupWriter(controller)
		cronBackups.EXPECT().DeleteCronBackupCollection(gomock.Any(), gomock.Any()).Return(nil)
		clusterWriter := clustermock.NewMockClusterWriter(controller)
		clusterWriter.EXPECT().UpdateCluster(gomock.Any(), gomock.Any()).DoAndReturn(
			func(context.Context, *v1.Cluster) (*v1.Cluster, error) {
				if sets.NewString(cluster.Finalizers...).Has(v1.ClusterFinalizer) {
					t.Fatal("cluster finalizer was not removed")
				}
				return cluster, nil
			},
		)
		reconciler := &ClusterReconciler{
			OperationStore:   store,
			NodeLister:       &fakeNodeLister{},
			NodeWriter:       nodeWriter,
			CronBackupWriter: cronBackups,
			ClusterWriter:    clusterWriter,
		}
		if err := reconciler.finalizeCluster(context.Background(), cluster); err != nil {
			t.Fatalf("finalize cluster: %v", err)
		}
		if store.cleanupCalls != 1 {
			t.Fatalf("cleanup calls = %d, want 1", store.cleanupCalls)
		}
	})

	// A failed node release must keep the finalizer so the reconcile retries
	// instead of leaking the node occupation labels.
	t.Run("keeps finalizer when node release fails", func(t *testing.T) {
		controller := gomock.NewController(t)
		cluster := newCluster()
		store := &recordingOperationStore{}
		nodeWriter := clustermock.NewMockNodeWriter(controller)
		nodeWriter.EXPECT().UpdateNode(gomock.Any(), gomock.Any()).Return(nil, errors.New("conflict")).AnyTimes()
		reconciler := &ClusterReconciler{
			OperationStore:   store,
			NodeLister:       &fakeNodeLister{nodes: []*v1.Node{labeledNode("node-a", "cluster-a")}},
			NodeWriter:       nodeWriter,
			CronBackupWriter: clustermock.NewMockCronBackupWriter(controller),
		}
		if err := reconciler.finalizeCluster(context.Background(), cluster); err == nil {
			t.Fatal("finalize cluster succeeded while node release failed")
		}
		if !sets.NewString(cluster.Finalizers...).Has(v1.ClusterFinalizer) {
			t.Fatalf("finalizers = %v, want cluster finalizer retained", cluster.Finalizers)
		}
	})

	t.Run("keeps finalizer when cron backup cleanup fails", func(t *testing.T) {
		controller := gomock.NewController(t)
		cluster := newCluster()
		store := &recordingOperationStore{}
		nodeWriter := clustermock.NewMockNodeWriter(controller)
		nodeWriter.EXPECT().UpdateNode(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
		cronBackups := clustermock.NewMockCronBackupWriter(controller)
		cronBackups.EXPECT().DeleteCronBackupCollection(gomock.Any(), gomock.Any()).Return(errors.New("etcd down"))
		reconciler := &ClusterReconciler{
			OperationStore:   store,
			NodeLister:       &fakeNodeLister{},
			NodeWriter:       nodeWriter,
			CronBackupWriter: cronBackups,
		}
		if err := reconciler.finalizeCluster(context.Background(), cluster); err == nil {
			t.Fatal("finalize cluster succeeded while cron backup cleanup failed")
		}
		if !sets.NewString(cluster.Finalizers...).Has(v1.ClusterFinalizer) {
			t.Fatalf("finalizers = %v, want cluster finalizer retained", cluster.Finalizers)
		}
	})
}

func TestReleaseClusterNodes(t *testing.T) {
	controller := gomock.NewController(t)
	nodeWriter := clustermock.NewMockNodeWriter(controller)
	var updated []*v1.Node
	nodeWriter.EXPECT().UpdateNode(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, node *v1.Node) (*v1.Node, error) {
			updated = append(updated, node)
			return node, nil
		},
	).Times(2)
	reconciler := &ClusterReconciler{NodeLister: &fakeNodeLister{nodes: []*v1.Node{
		labeledNode("node-a", "cluster-a"),
		labeledNode("node-b", "cluster-a"),
		labeledNode("node-c", "cluster-other"),
	}}, NodeWriter: nodeWriter}
	if err := reconciler.releaseClusterNodes(context.Background(), "cluster-a"); err != nil {
		t.Fatalf("release cluster nodes: %v", err)
	}
	if len(updated) != 2 {
		t.Fatalf("updated nodes = %d, want 2", len(updated))
	}
	for _, node := range updated {
		if _, ok := node.Labels[common.LabelClusterName]; ok {
			t.Fatalf("node %s still carries the cluster label", node.Name)
		}
		if _, ok := node.Labels[common.LabelNodeRole]; ok {
			t.Fatalf("node %s still carries the role label", node.Name)
		}
	}
}

func labeledNode(name, clusterName string) *v1.Node {
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{
		Name:   name,
		Labels: map[string]string{common.LabelClusterName: clusterName, common.LabelNodeRole: "worker"},
	}}
}

type nopLogging struct{}

func (nopLogging) Debug(_ string, _ ...zap.Field)             {}
func (nopLogging) Info(_ string, _ ...zap.Field)              {}
func (nopLogging) Warn(_ string, _ ...zap.Field)              {}
func (nopLogging) Error(_ string, _ ...zap.Field)             {}
func (nopLogging) Fatal(_ string, _ ...zap.Field)             {}
func (nopLogging) Debugf(_ string, _ ...interface{})          {}
func (nopLogging) Infof(_ string, _ ...interface{})           {}
func (nopLogging) Warnf(_ string, _ ...interface{})           {}
func (nopLogging) Errorf(_ string, _ ...interface{})          {}
func (nopLogging) Fatalf(_ string, _ ...interface{})          {}
func (l nopLogging) WithName(_ string) logger.Logging         { return l }
func (l nopLogging) WithFields(_ ...zap.Field) logger.Logging { return l }

func TestSyncClusterClientSkipsPhasesWithoutUsableKubeconfig(t *testing.T) {
	skipped := []v1.ClusterPhase{
		v1.ClusterInstalling,
		v1.ClusterInstallFailed,
		v1.ClusterTerminating,
		v1.ClusterTerminateFailed,
	}
	for _, phase := range skipped {
		t.Run(string(phase), func(t *testing.T) {
			// The store's embedded interface is nil: any attempt to reach the
			// operation store (e.g. creating a kubeconfig sync operation)
			// panics and fails the test.
			reconciler := &ClusterReconciler{OperationStore: &recordingOperationStore{}}
			cluster := &v1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", UID: "cluster-uid"},
				Status:     v1.ClusterStatus{Phase: phase},
			}
			if err := reconciler.syncClusterClient(context.Background(), nopLogging{}, cluster); err != nil {
				t.Fatalf("syncClusterClient(phase=%s) error: %v", phase, err)
			}
		})
	}

	t.Run("running cluster still requires kubeconfig sync", func(t *testing.T) {
		reconciler := &ClusterReconciler{OperationStore: &recordingOperationStore{}}
		cluster := &v1.Cluster{
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-a", UID: "cluster-uid"},
			Status:     v1.ClusterStatus{Phase: v1.ClusterRunning},
		}
		if err := reconciler.syncClusterClient(context.Background(), nopLogging{}, cluster); err == nil {
			t.Fatal("syncClusterClient(phase=Running) unexpectedly succeeded with no kubeconfig")
		}
	})
}

func TestCRIRegistryUpdateStepHasID(t *testing.T) {
	cluster := &v1.Cluster{}
	cluster.Name = "c1"
	cluster.ContainerRuntime = v1.ContainerRuntime{Type: v1.CRIContainerd, Version: "1.7.0"}
	registries := []v1.RegistrySpec{{Scheme: "https", Host: "registry.example.com"}}
	node := &v1.Node{}
	node.Name = "n1"
	step, err := CRIRegistryUpdateStep(cluster, registries, []*v1.Node{node})
	if err != nil {
		t.Fatalf("CRIRegistryUpdateStep: %v", err)
	}
	if step.ID == "" {
		t.Fatal("step must carry an ID: the operation v2 converter rejects steps without ID")
	}
}

type listOpsStore struct {
	operationv2store.Store
	ops []operations.Operation
}

func (s *listOpsStore) ListOperations(_ context.Context, _ types.UID, _ string) (*operations.OperationList, error) {
	return &operations.OperationList{Items: s.ops}, nil
}

func TestHasActiveCRIRegistryOperation(t *testing.T) {
	terminal := operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "done"}, Status: operations.OperationStatus{Phase: operations.OperationSucceeded}}
	running := operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "running"}, Status: operations.OperationStatus{Phase: operations.OperationRunning}}
	pending := operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "pending"}, Status: operations.OperationStatus{Phase: operations.OperationPending}}

	store := &listOpsStore{ops: []operations.Operation{terminal}}
	if active, err := (&ClusterReconciler{OperationStore: store}).hasActiveCRIRegistryOperation(context.Background(), "uid"); err != nil || active {
		t.Fatalf("terminal-only operations: active=%v err=%v, want false/nil", active, err)
	}

	store = &listOpsStore{ops: []operations.Operation{terminal, running}}
	if active, err := (&ClusterReconciler{OperationStore: store}).hasActiveCRIRegistryOperation(context.Background(), "uid"); err != nil || !active {
		t.Fatalf("running operation: active=%v err=%v, want true/nil", active, err)
	}

	store = &listOpsStore{ops: []operations.Operation{terminal, pending}}
	if active, err := (&ClusterReconciler{OperationStore: store}).hasActiveCRIRegistryOperation(context.Background(), "uid"); err != nil || !active {
		t.Fatalf("pending operation: active=%v err=%v, want true/nil", active, err)
	}
}

func TestUpdateCRIRegistriesSkipsDeletingCluster(t *testing.T) {
	// A cluster being deleted must not spawn further CRI-registry work: the
	// reconcile used to keep creating InstallComponents operations while the
	// uninstall ran (R21: 16k+ orphans). The guard must return before any
	// registry lookup, so a mock with zero expectations fails the test if the
	// guard is missing.
	controller := gomock.NewController(t)
	clusterOperator := clustermock.NewMockOperator(controller)
	reconciler := &ClusterReconciler{ClusterOperator: clusterOperator}
	cluster := &v1.Cluster{ObjectMeta: metav1.ObjectMeta{
		Name:              "deleting",
		UID:               "cluster-uid",
		DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
	}}
	if err := reconciler.updateCRIRegistries(context.Background(), cluster); err != nil {
		t.Fatalf("updateCRIRegistries on a deleting cluster: %v", err)
	}
}
