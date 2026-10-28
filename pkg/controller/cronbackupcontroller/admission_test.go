package cronbackupcontroller

import (
	"context"
	"net"
	"testing"

	"github.com/golang/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	listerv1 "github.com/kubeclipper/kubeclipper/pkg/client/lister/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/logger"
	clustermock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	operationstore "github.com/kubeclipper/kubeclipper/pkg/models/operationv2"
	operationbuilder "github.com/kubeclipper/kubeclipper/pkg/operationv2"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
	bs "github.com/kubeclipper/kubeclipper/pkg/simple/backupstore"
)

type scheduledOperationStore struct {
	operationstore.Store
	created *operations.Operation
}

func (s *scheduledOperationStore) CreateOperation(_ context.Context, op *operations.Operation) (*operations.Operation, error) {
	s.created = op
	return op, nil
}

func TestScheduledBackupReleasesPreviousManualAdmissionOwner(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:6443")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	c := &v1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid", Labels: map[string]string{common.LabelBackupPoint: "point"}, Annotations: map[string]string{operationbuilder.BackupOwnerAnnotation: "finished-manual-backup"}}, KubernetesVersion: "v1.36.4", Masters: []v1.WorkerNode{{ID: "master"}}, Status: v1.ClusterStatus{Phase: v1.ClusterRunning}}
	node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "master", UID: "master-uid", Labels: map[string]string{common.LabelClusterName: c.Name}}}
	node.Status.Ipv4DefaultIP = "127.0.0.1"
	node.Status.NodeInfo.Hostname = "master"
	point := &v1.BackupPoint{ObjectMeta: metav1.ObjectMeta{Name: "point"}, StorageType: bs.FSStorage, FsConfig: &v1.FsConfig{BackupRootDir: "/tmp/backup"}}
	clusters := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	nodes := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	points := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	backups := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	for _, item := range []struct {
		index  cache.Indexer
		object any
	}{{clusters, c}, {nodes, node}, {points, point}} {
		if err := item.index.Add(item.object); err != nil {
			t.Fatal(err)
		}
	}
	writer := clustermock.NewMockOperator(gomock.NewController(t))
	writer.EXPECT().GetNodeEx(gomock.Any(), node.Name, "").Return(node, nil).AnyTimes()
	writer.EXPECT().CreateBackup(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, b *v1.Backup) (*v1.Backup, error) { return b, nil })
	writer.EXPECT().UpdateCluster(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, next *v1.Cluster) (*v1.Cluster, error) {
		if next.Annotations[operationbuilder.BackupOwnerAnnotation] != "" || next.Status.Phase != v1.ClusterBackingUp {
			t.Fatalf("scheduled backup retained old owner: %v", next.Annotations)
		}
		return next, nil
	})
	store := &scheduledOperationStore{}
	r := CronBackupReconciler{ClusterWriter: writer, ClusterLister: listerv1.NewClusterLister(clusters), NodeLister: listerv1.NewNodeLister(nodes), BackupLister: listerv1.NewBackupLister(backups), OperationStore: store, NodeReader: writer, BackupWriter: writer, BackupPointLister: listerv1.NewBackupPointLister(points)}
	cb := &v1.CronBackup{ObjectMeta: metav1.ObjectMeta{Name: "scheduled", UID: "cron-uid"}, Spec: v1.CronBackupSpec{ClusterName: c.Name}}
	if err := r.createBackup(logger.FromContext(context.Background()), cb); err != nil {
		t.Fatal(err)
	}
	if store.created == nil || store.created.Annotations[operationbuilder.BackupOwnerAnnotation] != "" {
		t.Fatal("scheduled operation ownership changed")
	}
	if c.Status.Phase != v1.ClusterRunning || c.Annotations[operationbuilder.BackupOwnerAnnotation] != "finished-manual-backup" {
		t.Fatal("shared informer cluster mutated")
	}
}
