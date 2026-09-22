/*
 *
 *  * Copyright 2021 KubeClipper Authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package v1

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	apimachineryErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	mock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	operationv2store "github.com/kubeclipper/kubeclipper/pkg/models/operationv2"
	"github.com/kubeclipper/kubeclipper/pkg/query"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

// fakeOperationStore serves the deletion-operation lookups of activeDeletionOp
// from an in-memory map; missing names surface as real NotFound errors.
type fakeOperationStore struct {
	operationv2store.Store
	ops map[string]*operations.Operation
}

func (f *fakeOperationStore) GetOperation(ctx context.Context, name, resourceVersion string) (*operations.Operation, error) {
	if o, ok := f.ops[name]; ok {
		return o, nil
	}
	return nil, apimachineryErrors.NewNotFound(
		schema.GroupResource{Group: "operations.kubeclipper.io", Resource: "operations"}, name)
}

// A repeated delete request must reuse the committed cleanup task instead of
// creating duplicate side effects: a running operation is returned, while a
// terminal-failed or missing one falls through to a fresh retry attempt.
func TestActiveDeletionOp(t *testing.T) {
	running := &operations.Operation{}
	running.Name = "del-op"
	running.Status.Phase = operations.OperationRunning
	failed := &operations.Operation{}
	failed.Name = "del-op"
	failed.Status.Phase = operations.OperationFailed
	backupWithLabel := func() *v1.Backup {
		b := &v1.Backup{}
		b.Name = "backup-1"
		b.Labels = map[string]string{common.LabelDeleteOperationName: "del-op"}
		return b
	}

	// no committed deletion task
	h := newHandler(nil, nil, nil, &fakeOperationStore{ops: map[string]*operations.Operation{}}, nil, nil, nil)
	if h.activeDeletionOp(context.Background(), &v1.Backup{}) != nil {
		t.Fatalf("activeDeletionOp = non-nil, want nil without label")
	}

	// still running → reuse the same task
	h = newHandler(nil, nil, nil, &fakeOperationStore{ops: map[string]*operations.Operation{"del-op": running}}, nil, nil, nil)
	if h.activeDeletionOp(context.Background(), backupWithLabel()) == nil {
		t.Fatalf("activeDeletionOp = nil, want the running operation")
	}

	// terminal-failed → retry path
	h = newHandler(nil, nil, nil, &fakeOperationStore{ops: map[string]*operations.Operation{"del-op": failed}}, nil, nil, nil)
	if h.activeDeletionOp(context.Background(), backupWithLabel()) != nil {
		t.Fatalf("activeDeletionOp = non-nil, want nil for a failed operation")
	}

	// operation gone → retry path
	h = newHandler(nil, nil, nil, &fakeOperationStore{ops: map[string]*operations.Operation{}}, nil, nil, nil)
	if h.activeDeletionOp(context.Background(), backupWithLabel()) != nil {
		t.Fatalf("activeDeletionOp = non-nil, want nil for a missing operation")
	}
}

// Deleting a backup moves it into the deleting state with the cleanup
// operation recorded, and — the orphaned-file fix — keeps the record itself.
func TestCommitBackupDeletion(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewMockOperator(ctrl)
	h := newHandler(nil, m, nil, nil, nil, nil, nil)

	var saved *v1.Backup
	m.EXPECT().UpdateBackup(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, b *v1.Backup) (*v1.Backup, error) {
			saved = b
			return b, nil
		})

	b := &v1.Backup{}
	b.Name = "backup-1"
	b.Status.ClusterBackupStatus = v1.ClusterBackupAvailable
	if err := h.commitBackupDeletion(context.Background(), b, "del-op"); err != nil {
		t.Fatalf("commitBackupDeletion: %v", err)
	}
	if b.Status.ClusterBackupStatus != v1.ClusterBackupDeleting {
		t.Fatalf("status = %q, want deleting", b.Status.ClusterBackupStatus)
	}
	if b.Labels[common.LabelDeleteOperationName] != "del-op" {
		t.Fatalf("delete operation label = %q, want del-op", b.Labels[common.LabelDeleteOperationName])
	}
	if saved == nil || saved.Name != "backup-1" {
		t.Fatalf("UpdateBackup was not called with the backup")
	}
}

func newBackupHandler(t *testing.T, backups []v1.Backup) (*handler, *mock.MockOperator) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := mock.NewMockOperator(ctrl)
	list := &v1.BackupList{Items: backups}
	m.EXPECT().ListBackups(gomock.Any(), gomock.Any()).Return(list, nil).AnyTimes()
	h := newHandler(nil, m, nil, nil, nil, nil, nil)
	return h, m
}

// newCheckHandler wires a handler whose operator reports the given nodes;
// GetClusterEx always answers "not found" so the duplicate-name gate passes.
func newCheckHandler(t *testing.T, nodes []v1.Node) (*handler, *mock.MockOperator) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := mock.NewMockOperator(ctrl)
	m.EXPECT().GetClusterEx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	m.EXPECT().ListNodes(gomock.Any(), gomock.Any()).Return(&v1.NodeList{Items: nodes}, nil).AnyTimes()
	h := newHandler(nil, m, nil, nil, nil, nil, nil)
	return h, m
}

// Regression for the R7 finding: the describe endpoint previously passed
// resourceVersion into GetBackupEx as the backup name, so every existing
// backup was reported as NotFound.
func TestFindBackupExisting(t *testing.T) {
	backups := []v1.Backup{}
	b1 := v1.Backup{}
	b1.Name = "cluster-a-manual-1"
	backups = append(backups, b1)
	b2 := v1.Backup{}
	b2.Name = "cluster-b-cron-2"
	backups = append(backups, b2)

	h, _ := newBackupHandler(t, backups)

	got, err := h.findBackup(context.Background(), "cluster-b-cron-2")
	if err != nil {
		t.Fatalf("findBackup existing backup failed: %v", err)
	}
	if got.Name != "cluster-b-cron-2" {
		t.Fatalf("got backup %q, want cluster-b-cron-2", got.Name)
	}
}

func TestFindBackupNotFound(t *testing.T) {
	backups := []v1.Backup{}
	b1 := v1.Backup{}
	b1.Name = "cluster-a-manual-1"
	backups = append(backups, b1)

	h, _ := newBackupHandler(t, backups)

	_, err := h.findBackup(context.Background(), "no-such-backup")
	if err == nil {
		t.Fatalf("expected NotFound for missing backup")
	}
	if !apimachineryErrors.IsNotFound(err) {
		t.Fatalf("error = %v, want NotFound", err)
	}
}

// Regression test for R7 N9: the default watch timeout must be in seconds,
// not nanoseconds — a nanosecond-scale timer fires before the watch loop
// starts and closes the stream immediately.
func TestDefaultWatchTimeout(t *testing.T) {
	if got := defaultWatchTimeout(&query.Query{}); got < query.MinTimeoutSeconds*time.Second {
		t.Fatalf("default watch timeout %v is below MinTimeoutSeconds", got)
	}
	seconds := int64(42)
	if got := defaultWatchTimeout(&query.Query{TimeoutSeconds: &seconds}); got != 42*time.Second {
		t.Fatalf("explicit TimeoutSeconds not honored: got %v, want 42s", got)
	}
}

// createClusterCheck fixture: the cluster passes the dual-stack/overlap
// gates unless pods/svcs say otherwise, and claims exactly the given nodes.
func checkClusterFixture(pods, svcs []string, nodeIDs ...string) *v1.Cluster {
	c := &v1.Cluster{}
	c.Name = "r9-check"
	c.Networking.Pods.CIDRBlocks = pods
	c.Networking.Services.CIDRBlocks = svcs
	for _, id := range nodeIDs {
		c.Masters = append(c.Masters, v1.WorkerNode{ID: id})
	}
	return c
}

func checkNodeFixture(name, ip string) v1.Node {
	n := v1.Node{}
	n.Name = name
	n.Status.NodeIpv4DefaultIP = ip
	return n
}

func TestCreateClusterCheckOverlapRejectedBeforeStore(t *testing.T) {
	h, _ := newCheckHandler(t, nil)
	// no store expectations: any GetClusterEx/ListNodes call fails the test
	c := checkClusterFixture([]string{"10.96.0.0/16"}, []string{"10.96.0.0/12"}, "node-1")
	err := h.createClusterCheck(context.Background(), c)
	if err == nil || !strings.Contains(err.Error(), "overlaps") {
		t.Fatalf("expected overlap rejection, got %v", err)
	}
}

func TestCreateClusterCheckHostConflict(t *testing.T) {
	nodes := []v1.Node{
		checkNodeFixture("node-1", "172.16.131.208"),
		checkNodeFixture("node-2", "172.16.131.146"),
	}
	h, _ := newCheckHandler(t, nodes)
	c := checkClusterFixture([]string{"172.16.131.0/24"}, []string{"10.96.0.0/12"}, "node-1", "node-2")
	err := h.createClusterCheck(context.Background(), c)
	if err == nil || !strings.Contains(err.Error(), "pod subnet 172.16.131.0/24 conflicts with node node-1 address 172.16.131.208") {
		t.Fatalf("expected host conflict rejection, got %v", err)
	}

	// same check on the service list
	c2 := checkClusterFixture([]string{"172.20.0.0/16"}, []string{"172.16.131.144/28"}, "node-1", "node-2")
	err = h.createClusterCheck(context.Background(), c2)
	if err == nil || !strings.Contains(err.Error(), "service subnet 172.16.131.144/28 conflicts with node node-2 address 172.16.131.146") {
		t.Fatalf("expected service host conflict rejection, got %v", err)
	}
}

func TestCreateClusterCheckPasses(t *testing.T) {
	nodes := []v1.Node{
		checkNodeFixture("node-1", "172.16.131.208"),
		checkNodeFixture("node-2", "172.16.131.146"),
	}
	h, _ := newCheckHandler(t, nodes)
	c := checkClusterFixture(
		[]string{"172.20.0.0/16", "fd00:172:20::/64"},
		[]string{"10.96.0.0/12", "fd00:10:96::/64"},
		"node-1", "node-2")
	if err := h.createClusterCheck(context.Background(), c); err != nil {
		t.Fatalf("expected dual-stack creation to pass, got %v", err)
	}
}

func TestCreateClusterCheckSkipsIPLessNodes(t *testing.T) {
	nodes := []v1.Node{
		checkNodeFixture("node-1", ""), // no discovered address yet
		checkNodeFixture("node-2", "172.16.131.146"),
	}
	h, _ := newCheckHandler(t, nodes)
	c := checkClusterFixture([]string{"172.20.0.0/16"}, []string{"10.96.0.0/12"}, "node-1", "node-2")
	if err := h.createClusterCheck(context.Background(), c); err != nil {
		t.Fatalf("expected clean subnets to pass, got %v", err)
	}
}
