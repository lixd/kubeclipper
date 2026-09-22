/*
 *
 *  * Copyright 2026 KubeClipper Authors.
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

package backupcontroller

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	apimachineryErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"

	mock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	"github.com/kubeclipper/kubeclipper/pkg/logger"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

// fakeOperationLister backs reconcileDeletion tests with an in-memory
// operation view; missing names surface as real NotFound errors.
type fakeOperationLister struct {
	ops map[string]*operations.Operation
}

func (f *fakeOperationLister) List(selector labels.Selector) ([]*operations.Operation, error) {
	out := make([]*operations.Operation, 0, len(f.ops))
	for _, o := range f.ops {
		out = append(out, o)
	}
	return out, nil
}

func (f *fakeOperationLister) Get(name string) (*operations.Operation, error) {
	if o, ok := f.ops[name]; ok {
		return o, nil
	}
	return nil, apimachineryErrors.NewNotFound(
		schema.GroupResource{Group: "operations.kubeclipper.io", Resource: "operations"}, name)
}

func backupWithDeleteOp(name, opName string, status corev1.ClusterBackupStatus) *corev1.Backup {
	b := &corev1.Backup{}
	b.Name = name
	b.Labels = map[string]string{
		common.LabelClusterName:          "cluster-a",
		common.LabelDeleteOperationName:  opName,
	}
	b.Status.ClusterBackupStatus = status
	return b
}

func TestOperationNameIndexFuncCoversCreationAndDeletion(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		OperationNameIndex: operationNameIndexFunc,
	})
	for _, backup := range []*corev1.Backup{
		{ObjectMeta: metav1.ObjectMeta{Name: "by-creation", Labels: map[string]string{common.LabelOperationName: "operation-1"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "by-deletion", Labels: map[string]string{common.LabelDeleteOperationName: "operation-1"}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "other", Labels: map[string]string{common.LabelOperationName: "operation-2"}}},
	} {
		if err := indexer.Add(backup); err != nil {
			t.Fatal(err)
		}
	}

	requests := mapObjectsForOperation(indexer)(&operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "operation-1"}})
	got := map[string]bool{}
	for _, r := range requests {
		got[r.Name] = true
	}
	if len(got) != 2 || !got["by-creation"] || !got["by-deletion"] {
		t.Fatalf("requests = %#v, want both by-creation and by-deletion", requests)
	}
}

// After the cleanup operation succeeds the record is finally removed; the
// physical file is already gone at that point.
func TestReconcileDeletionSucceededRemovesRecord(t *testing.T) {
	ctrl := gomock.NewController(t)
	bw := mock.NewMockBackupWriter(ctrl)
	bw.EXPECT().DeleteBackup(gomock.Any(), "backup-1").Return(nil)

	r := &BackupReconciler{
		OperationLister: &fakeOperationLister{ops: map[string]*operations.Operation{
			"del-op": {ObjectMeta: metav1.ObjectMeta{Name: "del-op"}, Status: operations.OperationStatus{Phase: operations.OperationSucceeded}},
		}},
		BackupWriter: bw,
	}
	b := backupWithDeleteOp("backup-1", "del-op", corev1.ClusterBackupDeleting)
	if err := r.reconcileDeletion(context.Background(), logger.FromContext(context.Background()), b); err != nil {
		t.Fatalf("reconcileDeletion: %v", err)
	}
}

// A failed cleanup keeps the record in deleteFailed so the deletion stays
// visible and retryable — this is the orphaned-file fix.
func TestReconcileDeletionFailedKeepsRecord(t *testing.T) {
	ctrl := gomock.NewController(t)
	bw := mock.NewMockBackupWriter(ctrl)
	bw.EXPECT().DeleteBackup(gomock.Any(), gomock.Any()).Times(0)
	bw.EXPECT().UpdateBackup(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, b *corev1.Backup) (*corev1.Backup, error) {
			if b.Status.ClusterBackupStatus != corev1.ClusterBackupDeleteFailed {
				t.Fatalf("status = %q, want deleteFailed", b.Status.ClusterBackupStatus)
			}
			return b, nil
		})

	r := &BackupReconciler{
		OperationLister: &fakeOperationLister{ops: map[string]*operations.Operation{
			"del-op": {ObjectMeta: metav1.ObjectMeta{Name: "del-op"}, Status: operations.OperationStatus{Phase: operations.OperationFailed}},
		}},
		BackupWriter: bw,
	}
	b := backupWithDeleteOp("backup-1", "del-op", corev1.ClusterBackupDeleting)
	if err := r.reconcileDeletion(context.Background(), logger.FromContext(context.Background()), b); err != nil {
		t.Fatalf("reconcileDeletion: %v", err)
	}
}

// While the cleanup operation runs the backup stays in deleting; an already
// deleting record must not be rewritten.
func TestReconcileDeletionRunningKeepsDeletingWithoutWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	bw := mock.NewMockBackupWriter(ctrl)
	bw.EXPECT().UpdateBackup(gomock.Any(), gomock.Any()).Times(0)
	bw.EXPECT().DeleteBackup(gomock.Any(), gomock.Any()).Times(0)

	r := &BackupReconciler{
		OperationLister: &fakeOperationLister{ops: map[string]*operations.Operation{
			"del-op": {ObjectMeta: metav1.ObjectMeta{Name: "del-op"}, Status: operations.OperationStatus{Phase: operations.OperationRunning}},
		}},
		BackupWriter: bw,
	}
	b := backupWithDeleteOp("backup-1", "del-op", corev1.ClusterBackupDeleting)
	if err := r.reconcileDeletion(context.Background(), logger.FromContext(context.Background()), b); err != nil {
		t.Fatalf("reconcileDeletion: %v", err)
	}
}

// A missing delete operation (cluster-cascade purge) must not silently drop
// the record; it stays in deleteFailed and visible.
func TestReconcileDeletionOpMissingKeepsRecord(t *testing.T) {
	ctrl := gomock.NewController(t)
	bw := mock.NewMockBackupWriter(ctrl)
	bw.EXPECT().DeleteBackup(gomock.Any(), gomock.Any()).Times(0)
	bw.EXPECT().UpdateBackup(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, b *corev1.Backup) (*corev1.Backup, error) {
			if b.Status.ClusterBackupStatus != corev1.ClusterBackupDeleteFailed {
				t.Fatalf("status = %q, want deleteFailed", b.Status.ClusterBackupStatus)
			}
			return b, nil
		})

	r := &BackupReconciler{
		OperationLister: &fakeOperationLister{ops: map[string]*operations.Operation{}},
		BackupWriter:    bw,
	}
	b := backupWithDeleteOp("backup-1", "del-op", corev1.ClusterBackupDeleting)
	if err := r.reconcileDeletion(context.Background(), logger.FromContext(context.Background()), b); err != nil {
		t.Fatalf("reconcileDeletion: %v", err)
	}
}
