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

package backupcontroller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"

	"github.com/kubeclipper/kubeclipper/pkg/client/informers"
	listerv1 "github.com/kubeclipper/kubeclipper/pkg/client/lister/core/v1"
	operationslister "github.com/kubeclipper/kubeclipper/pkg/client/lister/operations/v1alpha1"
	ctrl "github.com/kubeclipper/kubeclipper/pkg/controller-runtime"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/client"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/controller"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/handler"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/manager"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/reconcile"
	"github.com/kubeclipper/kubeclipper/pkg/controller-runtime/source"
	"github.com/kubeclipper/kubeclipper/pkg/logger"
	"github.com/kubeclipper/kubeclipper/pkg/models/cluster"
	operationv2store "github.com/kubeclipper/kubeclipper/pkg/models/operationv2"
	operationbuilder "github.com/kubeclipper/kubeclipper/pkg/operationv2"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1/k8s"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

type BackupReconciler struct {
	AdmissionClusters operationbuilder.BackupClusters
	ClusterLister     listerv1.ClusterLister
	BackupLister      listerv1.BackupLister
	OperationLister   operationslister.OperationLister
	OperationStore    operationv2store.Store
	BackupWriter      cluster.BackupWriter
}

func (r *BackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logger.FromContext(ctx)

	b, err := r.BackupLister.Get(req.Name)
	if err != nil {
		// backup not found, possibly been deleted
		// need to do the cleanup
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		log.Error("Failed to get backup with name", zap.Error(err))
		return ctrl.Result{}, err
	}

	// Informer objects are shared. Never mutate them while reconciling.
	b = b.DeepCopy()
	err = r.updateBackupStatus(ctx, log, b)
	if b.Annotations[operationbuilder.BackupPlanAnnotation] != "" && b.Status.ClusterBackupStatus == v1.ClusterBackupCreating {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
	}
	return ctrl.Result{}, err
}

func (r *BackupReconciler) SetupWithManager(mgr manager.Manager, informerCache informers.InformerCache) error {
	backupInformer, err := informerCache.GetInformer(context.Background(), &v1.Backup{})
	if err != nil {
		return err
	}
	indexErr := backupInformer.AddIndexers(cache.Indexers{
		OperationNameIndex: operationNameIndexFunc,
	})
	if indexErr != nil {
		return indexErr
	}
	c, err := controller.NewUnmanaged("backup", controller.Options{
		MaxConcurrentReconciles: 2,
		Reconciler:              r,
		Log:                     mgr.GetLogger().WithName("backup-controller"),
		RecoverPanic:            true,
	})
	if err != nil {
		return err
	}
	watchErr := c.Watch(source.NewKindWithCache(&v1.Backup{}, informerCache), &handler.EnqueueRequestForObject{})
	if watchErr != nil {
		return watchErr
	}
	if watchErr := c.Watch(
		source.NewKindWithCache(&operations.Operation{}, informerCache),
		handler.EnqueueRequestsFromMapFunc(mapObjectsForOperation(backupInformer.GetIndexer())),
	); watchErr != nil {
		return watchErr
	}
	mgr.AddRunnable(c)
	return nil
}

func (r *BackupReconciler) updateBackupStatus(ctx context.Context, log logger.Logging, b *v1.Backup) error {
	// A backup with a committed deletion task is driven entirely by that
	// operation: the record is removed only after the storage cleanup
	// succeeds, and a failed cleanup moves it to deleteFailed for retry. The
	// creation-operation branches below must not touch such backups — their
	// creation operation may long have been purged.
	if b.Labels[common.LabelDeleteOperationName] != "" {
		return r.reconcileDeletion(ctx, log, b)
	}

	before := b.Status
	if err := r.projectBackupStatus(ctx, b); err != nil {
		return err
	}
	if b.Status == before {
		return nil
	}
	_, err := r.BackupWriter.UpdateBackup(ctx, b)
	return err
}

func (r *BackupReconciler) projectBackupStatus(ctx context.Context, b *v1.Backup) error {
	_, clusterErr := r.ClusterLister.Get(b.Labels[common.LabelClusterName])
	admitted := b.Annotations[operationbuilder.BackupPlanAnnotation] != "" && b.Status.ClusterBackupStatus == v1.ClusterBackupCreating
	if admitted {
		_, clusterErr = r.AdmissionClusters.GetClusterEx(ctx, b.Labels[common.LabelClusterName], "")
	}
	if clusterErr != nil {
		if !errors.IsNotFound(clusterErr) {
			return clusterErr
		}
		if admitted {
			b.Status.ClusterBackupStatus = v1.ClusterBackupError
			b.Annotations["kubeclipper.io/backup-admission-error"] = "target Cluster no longer exists"
			return nil
		}
		return r.BackupWriter.DeleteBackup(ctx, b.Name)
	}
	op, operationErr := r.backupOperation(ctx, b)
	if operationErr != nil {
		if !errors.IsNotFound(operationErr) && !errors.IsGone(operationErr) {
			return operationErr
		}
		b.Status.ClusterBackupStatus = v1.ClusterBackupError
		if admitted {
			b.Annotations["kubeclipper.io/backup-admission-error"] = operationErr.Error()
		}
		return nil
	}
	if op == nil {
		return nil
	} // another operation owns Cluster; retry the durable admission
	return r.applyBackupOperationStatus(ctx, b, op)
}

func (r *BackupReconciler) backupOperation(ctx context.Context, b *v1.Backup) (*operations.Operation, error) {
	raw := b.Annotations[operationbuilder.BackupPlanAnnotation]
	if raw != "" && b.Status.ClusterBackupStatus == v1.ClusterBackupCreating {
		var plan operations.Operation
		if err := json.Unmarshal([]byte(raw), &plan); err != nil {
			return nil, fmt.Errorf("decode backup admission: %w", err)
		}
		// Restore replaces the operation association. Never republish the old
		// creation plan after another lifecycle operation takes over the Backup.
		if plan.Name == b.Labels[common.LabelOperationName] {
			return operationbuilder.EnsureBackupOperation(ctx, b, r.AdmissionClusters, r.OperationStore)
		}
	}
	return r.OperationLister.Get(b.Labels[common.LabelOperationName])
}

func (r *BackupReconciler) applyBackupOperationStatus(ctx context.Context, b *v1.Backup, op *operations.Operation) error {
	switch op.Status.Phase {
	case operations.OperationRunning:
		switch op.Spec.Action {
		case v1.OperationBackupCluster:
			b.Status.ClusterBackupStatus = v1.ClusterBackupCreating
		case v1.OperationRecoverCluster:
			b.Status.ClusterBackupStatus = v1.ClusterBackupRestoring
		}
	case operations.OperationSucceeded:
		if op.Spec.Action == v1.OperationBackupCluster && b.Status.ClusterBackupStatus == v1.ClusterBackupCreating {
			return r.completeBackup(ctx, b, op)
		}
		if op.Spec.Action == v1.OperationRecoverCluster && b.Status.ClusterBackupStatus == v1.ClusterBackupRestoring {
			b.Status.ClusterBackupStatus = v1.ClusterBackupAvailable
		}
	default:
		if op.Status.Phase.IsTerminal() {
			// A failed restore leaves its existing snapshot usable. A failed backup
			// cannot become available based on another operation's Cluster phase.
			b.Status.ClusterBackupStatus = v1.ClusterBackupError
			if op.Spec.Action == v1.OperationRecoverCluster {
				b.Status.ClusterBackupStatus = v1.ClusterBackupAvailable
			}
		}
	}
	return nil
}

func (r *BackupReconciler) completeBackup(ctx context.Context, b *v1.Backup, op *operations.Operation) error {
	tasks, err := r.OperationStore.ListTasksByOperationUID(ctx, op.UID, "")
	if err != nil {
		return err
	}
	checkFile := k8s.CheckFile{}
	for i := range tasks.Items {
		if tasks.Items[i].Status.Result == nil {
			continue
		}
		response := tasks.Items[i].Status.Result.Outputs["response"]
		if response != "" && json.Unmarshal([]byte(response), &checkFile) == nil && checkFile.BackupFileMD5 != "" {
			break
		}
	}
	if checkFile.BackupFileSize <= 0 || checkFile.BackupFileMD5 == "" {
		return fmt.Errorf("backup result incomplete: size=%d md5-present=%t", checkFile.BackupFileSize, checkFile.BackupFileMD5 != "")
	}
	b.Status.BackupFileSize = checkFile.BackupFileSize
	b.Status.BackupFileMD5 = checkFile.BackupFileMD5
	b.Status.ClusterBackupStatus = v1.ClusterBackupAvailable
	return nil
}

// OperationNameIndex is the backup informer index keyed by operation names.
const OperationNameIndex = "operationNameIndex"

// reconcileDeletion follows the committed storage-cleanup operation of a
// backup. A successful operation means the FS/S3 object is gone and the
// record can finally be removed; a terminal failure keeps the record in
// deleteFailed with the operation reference for retry; a missing operation
// (only the cluster-cascade purge removes operations) keeps the record
// visible in deleteFailed rather than silently dropping it.
func (r *BackupReconciler) reconcileDeletion(ctx context.Context, log logger.Logging, b *v1.Backup) error {
	o, err := r.OperationLister.Get(b.Labels[common.LabelDeleteOperationName])
	if errors.IsNotFound(err) {
		log.Warnf("backup(%s) delete operation(%s) not found, keeping record in deleteFailed",
			b.Name, b.Labels[common.LabelDeleteOperationName])
		return r.setBackupStatus(ctx, log, b, v1.ClusterBackupDeleteFailed)
	}
	if err != nil {
		return err
	}
	switch {
	case o.Status.Phase == operations.OperationSucceeded:
		return r.BackupWriter.DeleteBackup(ctx, b.Name)
	case o.Status.Phase.IsTerminal():
		return r.setBackupStatus(ctx, log, b, v1.ClusterBackupDeleteFailed)
	default:
		return r.setBackupStatus(ctx, log, b, v1.ClusterBackupDeleting)
	}
}

func (r *BackupReconciler) setBackupStatus(ctx context.Context, log logger.Logging, b *v1.Backup, status v1.ClusterBackupStatus) error {
	if b.Status.ClusterBackupStatus == status {
		return nil
	}
	b = b.DeepCopy()
	b.Status.ClusterBackupStatus = status
	if _, err := r.BackupWriter.UpdateBackup(ctx, b); err != nil {
		log.Warnf("backup(%s) sync status failed: %s", b.Name, err.Error())
		return err
	}
	return nil
}

// operationNameIndexFunc keys Backups by both their creation operation and
// their committed deletion operation, so operation events fan out to the
// matching backups without a full lister scan.
func operationNameIndexFunc(raw any) ([]string, error) {
	backup, ok := raw.(*v1.Backup)
	if !ok || backup.Labels == nil {
		return nil, nil
	}
	var names []string
	for _, key := range []string{common.LabelOperationName, common.LabelDeleteOperationName} {
		if backup.Labels[key] != "" {
			names = append(names, backup.Labels[key])
		}
	}
	return names, nil
}

func mapObjectsForOperation(indexer cache.Indexer) handler.MapFunc {
	return func(clu client.Object) []reconcile.Request {
		matched, err := indexer.ByIndex(OperationNameIndex, clu.GetName())
		if err != nil {
			return []reconcile.Request{}
		}
		requests := make([]reconcile.Request, 0, len(matched))
		for _, raw := range matched {
			backup, ok := raw.(*v1.Backup)
			if !ok {
				continue
			}
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name: backup.Name,
				},
			})
		}
		return requests
	}
}
