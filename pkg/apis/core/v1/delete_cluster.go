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

package v1

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/util/retry"

	"github.com/kubeclipper/kubeclipper/pkg/logger"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

const (
	clusterDeleteOperationAnnotation   = "kubeclipper.io/delete-operation"
	clusterDeleteReservationAnnotation = "kubeclipper.io/delete-reservation"
)

func validateOperationTimeout(raw string) error {
	seconds, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || seconds < int64(operations.MinOperationTimeout/time.Second) || seconds > int64(operations.MaxOperationTimeout/time.Second) {
		return fmt.Errorf("invalid operation timeout %q: must be between %d and %d seconds", raw,
			int64(operations.MinOperationTimeout/time.Second), int64(operations.MaxOperationTimeout/time.Second))
	}
	return nil
}

// Inspect durable operations before recovering a Terminating cluster. Reuse the
// reserved name when a request stopped between the cluster and operation writes.
func (h *handler) clusterDeletionState(ctx context.Context, c *corev1.Cluster) (active bool, reservedName string, err error) {
	list, err := h.operationV2Store.ListOperations(ctx, c.UID, "")
	if err != nil {
		return false, "", err
	}
	reservedName = c.Annotations[clusterDeleteOperationAnnotation]
	for _, op := range list.Items {
		if op.Spec.TargetRef.UID != c.UID {
			continue
		}
		if op.Name == reservedName {
			reservedName = ""
		}
		executing := !op.Status.Phase.IsTerminal() || op.Spec.RetryGeneration > op.Status.ObservedRetryGeneration
		if op.Spec.Action == corev1.OperationDeleteCluster {
			if executing || op.Status.Phase == operations.OperationSucceeded {
				return true, "", nil
			}
		} else if executing {
			return false, "", fmt.Errorf("cluster has active operation %q", op.Name)
		}
	}
	return false, reservedName, nil
}

func (h *handler) submitClusterDeletion(ctx context.Context, c *corev1.Cluster, op *operations.Operation) error {
	reservation := uuid.New().String()
	// The reservation uses a fresh object and CAS: preserve controller metadata
	// and refuse to overwrite a concurrent lifecycle transition or reservation.
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		fresh, err := h.clusterOperator.GetClusterEx(ctx, c.Name, "")
		if err != nil {
			return err
		}
		if fresh.UID != c.UID || !fresh.DeletionTimestamp.IsZero() || fresh.Status.Phase != c.Status.Phase ||
			fresh.Annotations[clusterDeleteOperationAnnotation] != c.Annotations[clusterDeleteOperationAnnotation] {
			return apierrors.NewConflict(corev1.Resource("clusters"), c.Name, fmt.Errorf("cluster changed before deletion was submitted"))
		}
		fresh = fresh.DeepCopy()
		if fresh.Annotations == nil {
			fresh.Annotations = make(map[string]string)
		}
		fresh.Annotations[clusterDeleteOperationAnnotation] = op.Name
		fresh.Annotations[clusterDeleteReservationAnnotation] = reservation
		fresh.Status.Phase = corev1.ClusterTerminating
		_, err = h.clusterOperator.UpdateCluster(ctx, fresh)
		return err
	}); err != nil {
		return err
	}
	_, submitErr := h.operationV2Store.CreateOperation(ctx, op)
	if submitErr == nil {
		return nil
	}

	// A cancelled HTTP request must not prevent compensation. A failed write can
	// still have committed; use a quorum list before restoring the cluster phase.
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	list, err := h.operationV2Store.ListOperations(cleanupCtx, c.UID, "")
	if err != nil {
		return errors.Join(submitErr, fmt.Errorf("confirm deletion operation: %w", err))
	}
	for _, committed := range list.Items {
		if committed.Name == op.Name && committed.Spec.TargetRef.UID == c.UID && committed.Spec.Action == corev1.OperationDeleteCluster {
			logger.Warnf("deletion operation %s committed despite create error: %v", op.Name, submitErr)
			return nil
		}
	}
	rollbackErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		fresh, err := h.clusterOperator.GetClusterEx(cleanupCtx, c.Name, "")
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if fresh.UID != c.UID || !fresh.DeletionTimestamp.IsZero() || fresh.Status.Phase != corev1.ClusterTerminating ||
			fresh.Annotations[clusterDeleteOperationAnnotation] != op.Name ||
			fresh.Annotations[clusterDeleteReservationAnnotation] != reservation {
			return nil
		}
		fresh = fresh.DeepCopy()
		fresh.Status.Phase = c.Status.Phase
		if fresh.Status.Phase == corev1.ClusterTerminating {
			fresh.Status.Phase = corev1.ClusterTerminateFailed
		}
		delete(fresh.Annotations, clusterDeleteOperationAnnotation)
		delete(fresh.Annotations, clusterDeleteReservationAnnotation)
		_, err = h.clusterOperator.UpdateCluster(cleanupCtx, fresh)
		return err
	})
	if rollbackErr != nil {
		return errors.Join(submitErr, fmt.Errorf("restore cluster phase after deletion submission failed: %w", rollbackErr))
	}
	return submitErr
}
