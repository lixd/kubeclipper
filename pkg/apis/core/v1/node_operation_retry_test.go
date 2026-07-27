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

package v1

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kubeclipper/kubeclipper/pkg/clusteroperation"
	clustermock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/generic"
)

func TestEnqueuePendingNodeOperationRetriesConflict(t *testing.T) {
	ctrl := gomock.NewController(t)
	operator := clustermock.NewMockOperator(ctrl)
	h := &handler{
		genericConfig:   generic.NewServerRunOptions(),
		clusterOperator: operator,
	}
	patch := &clusteroperation.PatchNodes{
		Operation: clusteroperation.NodesOperationAdd,
		Role:      common.NodeRoleWorker,
		Nodes:     corev1.WorkerNodeList{{ID: "worker-1"}},
	}

	clusterAt := func(resourceVersion string) *corev1.Cluster {
		return &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", ResourceVersion: resourceVersion}}
	}
	conflict := apierrors.NewConflict(schema.GroupResource{Group: corev1.GroupName, Resource: "clusters"}, "demo", errors.New("concurrent status update"))

	gomock.InOrder(
		operator.EXPECT().GetClusterEx(gomock.Any(), "demo", "0").Return(clusterAt("2"), nil),
		operator.EXPECT().UpdateCluster(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cluster *corev1.Cluster) (*corev1.Cluster, error) {
			assertPendingNodeOperation(t, cluster, "2")
			return nil, conflict
		}),
		operator.EXPECT().GetClusterEx(gomock.Any(), "demo", "0").Return(clusterAt("3"), nil),
		operator.EXPECT().UpdateCluster(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cluster *corev1.Cluster) (*corev1.Cluster, error) {
			assertPendingNodeOperation(t, cluster, "3")
			return cluster, nil
		}),
	)

	updated, err := h.enqueuePendingNodeOperation(context.Background(), "demo", corev1.OperationAddNodes, corev1.DefaultOperationTimeoutSecs, patch)
	if err != nil {
		t.Fatalf("enqueuePendingNodeOperation() error = %v", err)
	}
	assertPendingNodeOperation(t, updated, "3")
}

func assertPendingNodeOperation(t *testing.T, cluster *corev1.Cluster, resourceVersion string) {
	t.Helper()
	if cluster.Status.Phase != corev1.ClusterUpdating {
		t.Fatalf("cluster phase = %q, want %q", cluster.Status.Phase, corev1.ClusterUpdating)
	}
	if len(cluster.Workers) != 1 || cluster.Workers[0].ID != "worker-1" {
		t.Fatalf("cluster workers = %#v, want worker-1", cluster.Workers)
	}
	if len(cluster.PendingOperations) != 1 {
		t.Fatalf("pending operations = %d, want 1", len(cluster.PendingOperations))
	}
	pending := cluster.PendingOperations[0]
	if pending.OperationType != corev1.OperationAddNodes {
		t.Fatalf("operation type = %q, want %q", pending.OperationType, corev1.OperationAddNodes)
	}
	if pending.ClusterResourceVersion != resourceVersion {
		t.Fatalf("operation resource version = %q, want %q", pending.ClusterResourceVersion, resourceVersion)
	}
}
