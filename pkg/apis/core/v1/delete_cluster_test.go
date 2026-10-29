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
	"github.com/kubeclipper/kubeclipper/pkg/models"
	"github.com/kubeclipper/kubeclipper/pkg/models/cluster"
	operationstore "github.com/kubeclipper/kubeclipper/pkg/models/operationv2"
	"github.com/kubeclipper/kubeclipper/pkg/query"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/generic"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/emicklei/go-restful"
	"github.com/golang/mock/gomock"
	clustermock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
)

func TestDeleteClusterRejectsInvalidTimeoutBeforeMutation(t *testing.T) {
	for _, timeout := range []string{"abc", "0", "-1", "59", "86401", "9223372036854775807"} {
		t.Run(timeout, func(t *testing.T) {
			operator := clustermock.NewMockOperator(gomock.NewController(t))
			h := newHandler(nil, operator, nil, nil, nil, nil, nil)
			container := restful.NewContainer()
			ws := new(restful.WebService)
			ws.Path("/").Produces(restful.MIME_JSON)
			ws.Route(ws.DELETE("/clusters/{name}").To(h.DeleteCluster))
			container.Add(ws)
			response := httptest.NewRecorder()
			container.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/clusters/demo?timeout="+timeout, nil))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body: %s", response.Code, response.Body.String())
			}
		})
	}
}

// This fixture exercises handler/storage failure boundaries without host tasks.
type deleteClusterOperator struct {
	cluster.Operator
	deleteCalls    int
	deleteErr      error
	current        *corev1.Cluster
	invalidNodeUID bool
	updateCalls    int
	update         func(context.Context, *corev1.Cluster) error
}

func (f *deleteClusterOperator) GetClusterEx(_ context.Context, _, _ string) (*corev1.Cluster, error) {
	return f.current.DeepCopy(), nil
}

func (f *deleteClusterOperator) UpdateCluster(ctx context.Context, c *corev1.Cluster) (*corev1.Cluster, error) {
	f.updateCalls++
	if f.update != nil {
		if err := f.update(ctx, c); err != nil {
			return nil, err
		}
	}
	f.current = c.DeepCopy()
	return f.current.DeepCopy(), nil
}

func (f *deleteClusterOperator) DeleteCluster(context.Context, string) error {
	f.deleteCalls++
	return f.deleteErr
}

func (f *deleteClusterOperator) ListBackupEx(context.Context, *query.Query) (*models.PageableResponse, error) {
	return &models.PageableResponse{}, nil
}

func (f *deleteClusterOperator) GetNodeEx(_ context.Context, name, _ string) (*corev1.Node, error) {
	uid := types.UID(name)
	if f.invalidNodeUID {
		uid = ""
	}
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name, UID: uid, Labels: map[string]string{common.LabelTopologyRegion: "default"}},
		Status: corev1.NodeStatus{Ipv4DefaultIP: "192.0.2.10", NodeIpv4DefaultIP: "192.0.2.10"}}, nil
}

type deleteOperationStore struct {
	operationstore.Store
	items       []operations.Operation
	create      func(context.Context, *operations.Operation) (*operations.Operation, error)
	listErr     error
	createCalls int
}

func (f *deleteOperationStore) CreateOperation(ctx context.Context, op *operations.Operation) (*operations.Operation, error) {
	f.createCalls++
	if f.create != nil {
		return f.create(ctx, op)
	}
	f.items = append(f.items, *op.DeepCopy())
	return op.DeepCopy(), nil
}

func (f *deleteOperationStore) ListOperations(ctx context.Context, _ types.UID, _ string) (*operations.OperationList, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &operations.OperationList{Items: f.items}, f.listErr
}

func deleteClusterFixture(phase corev1.ClusterPhase) *corev1.Cluster {
	return &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "demo", UID: "cluster-uid", Finalizers: []string{"cluster-finalizer"}},
		Status: corev1.ClusterStatus{Phase: phase}, Masters: corev1.WorkerNodeList{{ID: "master"}}, KubernetesVersion: "v1.28.0",
		ContainerRuntime: corev1.ContainerRuntime{Type: corev1.CRIContainerd, Version: "v1.6.24"}}
}

func deletionOperation() *operations.Operation {
	return &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "delete-op"},
		Spec:   operations.OperationSpec{Action: corev1.OperationDeleteCluster, TargetRef: operations.ObjectReference{Name: "demo", UID: "cluster-uid"}},
		Status: operations.OperationStatus{Phase: operations.OperationPending}}
}

func TestSubmitClusterDeletion(t *testing.T) {
	submitErr := errors.New("operation write failed")
	for _, name := range []string{"success", "write failed", "request cancelled", "committed despite error", "confirmation failed", "rollback conflict", "rollback failed", "target replaced", "phase changed", "reservation changed"} {
		t.Run(name, func(t *testing.T) {
			original := deleteClusterFixture(corev1.ClusterRunning)
			operator := &deleteClusterOperator{current: original.DeepCopy()}
			store := &deleteOperationStore{}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wantPhase, wantErr := corev1.ClusterRunning, true
			store.create = func(_ context.Context, op *operations.Operation) (*operations.Operation, error) {
				switch name {
				case "success":
					store.items = append(store.items, *op.DeepCopy())
					return op.DeepCopy(), nil
				case "request cancelled":
					cancel()
				case "committed despite error":
					store.items = append(store.items, *op.DeepCopy())
				case "confirmation failed":
					store.listErr = errors.New("quorum read failed")
				case "target replaced":
					operator.current.UID = "replacement-uid"
				case "phase changed":
					operator.current.Status.Phase = corev1.ClusterUpgradeFailed
				case "reservation changed":
					operator.current.Annotations[clusterDeleteReservationAnnotation] = "another-request"
				}
				return nil, submitErr
			}
			switch name {
			case "success", "committed despite error":
				wantPhase, wantErr = corev1.ClusterTerminating, false
			case "confirmation failed", "rollback failed", "target replaced", "reservation changed":
				wantPhase = corev1.ClusterTerminating
			case "phase changed":
				wantPhase = corev1.ClusterUpgradeFailed
			}
			operator.update = func(ctx context.Context, c *corev1.Cluster) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if name == "rollback conflict" && operator.updateCalls == 2 {
					operator.current.Finalizers = append(operator.current.Finalizers, "concurrent-finalizer")
					return apierrors.NewConflict(corev1.Resource("clusters"), c.Name, errors.New("stale resource version"))
				}
				if name == "rollback failed" && operator.updateCalls > 1 {
					return errors.New("cluster write failed")
				}
				return nil
			}
			h := newHandler(nil, operator, nil, store, nil, nil, nil)
			err := h.submitClusterDeletion(ctx, original, deletionOperation())
			if (err != nil) != wantErr {
				t.Fatalf("error=%v, wantErr=%v", err, wantErr)
			}
			if wantErr && !errors.Is(err, submitErr) {
				t.Fatalf("original error lost: %v", err)
			}
			if operator.current.Status.Phase != wantPhase {
				t.Fatalf("phase=%s, want %s", operator.current.Status.Phase, wantPhase)
			}
			if original.Status.Phase != corev1.ClusterRunning {
				t.Fatal("mutated request snapshot")
			}
			if name == "rollback conflict" && len(operator.current.Finalizers) != 2 {
				t.Fatal("rollback lost concurrent finalizer")
			}
			if name == "rollback failed" && !strings.Contains(err.Error(), "restore cluster phase") {
				t.Fatalf("rollback error hidden: %v", err)
			}
			if wantPhase == corev1.ClusterRunning && operator.current.Annotations[clusterDeleteOperationAnnotation] != "" {
				t.Fatal("reservation remained after rollback")
			}
		})
	}
}

func TestClusterDeletionState(t *testing.T) {
	for _, name := range []string{"missing", "pending", "running", "succeeded", "failed", "retry requested", "other cluster", "other action", "read failed"} {
		t.Run(name, func(t *testing.T) {
			c := deleteClusterFixture(corev1.ClusterTerminating)
			c.Annotations = map[string]string{clusterDeleteOperationAnnotation: "delete-op"}
			op := deletionOperation()
			store := &deleteOperationStore{items: []operations.Operation{*op}}
			wantActive, wantName, wantErr := false, "", false
			switch name {
			case "missing":
				store.items = nil
				wantName = "delete-op"
			case "pending":
				wantActive = true
			case "running":
				store.items[0].Status.Phase = operations.OperationRunning
				wantActive = true
			case "succeeded":
				store.items[0].Status.Phase = operations.OperationSucceeded
				wantActive = true
			case "failed":
				store.items[0].Status.Phase = operations.OperationFailed
			case "retry requested":
				store.items[0].Status.Phase = operations.OperationFailed
				store.items[0].Spec.RetryGeneration = 1
				wantActive = true
			case "other cluster":
				store.items[0].Spec.TargetRef.UID = "different-uid"
				wantName = "delete-op"
			case "other action":
				store.items[0].Spec.Action = corev1.OperationUpgradeCluster
				wantErr = true
			case "read failed":
				store.listErr = errors.New("read failed")
				wantErr = true
			}
			h := newHandler(nil, nil, nil, store, nil, nil, nil)
			active, reserved, err := h.clusterDeletionState(context.Background(), c)
			if (err != nil) != wantErr || active != wantActive || (!wantErr && reserved != wantName) {
				t.Fatalf("got active=%v reserved=%q error=%v", active, reserved, err)
			}
		})
	}
}

func TestDeleteClusterSubmissionFailureIsRetryable(t *testing.T) {
	operator := &deleteClusterOperator{current: deleteClusterFixture(corev1.ClusterRunning)}
	store := &deleteOperationStore{create: func(context.Context, *operations.Operation) (*operations.Operation, error) {
		return nil, errors.New("injected storage failure")
	}}
	h := newHandler(&generic.ServerRunOptions{}, operator, nil, store, nil, nil, nil)
	container := restful.NewContainer()
	ws := new(restful.WebService)
	ws.Path("/").Produces(restful.MIME_JSON)
	ws.Route(ws.DELETE("/clusters/{name}").To(h.DeleteCluster))
	container.Add(ws)
	response := httptest.NewRecorder()
	container.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/clusters/demo", nil))
	if response.Code != http.StatusInternalServerError || operator.current.Status.Phase != corev1.ClusterRunning {
		t.Fatalf("failed submission: status=%d phase=%s body=%s", response.Code, operator.current.Status.Phase, response.Body.String())
	}
	store.create = nil
	response = httptest.NewRecorder()
	container.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/clusters/demo", nil))
	if response.Code != http.StatusOK || store.createCalls != 2 || operator.current.Status.Phase != corev1.ClusterTerminating {
		t.Fatalf("retry: status=%d creates=%d phase=%s body=%s", response.Code, store.createCalls, operator.current.Status.Phase, response.Body.String())
	}
}

func TestDeleteClusterRecoversTerminatingWithoutDuplicatingOperations(t *testing.T) {
	for _, name := range []string{"legacy orphan", "reserved orphan", "active delete", "failed delete", "dry run"} {
		t.Run(name, func(t *testing.T) {
			operator := &deleteClusterOperator{current: deleteClusterFixture(corev1.ClusterTerminating)}
			store := &deleteOperationStore{}
			if name != "legacy orphan" {
				operator.current.Annotations = map[string]string{clusterDeleteOperationAnnotation: "reserved-delete"}
			}
			if name == "active delete" || name == "failed delete" {
				op := deletionOperation()
				op.Name = "reserved-delete"
				if name == "failed delete" {
					op.Status.Phase = operations.OperationFailed
				}
				store.items = append(store.items, *op)
			}
			h := newHandler(&generic.ServerRunOptions{}, operator, nil, store, nil, nil, nil)
			container := restful.NewContainer()
			ws := new(restful.WebService)
			ws.Path("/").Produces(restful.MIME_JSON)
			ws.Route(ws.DELETE("/clusters/{name}").To(h.DeleteCluster))
			container.Add(ws)
			path := "/clusters/demo"
			if name == "dry run" {
				path += "?dryRun=true"
			}
			response := httptest.NewRecorder()
			container.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, path, nil))
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if name == "active delete" || name == "dry run" {
				if store.createCalls != 0 || operator.updateCalls != 0 {
					t.Fatal("repeated/dry-run deletion mutated storage")
				}
			} else {
				if store.createCalls != 1 {
					t.Fatalf("creates=%d, want 1", store.createCalls)
				}
				created := store.items[len(store.items)-1]
				if name == "reserved orphan" && created.Name != "reserved-delete" {
					t.Fatal("lost stable operation identity")
				}
				if name == "failed delete" && created.Name == "reserved-delete" {
					t.Fatal("reused terminal failed operation instead of creating a retry")
				}
			}
		})
	}
}

func TestSubmitClusterDeletionRejectsConcurrentTransition(t *testing.T) {
	snapshot := deleteClusterFixture(corev1.ClusterRunning)
	operator := &deleteClusterOperator{current: snapshot.DeepCopy()}
	operator.current.Status.Phase = corev1.ClusterUpgrading
	store := &deleteOperationStore{}
	h := newHandler(nil, operator, nil, store, nil, nil, nil)
	err := h.submitClusterDeletion(context.Background(), snapshot, deletionOperation())
	if !apierrors.IsConflict(err) || operator.updateCalls != 0 || store.createCalls != 0 || operator.current.Status.Phase != corev1.ClusterUpgrading {
		t.Fatalf("concurrent phase overwritten: err=%v updates=%d creates=%d phase=%s", err, operator.updateCalls, store.createCalls, operator.current.Status.Phase)
	}
}

func TestClusterDeleteTimeoutBoundaries(t *testing.T) {
	for _, raw := range []string{"60", "86400", corev1.DefaultOperationTimeoutSecs} {
		if err := validateOperationTimeout(raw); err != nil {
			t.Fatalf("valid timeout %s rejected: %v", raw, err)
		}
	}
}

func TestDeleteClusterValidatesConvertedPlanBeforeMutation(t *testing.T) {
	operator := &deleteClusterOperator{current: deleteClusterFixture(corev1.ClusterRunning), invalidNodeUID: true}
	store := &deleteOperationStore{}
	h := newHandler(&generic.ServerRunOptions{}, operator, nil, store, nil, nil, nil)
	container := restful.NewContainer()
	ws := new(restful.WebService)
	ws.Path("/").Produces(restful.MIME_JSON)
	ws.Route(ws.DELETE("/clusters/{name}").To(h.DeleteCluster))
	container.Add(ws)
	response := httptest.NewRecorder()
	container.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/clusters/demo", nil))
	if response.Code != http.StatusBadRequest || operator.updateCalls != 0 || store.createCalls != 0 || operator.current.Status.Phase != corev1.ClusterRunning {
		t.Fatalf("invalid plan mutated storage: status=%d updates=%d creates=%d phase=%s body=%s", response.Code, operator.updateCalls, store.createCalls, operator.current.Status.Phase, response.Body.String())
	}
}

func TestDeleteClusterForceFailureIsRetryable(t *testing.T) {
	operator := &deleteClusterOperator{current: deleteClusterFixture(corev1.ClusterRunning), deleteErr: errors.New("cluster deletion write failed")}
	// Master directly deletes when no master nodes remain; OCI force deletion
	// always skips host cleanup. Neither path should reserve Terminating first.
	operator.current.Masters = nil
	store := &deleteOperationStore{}
	h := newHandler(&generic.ServerRunOptions{}, operator, nil, store, nil, nil, nil)
	container := restful.NewContainer()
	ws := new(restful.WebService)
	ws.Path("/").Produces(restful.MIME_JSON)
	ws.Route(ws.DELETE("/clusters/{name}").To(h.DeleteCluster))
	container.Add(ws)
	response := httptest.NewRecorder()
	container.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/clusters/demo?force=true", nil))
	if response.Code != http.StatusInternalServerError || operator.current.Status.Phase != corev1.ClusterRunning || operator.updateCalls != 0 {
		t.Fatalf("force failure: status=%d phase=%s updates=%d body=%s", response.Code, operator.current.Status.Phase, operator.updateCalls, response.Body.String())
	}
	operator.deleteErr = nil
	response = httptest.NewRecorder()
	container.ServeHTTP(response, httptest.NewRequest(http.MethodDelete, "/clusters/demo?force=true", nil))
	if response.Code != http.StatusOK || operator.deleteCalls != 2 || store.createCalls != 0 {
		t.Fatalf("force retry: status=%d deletes=%d creates=%d body=%s", response.Code, operator.deleteCalls, store.createCalls, response.Body.String())
	}
}
