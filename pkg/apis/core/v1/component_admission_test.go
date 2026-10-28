package v1

import (
	"context"
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clustermock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	operationbuilder "github.com/kubeclipper/kubeclipper/pkg/operationv2"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

func TestComponentAdmissionReadbackAfterLostUpdateResponse(t *testing.T) {
	for _, committed := range []bool{true, false} {
		t.Run(map[bool]string{true: "committed", false: "not-committed"}[committed], func(t *testing.T) {
			c := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", UID: "cluster-uid"}, Status: corev1.ClusterStatus{Phase: corev1.ClusterRunning}}
			stored := c.DeepCopy()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			plan := &corev1.Operation{ObjectMeta: metav1.ObjectMeta{Name: "op1", Labels: map[string]string{common.LabelOperationAction: corev1.OperationInstallComponents}}, Steps: []corev1.Step{{ID: "step", Nodes: []corev1.StepNode{{ID: "node1"}}}}}
			operator := clustermock.NewMockOperator(gomock.NewController(t))
			operator.EXPECT().GetNodeEx(gomock.Any(), "node1", "").Return(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1", UID: "node-uid"}}, nil)
			operator.EXPECT().UpdateCluster(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, updated *corev1.Cluster) (*corev1.Cluster, error) {
				cancel()
				if committed {
					stored = updated.DeepCopy()
				}
				return nil, errors.New("lost update response")
			})
			operator.EXPECT().GetClusterEx(gomock.Any(), "c1", "").DoAndReturn(func(ctx context.Context, _, _ string) (*corev1.Cluster, error) {
				if ctx.Err() != nil {
					t.Fatal("readback used canceled context")
				}
				return stored.DeepCopy(), nil
			})
			h := &handler{clusterOperator: operator, operationV2Store: &fakeOperationStore{ops: map[string]*operations.Operation{}}}
			got, err := h.admitComponentOperation(ctx, c, plan)
			if committed && (err != nil || got.Annotations[operationbuilder.ComponentOwnerAnnotation] != "op1") {
				t.Fatalf("committed admission was not recovered: %v", err)
			}
			if !committed && err == nil {
				t.Fatal("uncommitted admission reported success")
			}
		})
	}
}

func TestComponentReservationAnnotationsCannotBeOverwritten(t *testing.T) {
	existing := map[string]string{operationbuilder.ComponentOwnerAnnotation: "op1", operationbuilder.ComponentPlanAnnotation: "plan1", "ordinary": "old"}
	requested := map[string]string{operationbuilder.ComponentOwnerAnnotation: "forged", operationbuilder.ComponentPlanAnnotation: "forged", "ordinary": "new"}
	got := componentReservedAnnotations(existing, requested)
	if got[operationbuilder.ComponentOwnerAnnotation] != "op1" || got[operationbuilder.ComponentPlanAnnotation] != "plan1" || got["ordinary"] != "new" {
		t.Fatalf("wrong protected annotations: %#v", got)
	}
	got = componentReservedAnnotations(nil, requested)
	if got[operationbuilder.ComponentOwnerAnnotation] != "" || got[operationbuilder.ComponentPlanAnnotation] != "" {
		t.Fatalf("new reservation was injected: %#v", got)
	}
}
