package v1

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/emicklei/go-restful"
	"github.com/golang/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	mock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestAddNodesRejectsNodeOwnedByAnotherCluster(t *testing.T) {
	for _, role := range []string{"master", "worker"} {
		t.Run(role, func(t *testing.T) {
			operator := mock.NewMockOperator(gomock.NewController(t))
			cluster := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "target", UID: "target-uid", Labels: map[string]string{common.LabelTopologyRegion: "default"}}, Masters: corev1.WorkerNodeList{{ID: "target-master"}}, Status: corev1.ClusterStatus{Phase: corev1.ClusterRunning}}
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "occupied", Labels: map[string]string{common.LabelNodeRole: role, common.LabelClusterName: "other-cluster", common.LabelTopologyRegion: "default"}}, Status: corev1.NodeStatus{Ipv4DefaultIP: "192.0.2.10"}}
			operator.EXPECT().GetClusterEx(gomock.Any(), "target", "0").Return(cluster, nil)
			operator.EXPECT().GetNodeEx(gomock.Any(), "occupied", "0").Return(node, nil)
			h := &handler{clusterOperator: operator, operationV2Store: &backupTestOperations{}}
			ws := new(restful.WebService).Path("/").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)
			ws.Route(ws.PUT("/clusters/{name}/nodes").To(h.AddOrRemoveNodes))
			container := restful.NewContainer()
			container.Add(ws)
			req := httptest.NewRequest(http.MethodPut, "/clusters/target/nodes", strings.NewReader(`{"operation":"add","role":"worker","nodes":[{"id":"occupied"}]}`))
			req.Header.Set("Content-Type", restful.MIME_JSON)
			res := httptest.NewRecorder()
			container.ServeHTTP(res, req)
			if res.Code != http.StatusBadRequest || !strings.Contains(res.Body.String(), "already in use") {
				t.Fatalf("occupied %s must be rejected before cluster or node writes: status=%d body=%s", role, res.Code, res.Body.String())
			}
		})
	}
}
