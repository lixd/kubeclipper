package v1

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/emicklei/go-restful"
	"github.com/golang/mock/gomock"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"

	"github.com/kubeclipper/kubeclipper/pkg/client/clientrest"
	"github.com/kubeclipper/kubeclipper/pkg/models"
	mock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	operationv2builder "github.com/kubeclipper/kubeclipper/pkg/operationv2"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	serverrequest "github.com/kubeclipper/kubeclipper/pkg/server/request"
)

type clusterResponseTestWatch struct{ events chan watch.Event }

func (*clusterResponseTestWatch) Stop()                            {}
func (w *clusterResponseTestWatch) ResultChan() <-chan watch.Event { return w.events }

type clusterResponseCachedObject struct{ runtime.Object }

func (c *clusterResponseCachedObject) GetObject() runtime.Object { return c.Object.DeepCopyObject() }
func (c *clusterResponseCachedObject) CacheEncode(_ runtime.Identifier, encode func(runtime.Object, io.Writer) error, out io.Writer) error {
	return encode(c.GetObject(), out)
}

func TestClusterReadResponsesDoNotExposeCredentials(t *testing.T) {
	for _, test := range []struct {
		name, path, username  string
		raw, internal, cached bool
	}{
		{name: "detail", path: "/clusters/cluster"},
		{name: "paged list", path: "/clusters"},
		{name: "spoofed raw detail", path: "/clusters/cluster", raw: true},
		{name: "spoofed raw list", path: "/clusters", raw: true},
		{name: "watch", path: "/clusters?watch=true"},
		{name: "spoofed raw watch", path: "/clusters?watch=true", raw: true},
		{name: "cached watch", path: "/clusters?watch=true", cached: true},
		{name: "spoofed cached watch", path: "/clusters?watch=true", raw: true, cached: true},
		{name: "internal cached watch", path: "/clusters?watch=true", username: "system:kc-server", raw: true, internal: true, cached: true},
		{name: "internal detail", path: "/clusters/cluster", username: "system:kc-server", raw: true, internal: true},
		{name: "internal list", path: "/clusters", username: "system:kc-server", raw: true, internal: true},
		{name: "internal watch", path: "/clusters?watch=true", username: "system:kc-server", raw: true, internal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			operator := mock.NewMockOperator(gomock.NewController(t))
			cluster := &corev1.Cluster{TypeMeta: metav1.TypeMeta{Kind: "Cluster", APIVersion: corev1.SchemeGroupVersion.String()}, ObjectMeta: metav1.ObjectMeta{Name: "cluster", Annotations: map[string]string{operationv2builder.ComponentPlanAnnotation: "frozen-secret", "public": "preserved"}}, ExternalCaCert: "public-ca", ExternalCaKey: "private-ca-secret", KubeConfig: []byte("kubeconfig-secret"), PendingOperations: []corev1.PendingOperation{{OperationID: "pending-secret"}}, Status: corev1.ClusterStatus{Registries: []corev1.RegistrySpec{{Host: "registry.example.invalid", RegistryAuth: &corev1.RegistryAuth{Username: "pull-user", Password: "registry-secret"}}}}}
			original, _ := json.Marshal(cluster)
			operator.EXPECT().GetClusterEx(gomock.Any(), "cluster", gomock.Any()).Return(cluster, nil).AnyTimes()
			operator.EXPECT().ListClusterEx(gomock.Any(), gomock.Any()).Return(&models.PageableResponse{Items: []interface{}{cluster}, TotalCount: 1}, nil).AnyTimes()
			operator.EXPECT().ListClusters(gomock.Any(), gomock.Any()).Return(&corev1.ClusterList{Items: []corev1.Cluster{*cluster}}, nil).AnyTimes()
			events := make(chan watch.Event, 1)
			var eventObject runtime.Object = cluster
			if test.cached {
				eventObject = &clusterResponseCachedObject{Object: cluster}
			}
			events <- watch.Event{Type: watch.Added, Object: eventObject}
			close(events)
			operator.EXPECT().WatchClusters(gomock.Any(), gomock.Any()).Return(&clusterResponseTestWatch{events}, nil).AnyTimes()
			h := &handler{clusterOperator: operator}
			ws := new(restful.WebService).Path("/").Produces(restful.MIME_JSON)
			ws.Route(ws.GET("/clusters").To(h.ListClusters))
			ws.Route(ws.GET("/clusters/{name}").To(h.DescribeCluster))
			container := restful.NewContainer()
			container.Add(ws)
			req := httptest.NewRequest(http.MethodGet, test.path, nil)
			req.Header.Set("Accept", restful.MIME_JSON)
			if test.raw {
				req.Header.Set(clientrest.QueryTypeHeader, clientrest.InformerQuery)
			}
			name := test.username
			if name == "" {
				name = "viewer"
			}
			req = req.WithContext(serverrequest.WithUser(req.Context(), &user.DefaultInfo{Name: name, Groups: []string{user.AllAuthenticated}}))
			res := httptest.NewRecorder()
			container.ServeHTTP(res, req)
			body := res.Body.String()
			if res.Code != 200 {
				t.Fatalf("status=%d body=%s", res.Code, body)
			}
			for _, secret := range []string{"private-ca-secret", "registry-secret", "frozen-secret", "pending-secret", "a3ViZWNvbmZpZy1zZWNyZXQ="} {
				if strings.Contains(body, secret) != test.internal {
					t.Errorf("%s credential visibility must match trusted internal read=%v", secret, test.internal)
				}
			}
			if !strings.Contains(body, "public-ca") || !strings.Contains(body, "registry.example.invalid") || !strings.Contains(body, "preserved") {
				t.Error("public cluster metadata was lost")
			}
			after, _ := json.Marshal(cluster)
			if string(original) != string(after) {
				t.Error("response filtering mutated the stored cluster needed by controllers")
			}
		})
	}
}
