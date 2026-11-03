package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kubeclipper/kubeclipper/pkg/authorization/authorizer"
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
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	serverrequest "github.com/kubeclipper/kubeclipper/pkg/server/request"
)

type providerResponseAuthorizer struct {
	t        *testing.T
	decision string
}

func (a providerResponseAuthorizer) Authorize(_ context.Context, attr authorizer.Attributes) (authorizer.Decision, string, error) {
	if attr.GetVerb() != "update" || attr.GetResource() != "cloudproviders" || attr.GetAPIGroup() != corev1.GroupName || attr.GetName() != "cluster" || !attr.IsResourceRequest() {
		a.t.Fatal("credential authorization must check provider update permission for the concrete name")
	}
	switch a.decision {
	case "allow":
		return authorizer.DecisionAllow, "", nil
	case "error":
		return authorizer.DecisionAllow, "", fmt.Errorf("authorization unavailable")
	default:
		return authorizer.DecisionDeny, "", nil
	}
}

func TestCloudProviderReadResponsesDoNotExposeCredentials(t *testing.T) {
	for _, test := range []struct {
		name, path, username  string
		raw, internal, cached bool
		authorization         string
	}{
		{name: "detail", path: "/cloudproviders/cluster"},
		{name: "paged list", path: "/cloudproviders"},
		{name: "spoofed raw detail", path: "/cloudproviders/cluster", raw: true},
		{name: "spoofed raw list", path: "/cloudproviders", raw: true},
		{name: "watch", path: "/cloudproviders?watch=true"},
		{name: "spoofed raw watch", path: "/cloudproviders?watch=true", raw: true},
		{name: "cached watch", path: "/cloudproviders?watch=true", cached: true},
		{name: "spoofed cached watch", path: "/cloudproviders?watch=true", raw: true, cached: true},
		{name: "internal cached watch", path: "/cloudproviders?watch=true", username: "system:kc-server", raw: true, internal: true, cached: true},
		{name: "internal detail", path: "/cloudproviders/cluster", username: "system:kc-server", raw: true, internal: true},
		{name: "internal list", path: "/cloudproviders", username: "system:kc-server", raw: true, internal: true},
		{name: "internal watch", path: "/cloudproviders?watch=true", username: "system:kc-server", raw: true, internal: true},
		{name: "editor detail", path: "/cloudproviders/cluster", authorization: "allow", internal: true},
		{name: "editor list", path: "/cloudproviders", authorization: "allow", internal: true},
		{name: "editor raw list", path: "/cloudproviders", raw: true, authorization: "allow", internal: true},
		{name: "editor cached watch", path: "/cloudproviders?watch=true", cached: true, authorization: "allow", internal: true},
		{name: "explicit update denied", path: "/cloudproviders/cluster", authorization: "deny"},
		{name: "authorization error fails closed", path: "/cloudproviders/cluster", authorization: "error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			operator := mock.NewMockOperator(gomock.NewController(t))
			cluster := &corev1.CloudProvider{TypeMeta: metav1.TypeMeta{Kind: "CloudProvider", APIVersion: corev1.SchemeGroupVersion.String()}, ObjectMeta: metav1.ObjectMeta{Name: "cluster", Annotations: map[string]string{"public": "preserved"}}, Type: "kubeadm", SSH: corev1.SSH{User: "root", Port: 22, Password: "ssh-password-secret", PrivateKey: "ssh-private-key-secret", PrivateKeyPassword: "ssh-key-password-secret"}, Config: runtime.RawExtension{Raw: []byte(`{"clusterName":"external","kubeConfig":"kubeconfig-secret"}`)}}
			original, _ := json.Marshal(cluster)
			operator.EXPECT().GetCloudProviderEx(gomock.Any(), "cluster", gomock.Any()).Return(cluster, nil).AnyTimes()
			operator.EXPECT().ListCloudProvidersEx(gomock.Any(), gomock.Any()).Return(&models.PageableResponse{Items: []interface{}{cluster}, TotalCount: 1}, nil).AnyTimes()
			operator.EXPECT().ListCloudProviders(gomock.Any(), gomock.Any()).Return(&corev1.CloudProviderList{Items: []corev1.CloudProvider{*cluster}}, nil).AnyTimes()
			events := make(chan watch.Event, 1)
			var eventObject runtime.Object = cluster
			if test.cached {
				eventObject = &clusterResponseCachedObject{Object: cluster}
			}
			events <- watch.Event{Type: watch.Added, Object: eventObject}
			close(events)
			operator.EXPECT().WatchCloudProviders(gomock.Any(), gomock.Any()).Return(&clusterResponseTestWatch{events}, nil).AnyTimes()
			h := &handler{clusterOperator: operator}
			if test.authorization != "" {
				h.authz = providerResponseAuthorizer{t, test.authorization}
			}
			ws := new(restful.WebService).Path("/").Produces(restful.MIME_JSON)
			ws.Route(ws.GET("/cloudproviders").To(h.ListCloudProviders))
			ws.Route(ws.GET("/cloudproviders/{name}").To(h.DescribeCloudProvider))
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
			for _, secret := range []string{"ssh-password-secret", "ssh-private-key-secret", "ssh-key-password-secret", "kubeconfig-secret"} {
				if strings.Contains(body, secret) != test.internal {
					t.Errorf("%s credential visibility must match trusted internal read=%v", secret, test.internal)
				}
			}
			if !strings.Contains(body, "root") || !strings.Contains(body, "kubeadm") || !strings.Contains(body, "preserved") {
				t.Error("public provider metadata was lost")
			}
			after, _ := json.Marshal(cluster)
			if string(original) != string(after) {
				t.Error("response filtering mutated the stored cluster needed by controllers")
			}
		})
	}
}
