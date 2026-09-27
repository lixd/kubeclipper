package v1

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/emicklei/go-restful"
	"github.com/golang/mock/gomock"
	platformmock "github.com/kubeclipper/kubeclipper/pkg/models/platform/mock"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/utils/certs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestHasSecureWebTerminalKey(t *testing.T) {
	weakPrivate, weakPublic, err := certs.GetSSHKeyPair(1024)
	if err != nil {
		t.Fatal(err)
	}
	if hasSecureWebTerminalKey(corev1.WebTerminal{PrivateKey: weakPrivate, PublicKey: weakPublic}) {
		t.Fatal("key smaller than 2048 bits must be rejected")
	}

	privateKey, publicKey, err := certs.GetSSHKeyPair(certs.DefaultRSAKeySize)
	if err != nil {
		t.Fatal(err)
	}
	if !hasSecureWebTerminalKey(corev1.WebTerminal{PrivateKey: privateKey, PublicKey: publicKey}) {
		t.Fatal("2048-bit web terminal key must be accepted")
	}
}

func TestPlatformTemplateDoesNotExposeRegistryPasswords(t *testing.T) {
	const (
		host     = "registry.example.invalid"
		password = "r31-canary-password"
		replace  = "r31-replacement-password"
	)

	tests := []struct {
		name, method, body, wantStoredPassword string
		wantStored                             bool
	}{
		{name: "get", method: http.MethodGet, wantStoredPassword: password},
		{
			name:               "put preserves omitted password",
			method:             http.MethodPut,
			body:               `{"insecureRegistry":[{"host":"` + host + `"}]}`,
			wantStoredPassword: password,
			wantStored:         true,
		},
		{
			name:               "put redacts submitted password",
			method:             http.MethodPut,
			body:               `{"insecureRegistry":[{"host":"` + host + `","password":"` + replace + `"}]}`,
			wantStoredPassword: replace,
			wantStored:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			operator := platformmock.NewMockOperator(ctrl)
			stored := &corev1.PlatformSetting{
				ObjectMeta: metav1.ObjectMeta{Name: "system-setting"},
				Template:   corev1.DockerRegistry{InsecureRegistry: []corev1.InsecureRegistry{{Host: host, Password: password}}},
			}
			operator.EXPECT().GetPlatformSetting(gomock.Any()).Return(stored, nil)
			if tt.wantStored {
				operator.EXPECT().UpdatePlatformSetting(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, setting *corev1.PlatformSetting) (*corev1.PlatformSetting, error) {
						if got := setting.Template.InsecureRegistry[0].Password; got != tt.wantStoredPassword {
							t.Fatalf("stored password = %q, want submitted or preserved password", got)
						}
						return setting, nil
					},
				)
			}

			container := restful.NewContainer()
			ws := new(restful.WebService)
			ws.Path("/").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)
			h := newHandler(operator, nil, nil, nil)
			if tt.method == http.MethodGet {
				ws.Route(ws.GET("/template").To(h.DescribeTemplate))
			} else {
				ws.Route(ws.PUT("/template").To(h.UpdateTemplate))
			}
			container.Add(ws)

			request := httptest.NewRequest(tt.method, "/template", strings.NewReader(tt.body))
			if tt.method == http.MethodPut {
				request.Header.Set("Content-Type", "application/json")
			}
			response := httptest.NewRecorder()
			container.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body: %s", response.Code, http.StatusOK, response.Body.String())
			}
			if strings.Contains(response.Body.String(), password) || strings.Contains(response.Body.String(), replace) {
				t.Fatalf("response exposed registry password: %s", response.Body.String())
			}
			var got corev1.DockerRegistry
			if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if len(got.InsecureRegistry) != 1 || got.InsecureRegistry[0].Password != "" {
				t.Fatalf("response registry template = %#v, want one entry with empty password", got.InsecureRegistry)
			}
		})
	}
}
