package v1

import (
	"net/http"

	"k8s.io/apimachinery/pkg/runtime"

	"github.com/kubeclipper/kubeclipper/pkg/authorization/authorizer"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	serverrequest "github.com/kubeclipper/kubeclipper/pkg/server/request"
)

func (h *handler) cloudProviderReadResponse(req *http.Request, provider *corev1.CloudProvider) *corev1.CloudProvider {
	result := provider.DeepCopy()
	if internalClusterRead(req) {
		return result
	}
	// Editors need the stored credentials for the existing provider edit form.
	// Read permission alone must not grant access to SSH keys or kubeconfigs.
	if info, ok := serverrequest.UserFrom(req.Context()); ok && h.authz != nil {
		decision, _, err := h.authz.Authorize(req.Context(), &authorizer.AttributesRecord{
			User: info, Verb: "update", APIGroup: corev1.GroupName, APIVersion: "v1",
			Resource: "cloudproviders", Name: provider.Name, ResourceRequest: true,
		})
		if err == nil && decision == authorizer.DecisionAllow {
			return result
		}
	}
	result.SSH.Password = ""
	result.SSH.PrivateKey = ""
	result.SSH.PrivateKeyPassword = ""
	result.Config = runtime.RawExtension{}
	return result
}
