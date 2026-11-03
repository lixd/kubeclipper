package v1

import (
	"net/http"

	userapi "k8s.io/apiserver/pkg/authentication/user"

	"github.com/kubeclipper/kubeclipper/pkg/client/clientrest"
	operationv2builder "github.com/kubeclipper/kubeclipper/pkg/operationv2"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	serverrequest "github.com/kubeclipper/kubeclipper/pkg/server/request"
	"github.com/kubeclipper/kubeclipper/pkg/utils/strutil"
)

func internalClusterRead(req *http.Request) bool {
	if !clientrest.IsInformerRawQuery(req) {
		return false
	}
	info, ok := serverrequest.UserFrom(req.Context())
	if !ok {
		return false
	}
	if info.GetName() == "system:kc-server" {
		return true
	}
	for _, group := range info.GetGroups() {
		if group == userapi.SystemPrivilegedGroup {
			return true
		}
	}
	return false
}

func clusterReadResponse(cluster *corev1.Cluster) *corev1.Cluster {
	result := cluster.DeepCopy()
	result.ExternalCaKey = ""
	result.KubeConfig = nil
	result.PendingOperations = nil
	delete(result.Annotations, operationv2builder.ComponentPlanAnnotation)
	for i := range result.Status.Registries {
		result.Status.Registries[i] = strutil.HiddenPassword(corev1.Registry{RegistrySpec: result.Status.Registries[i]}).RegistrySpec
	}
	return result
}
