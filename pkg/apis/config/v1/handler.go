/*
 *
 *  * Copyright 2021 KubeClipper Authors.
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
	"fmt"
	"net/http"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/component-base/version"

	deliverycore "github.com/kubeclipper/kubeclipper/pkg/apis/core/v1"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/models/core"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/pkg/utils/certs"

	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"

	serverconfig "github.com/kubeclipper/kubeclipper/pkg/server/config"

	"github.com/emicklei/go-restful"

	"github.com/kubeclipper/kubeclipper/pkg/models/platform"
	"github.com/kubeclipper/kubeclipper/pkg/platformstatus"
	"github.com/kubeclipper/kubeclipper/pkg/server/restplus"
)

type handler struct {
	platformOperator platform.Operator
	coreOperator     core.Operator
	serverConfig     *serverconfig.Config
	statusProvider   platformstatus.Provider
	deliveryIndexer  deliverycore.RegistryPackageInventoryIndexer
}

func newHandler(operator platform.Operator, coreOperator core.Operator, config *serverconfig.Config, statusProvider platformstatus.Provider) *handler {
	return &handler{
		platformOperator: operator,
		coreOperator:     coreOperator,
		serverConfig:     config,
		statusProvider:   statusProvider,
	}
}

func (h *handler) GetPlatformStatus(request *restful.Request, response *restful.Response) {
	if h.statusProvider == nil {
		restplus.HandleInternalError(response, request, fmt.Errorf("platform status provider is not configured"))
		return
	}
	if err := response.WriteHeaderAndEntity(
		http.StatusOK,
		h.statusProvider.PlatformStatus(request.Request.Context()),
	); err != nil {
		restplus.HandleInternalError(response, request, err)
	}
}

func (h *handler) ListOfflineResource(request *restful.Request, response *restful.Response) {
	source, err := deliverycore.ResolveDeliverySourceForConfig(
		request.Request.Context(), h.platformOperator, h.coreOperator, h.deliveryIndexer,
	)
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	if source.InventoryStore == nil || source.PolicyStore == nil {
		restplus.HandleInternalError(response, request, fmt.Errorf("OCI package registry and delivery policy are required"))
		return
	}
	inventory, err := source.InventoryStore.Get(request.Request.Context())
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	policy, err := source.PolicyStore.Get(request.Request.Context())
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	projection, err := deliveryapis.ProjectComponentMeta(inventory, policy, deliveryapis.ProjectOptions{
		Archs:              componentMetaArchs(request.QueryParameter("arch")),
		KubeClipperVersion: version.Get().GitVersion,
	})
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	result := kc.ComponentMeta{Rules: projection.Rules, Addons: projection.Addons}
	_ = response.WriteHeaderAndEntity(http.StatusOK, result)
}

func componentMetaArchs(raw string) []string {
	if raw == "" {
		return nil
	}
	archs := make([]string, 0, 1)
	for _, arch := range strings.Split(raw, ",") {
		if arch = strings.TrimSpace(arch); arch != "" {
			archs = append(archs, arch)
		}
	}
	return archs
}

func (h *handler) GetDeliveryPolicy(request *restful.Request, response *restful.Response) {
	store, err := h.deliveryPolicyStore(request)
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	policy, err := store.Get(request.Request.Context())
	if apierrors.IsNotFound(err) {
		restplus.HandleNotFound(response, request, err)
		return
	}
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	_ = response.WriteHeaderAndEntity(http.StatusOK, policy)
}

func (h *handler) UpdateDeliveryPolicy(request *restful.Request, response *restful.Response) {
	store, err := h.deliveryPolicyStore(request)
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	policy := &deliveryapis.SupportPolicy{}
	if err = request.ReadEntity(policy); err != nil {
		restplus.HandleBadRequest(response, request, err)
		return
	}
	if policy.Metadata.Name == "" {
		policy.Metadata.Name = "default"
	}
	if err = store.Update(request.Request.Context(), func(current *deliveryapis.SupportPolicy) error {
		*current = *policy
		return current.Validate()
	}); err != nil {
		restplus.HandleBadRequest(response, request, err)
		return
	}
	updated, err := store.Get(request.Request.Context())
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	_ = response.WriteHeaderAndEntity(http.StatusOK, updated)
}

func (h *handler) deliveryPolicyStore(request *restful.Request) (deliveryapis.PolicyStore, error) {
	source, err := deliverycore.ResolveDeliverySourceForConfig(
		request.Request.Context(), h.platformOperator, h.coreOperator, h.deliveryIndexer,
	)
	if err != nil {
		return nil, err
	}
	if source.PolicyStore == nil {
		return nil, fmt.Errorf("delivery policy storage is not configured")
	}
	return source.PolicyStore, nil
}

// Deprecated: use core/v1/handler.DescribeTemplate instead
func (h *handler) DescribeTemplate(req *restful.Request, resp *restful.Response) {
	setting, err := h.platformOperator.GetPlatformSetting(req.Request.Context())
	if err != nil {
		restplus.HandleInternalError(resp, req, err)
		return
	}
	_ = resp.WriteHeaderAndEntity(http.StatusOK, setting.Template)
}

// Deprecated: use core/v1/handler.UpdateTemplate instead
func (h *handler) UpdateTemplate(req *restful.Request, resp *restful.Response) {
	c := &v1.DockerRegistry{}
	var (
		err     error
		setting *v1.PlatformSetting
	)
	if err = req.ReadEntity(c); err != nil {
		restplus.HandleBadRequest(resp, req, err)
		return
	}
	for index := range c.InsecureRegistry {
		if c.InsecureRegistry[index].CreateAt.IsZero() {
			c.InsecureRegistry[index].CreateAt = metav1.Now()
		}
	}
	setting, err = h.platformOperator.GetPlatformSetting(req.Request.Context())
	if err != nil {
		restplus.HandleInternalError(resp, req, err)
		return
	}
	if setting == nil || setting.Name == "" {
		setting = generatePlatformSetting()
		setting.Template = *c
		_, err = h.platformOperator.CreatePlatformSetting(req.Request.Context(), setting)
	} else {
		setting.Template = *c
		_, err = h.platformOperator.UpdatePlatformSetting(req.Request.Context(), setting)
	}
	if err != nil {
		restplus.HandleInternalError(resp, req, err)
		return
	}
	_ = resp.WriteHeaderAndEntity(http.StatusOK, c)
}

func (h *handler) GetSSHRSAKey(req *restful.Request, resp *restful.Response) {
	var (
		err     error
		setting *v1.PlatformSetting
	)

	setting, err = h.platformOperator.GetPlatformSetting(req.Request.Context())
	if err != nil {
		restplus.HandleInternalError(resp, req, err)
		return
	}
	if setting == nil || setting.Name == "" {
		setting = generatePlatformSetting()
		setting.Terminal, err = generateWebTerminal()
		if err != nil {
			restplus.HandleInternalError(resp, req, err)
			return
		}
		_, err = h.platformOperator.CreatePlatformSetting(req.Request.Context(), setting)
	} else {
		if setting.Terminal.PrivateKey == "" {
			setting.Terminal, err = generateWebTerminal()
			if err != nil {
				restplus.HandleInternalError(resp, req, err)
				return
			}
			_, err = h.platformOperator.UpdatePlatformSetting(req.Request.Context(), setting)
		}
	}
	if err != nil {
		restplus.HandleInternalError(resp, req, err)
		return
	}
	setting.Terminal.PrivateKey = ""
	_ = resp.WriteHeaderAndEntity(http.StatusOK, setting.Terminal)
}

func (h *handler) CreateSSHRSAKey(req *restful.Request, resp *restful.Response) {
	t := v1.WebTerminal{}
	setting, err := h.platformOperator.GetPlatformSetting(req.Request.Context())
	if err != nil {
		restplus.HandleInternalError(resp, req, err)
		return
	}
	t.PrivateKey, t.PublicKey, err = certs.GetSSHKeyPair(certs.DefaultRSAKeySize)
	if err != nil {
		restplus.HandleInternalError(resp, req, err)
		return
	}
	setting.Terminal = t
	_, err = h.platformOperator.UpdatePlatformSetting(req.Request.Context(), setting)
	if err != nil {
		restplus.HandleInternalError(resp, req, err)
		return
	}
	resp.WriteHeader(http.StatusOK)
}

func generateWebTerminal() (v1.WebTerminal, error) {
	priv, pub, err := certs.GetSSHKeyPair(certs.DefaultRSAKeySize)
	if err != nil {
		return v1.WebTerminal{}, err
	}
	return v1.WebTerminal{
		PrivateKey: priv,
		PublicKey:  pub,
	}, nil

}

func generatePlatformSetting() *v1.PlatformSetting {
	return &v1.PlatformSetting{
		TypeMeta: metav1.TypeMeta{
			Kind:       "PlatformSetting",
			APIVersion: "core.kubeclipper.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "system-",
		},
	}
}
