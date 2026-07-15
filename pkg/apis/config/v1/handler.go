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
	"context"
	"io"
	"net/http"
	"strings"

	apimachineryErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/component-base/version"

	deliverycore "github.com/kubeclipper/kubeclipper/pkg/apis/core/v1"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/query"

	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/pkg/utils/certs"

	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"

	serverconfig "github.com/kubeclipper/kubeclipper/pkg/server/config"

	"github.com/emicklei/go-restful"

	"github.com/kubeclipper/kubeclipper/pkg/models/core"
	"github.com/kubeclipper/kubeclipper/pkg/models/platform"
	"github.com/kubeclipper/kubeclipper/pkg/server/restplus"
)

type handler struct {
	platformOperator platform.Operator
	coreOperator     core.Operator
	serverConfig     *serverconfig.Config
	deliveryIndexer  deliverycore.RegistryPackageInventoryIndexer
}

func newHandler(operator platform.Operator, coreOperator core.Operator, config *serverconfig.Config) *handler {
	return &handler{
		platformOperator: operator,
		coreOperator:     coreOperator,
		serverConfig:     config,
		deliveryIndexer:  nil,
	}
}

func (h *handler) ListOfflineResource(request *restful.Request, response *restful.Response) {
	result, err := h.listOfflineResourceFromRegistryInventory(request)
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	_ = response.WriteHeaderAndEntity(http.StatusOK, result)
}

func (h *handler) listOfflineResourceFromRegistryInventory(request *restful.Request) (*kc.ComponentMeta, error) {
	source, err := deliverycore.ResolveDeliverySourceForConfig(request.Request.Context(), h.platformOperator, h.coreOperator, h.deliveryIndexer)
	if err != nil {
		return nil, err
	}
	if source.InventoryStore == nil || source.PolicyStore == nil {
		return nil, &deliveryapis.ResolverError{Code: deliveryapis.ErrArtifactNotPublished, Message: "delivery source requires package inventory and support policy"}
	}
	inventory, err := loadInventory(request, source)
	if err != nil {
		return nil, err
	}
	policy, err := source.PolicyStore.Get(request.Request.Context())
	if err != nil {
		return nil, err
	}
	projection, err := deliveryapis.ProjectComponentMeta(inventory, policy, deliveryapis.ProjectOptions{
		Archs:              archQuery(request.QueryParameter("arch")),
		KubeClipperVersion: version.Get().GitVersion,
	})
	if err != nil {
		return nil, err
	}
	return &kc.ComponentMeta{Rules: projection.Rules, Addons: projection.Addons, Unavailable: projection.Unavailable}, nil
}

func loadInventory(request *restful.Request, source deliverycore.DeliverySource) (*deliveryapis.PackageInventory, error) {
	if source.InventoryStore == nil {
		return nil, &deliveryapis.ResolverError{Code: deliveryapis.ErrArtifactNotPublished, Message: "delivery source requires package inventory"}
	}
	if request != nil && query.GetBoolValueWithDefault(request, "refresh", false) {
		if refresher, ok := source.InventoryStore.(interface {
			Refresh(ctx context.Context) (*deliveryapis.PackageInventory, error)
		}); ok {
			return refresher.Refresh(request.Request.Context())
		}
	}
	return source.InventoryStore.Get(request.Request.Context())
}

func (h *handler) GetDeliveryPolicy(request *restful.Request, response *restful.Response) {
	source, err := deliverycore.ResolveDeliverySourceForConfig(request.Request.Context(), h.platformOperator, h.coreOperator, h.deliveryIndexer)
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	if source.PolicyStore == nil {
		restplus.HandleInternalError(response, request, &deliveryapis.ResolverError{Code: deliveryapis.ErrArtifactNotPublished, Message: "delivery source requires support policy"})
		return
	}
	policy, err := source.PolicyStore.Get(request.Request.Context())
	if err != nil {
		if apimachineryErrors.IsNotFound(err) {
			restplus.HandleNotFound(response, request, err)
			return
		}
		restplus.HandleInternalError(response, request, err)
		return
	}
	_ = response.WriteHeaderAndEntity(http.StatusOK, policy)
}

func (h *handler) UpdateDeliveryPolicy(request *restful.Request, response *restful.Response) {
	source, err := deliverycore.ResolveDeliverySourceForConfig(request.Request.Context(), h.platformOperator, h.coreOperator, h.deliveryIndexer)
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	if source.PolicyStore == nil {
		restplus.HandleInternalError(response, request, &deliveryapis.ResolverError{Code: deliveryapis.ErrArtifactNotPublished, Message: "delivery source requires support policy"})
		return
	}
	data, err := io.ReadAll(request.Request.Body)
	if err != nil {
		restplus.HandleBadRequest(response, request, err)
		return
	}
	policy := &deliveryapis.SupportPolicy{}
	if err = deliveryapis.DecodeSupportPolicy(data, policy); err != nil {
		restplus.HandleBadRequest(response, request, err)
		return
	}
	if policy.Metadata.Name == "" {
		policy.Metadata.Name = "default"
	}
	if err = source.PolicyStore.Update(request.Request.Context(), func(current *deliveryapis.SupportPolicy) error {
		*current = *policy
		if current.Metadata.Name == "" {
			current.Metadata.Name = "default"
		}
		return nil
	}); err != nil {
		restplus.HandleBadRequest(response, request, err)
		return
	}
	updated, err := source.PolicyStore.Get(request.Request.Context())
	if err != nil {
		restplus.HandleInternalError(response, request, err)
		return
	}
	_ = response.WriteHeaderAndEntity(http.StatusOK, updated)
}

func archQuery(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	archs := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			archs = append(archs, part)
		}
	}
	return archs
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
	t.PrivateKey, t.PublicKey, err = certs.GetSSHKeyPair(2048)
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
	priv, pub, err := certs.GetSSHKeyPair(2048)
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
