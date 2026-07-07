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
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/emicklei/go-restful"
	"github.com/golang/mock/gomock"
	"k8s.io/component-base/version"

	"github.com/kubeclipper/kubeclipper/pkg/models"
	mockplatform "github.com/kubeclipper/kubeclipper/pkg/models/platform/mock"
	"github.com/kubeclipper/kubeclipper/pkg/query"
	serverconfig "github.com/kubeclipper/kubeclipper/pkg/server/config"

	deliverycore "github.com/kubeclipper/kubeclipper/pkg/apis/core/v1"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

func TestListOfflineResourceFromRegistryInventory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	platformOperator := mockplatform.NewMockOperator(ctrl)
	platformOperator.EXPECT().GetPlatformSetting(gomock.Any()).Return(&v1.PlatformSetting{
		Template: v1.DockerRegistry{
			InsecureRegistry: []v1.InsecureRegistry{{Host: "registry.local:5000"}},
		},
	}, nil)

	h := &handler{
		platformOperator: platformOperator,
		coreOperator:     newFakeConfigCoreOperator(t, handlerPolicy()),
		serverConfig:     &serverconfig.Config{},
		deliveryIndexer:  &fakeConfigCatalogIndexer{catalog: registryHandlerCatalog()},
	}

	req := restful.NewRequest(httptest.NewRequest(http.MethodGet, "/componentmeta?arch=amd64", nil))
	result, err := h.listOfflineResourceFromRegistryInventory(req)
	if err != nil {
		t.Fatalf("listOfflineResourceFromRegistryInventory() error: %+v", err)
	}
	if len(result.Addons) != 3 {
		t.Fatalf("addons length = %d, want 3", len(result.Addons))
	}
	if len(result.Rules) != 1 {
		t.Fatalf("rules length = %d, want 1", len(result.Rules))
	}
	vc := result.Rules[0]["version_control"].(map[string]interface{})
	cri := vc["cri"].([]map[string]interface{})
	if len(cri) != 1 || cri[0]["name"] != "docker" {
		t.Fatalf("cri projection = %+v, want docker only for current KC version %q", cri, version.Get().GitVersion)
	}
}

func TestListOfflineResourceFromRegistryInventoryRequiresDeliverySource(t *testing.T) {
	h := &handler{
		serverConfig: &serverconfig.Config{},
	}
	req := restful.NewRequest(httptest.NewRequest(http.MethodGet, "/componentmeta", nil))
	_, err := h.listOfflineResourceFromRegistryInventory(req)
	if err == nil {
		t.Fatalf("listOfflineResourceFromRegistryInventory() expected error")
	}
}

func TestListOfflineResourceFromRegistryInventoryUsesRegistryDerivedInventory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	platformOperator := mockplatform.NewMockOperator(ctrl)
	platformOperator.EXPECT().GetPlatformSetting(gomock.Any()).Return(&v1.PlatformSetting{
		Template: v1.DockerRegistry{
			InsecureRegistry: []v1.InsecureRegistry{{Host: "registry.local:5000"}},
		},
	}, nil)

	h := &handler{
		platformOperator: platformOperator,
		coreOperator:     newFakeConfigCoreOperator(t, handlerPolicy()),
		serverConfig:     &serverconfig.Config{},
		deliveryIndexer:  &fakeConfigCatalogIndexer{catalog: registryHandlerCatalog()},
	}

	req := restful.NewRequest(httptest.NewRequest(http.MethodGet, "/componentmeta?arch=amd64", nil))
	result, err := h.listOfflineResourceFromRegistryInventory(req)
	if err != nil {
		t.Fatalf("listOfflineResourceFromRegistryInventory() error: %+v", err)
	}
	if len(result.Addons) != 3 {
		t.Fatalf("addons length = %d, want 3", len(result.Addons))
	}
}

func TestListOfflineResourceFromRegistryInventoryRefreshesInventoryWhenRequested(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	platformOperator := mockplatform.NewMockOperator(ctrl)
	platformOperator.EXPECT().GetPlatformSetting(gomock.Any()).Return(&v1.PlatformSetting{
		Template: v1.DockerRegistry{
			InsecureRegistry: []v1.InsecureRegistry{{Host: "registry.local:5000"}},
		},
	}, nil)

	indexer := &fakeConfigCatalogIndexer{catalog: registryHandlerCatalog()}
	h := &handler{
		platformOperator: platformOperator,
		coreOperator:     newFakeConfigCoreOperator(t, handlerPolicy()),
		serverConfig:     &serverconfig.Config{},
		deliveryIndexer:  indexer,
	}

	req := restful.NewRequest(httptest.NewRequest(http.MethodGet, "/componentmeta?arch=amd64&refresh=true", nil))
	if _, err := h.listOfflineResourceFromRegistryInventory(req); err != nil {
		t.Fatalf("listOfflineResourceFromRegistryInventory() error: %+v", err)
	}
	if indexer.refreshCalls != 1 {
		t.Fatalf("refreshCalls = %d, want 1", indexer.refreshCalls)
	}
	if indexer.indexCalls != 0 {
		t.Fatalf("indexCalls = %d, want 0", indexer.indexCalls)
	}
}

func TestGetDeliveryPolicy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	platformOperator := mockplatform.NewMockOperator(ctrl)
	platformOperator.EXPECT().GetPlatformSetting(gomock.Any()).Return(&v1.PlatformSetting{}, nil)

	policy := handlerPolicy()
	coreOperator := newFakeConfigCoreOperator(t, policy)
	container := restful.NewContainer()
	if err := AddToContainer(container, platformOperator, coreOperator, &serverconfig.Config{}); err != nil {
		t.Fatalf("AddToContainer() error: %v", err)
	}

	rawReq := httptest.NewRequest(http.MethodGet, "/api/config.kubeclipper.io/v1/deliverypolicy", nil)
	rawReq.Header.Set("Accept", restful.MIME_JSON)
	rec := httptest.NewRecorder()
	container.ServeHTTP(rec, rawReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	got := &deliveryapis.SupportPolicy{}
	if err := json.Unmarshal(rec.Body.Bytes(), got); err != nil {
		t.Fatalf("Unmarshal() error: %v", err)
	}
	if got.Metadata.Name != policy.Metadata.Name {
		t.Fatalf("policy = %+v, want name %q", got, policy.Metadata.Name)
	}
}

func TestUpdateDeliveryPolicy(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	platformOperator := mockplatform.NewMockOperator(ctrl)
	platformOperator.EXPECT().GetPlatformSetting(gomock.Any()).Return(&v1.PlatformSetting{}, nil)

	coreOperator := newFakeConfigCoreOperator(t, handlerPolicy())
	container := restful.NewContainer()
	if err := AddToContainer(container, platformOperator, coreOperator, &serverconfig.Config{}); err != nil {
		t.Fatalf("AddToContainer() error: %v", err)
	}
	updated := deliveryapis.NewSupportPolicy("custom")
	updated.Spec.Policies = []deliveryapis.KubernetesSupportPolicy{{
		Name:  "k8s-v1.36-stable",
		Match: deliveryapis.PolicyMatch{KubernetesVersion: "v1.36.*"},
		ComponentSlots: []deliveryapis.ComponentSlotRule{{
			Slot:      "cri",
			Selection: deliveryapis.SelectionOneOf,
			Required:  true,
			Default:   deliveryapis.ComponentChoice{Name: "containerd", Version: "2.1.0"},
			Options:   []deliveryapis.ComponentOption{{Name: "containerd", Kind: "cri", AllowedVersions: []string{"2.1.0"}}},
		}},
	}}
	data, err := json.Marshal(updated)
	if err != nil {
		t.Fatalf("Marshal() error: %v", err)
	}

	rawReq := httptest.NewRequest(http.MethodPut, "/api/config.kubeclipper.io/v1/deliverypolicy", bytes.NewReader(data))
	rawReq.Header.Set("Accept", restful.MIME_JSON)
	rawReq.Header.Set("Content-Type", restful.MIME_JSON)
	rec := httptest.NewRecorder()
	container.ServeHTTP(rec, rawReq)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", rec.Code, rec.Body.String())
	}
	stored, err := coreOperator.GetConfigMap(context.Background(), deliverycore.DeliveryPolicyConfigMapNameForTest())
	if err != nil {
		t.Fatalf("GetConfigMap() error: %v", err)
	}
	raw := stored.Data[deliverycore.DeliveryPolicyConfigMapKeyForTest()]
	got := &deliveryapis.SupportPolicy{}
	if err = json.Unmarshal([]byte(raw), got); err != nil {
		t.Fatalf("Unmarshal stored policy: %v", err)
	}
	if got.Metadata.Name != "custom" {
		t.Fatalf("stored policy = %+v, want custom", got)
	}
}

func TestUpdateDeliveryPolicyRejectsLegacyResourceFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	platformOperator := mockplatform.NewMockOperator(ctrl)
	platformOperator.EXPECT().GetPlatformSetting(gomock.Any()).Return(&v1.PlatformSetting{}, nil)

	original := handlerPolicy()
	coreOperator := newFakeConfigCoreOperator(t, original)
	container := restful.NewContainer()
	if err := AddToContainer(container, platformOperator, coreOperator, &serverconfig.Config{}); err != nil {
		t.Fatalf("AddToContainer() error: %v", err)
	}

	body := []byte(`{
		"apiVersion": "delivery.kubeclipper.io/v1alpha1",
		"kind": "SupportPolicy",
		"metadata": {"name": "legacy"},
		"spec": {
			"policies": [{
				"name": "k8s-v1.36-stable",
				"match": {"kubernetesVersion": "v1.36.*"},
				"componentSlots": [{
					"slot": "cri",
					"selection": "oneOf",
					"required": true,
					"default": {"name": "containerd", "version": "2.1.0"},
					"options": [{"name": "containerd", "kind": "cri", "allowedVersions": ["2.1.0"]}],
					"digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111"
				}]
			}]
		}
	}`)
	rawReq := httptest.NewRequest(http.MethodPut, "/api/config.kubeclipper.io/v1/deliverypolicy", bytes.NewReader(body))
	rawReq.Header.Set("Accept", restful.MIME_JSON)
	rawReq.Header.Set("Content-Type", restful.MIME_JSON)
	rec := httptest.NewRecorder()
	container.ServeHTTP(rec, rawReq)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 body=%s", rec.Code, rec.Body.String())
	}
	stored, err := coreOperator.GetConfigMap(context.Background(), deliverycore.DeliveryPolicyConfigMapNameForTest())
	if err != nil {
		t.Fatalf("GetConfigMap() error: %v", err)
	}
	got := &deliveryapis.SupportPolicy{}
	if err = json.Unmarshal([]byte(stored.Data[deliverycore.DeliveryPolicyConfigMapKeyForTest()]), got); err != nil {
		t.Fatalf("Unmarshal stored policy: %v", err)
	}
	if got.Metadata.Name != original.Metadata.Name {
		t.Fatalf("stored policy name = %q, want original %q", got.Metadata.Name, original.Metadata.Name)
	}
}

func handlerCatalog() *deliveryapis.PackageInventory {
	catalog := deliveryapis.NewPackageInventory("default")
	catalog.Spec.Packages = []deliveryapis.PackageEntry{
		handlerPackage("k8s", "k8s", "v1.36.0", "amd64", deliveryapis.ContentProfileK8s),
		handlerPackage("containerd", "cri", "2.1.0", "amd64", deliveryapis.ContentProfileRuntime),
		handlerPackage("docker", "cri", "24.0.0", "amd64", deliveryapis.ContentProfileRuntime),
		handlerPackage("calico", "cni", "v3.30.0", "amd64", deliveryapis.ContentProfileAddon),
	}
	return catalog
}

func handlerPolicy() *deliveryapis.SupportPolicy {
	currentKCVersion := version.Get().GitVersion
	if currentKCVersion == "" {
		currentKCVersion = "v0.0.0"
	}
	policy := deliveryapis.NewSupportPolicy("default")
	policy.Spec.Policies = []deliveryapis.KubernetesSupportPolicy{
		{
			Name: "k8s-v1.36-other",
			Match: deliveryapis.PolicyMatch{
				KubernetesVersion:  "v1.36.*",
				KubeClipperVersion: "v9.9.9",
			},
			ComponentSlots: []deliveryapis.ComponentSlotRule{
				{
					Slot:      "cri",
					Selection: deliveryapis.SelectionOneOf,
					Required:  true,
					Default:   deliveryapis.ComponentChoice{Name: "containerd", Version: "2.1.0"},
					Options:   []deliveryapis.ComponentOption{{Name: "containerd", Kind: "cri", AllowedVersions: []string{"2.1.0"}}},
				},
				{
					Slot:      "cni",
					Selection: deliveryapis.SelectionOneOf,
					Required:  true,
					Default:   deliveryapis.ComponentChoice{Name: "calico", Version: "v3.30.0"},
					Options:   []deliveryapis.ComponentOption{{Name: "calico", Kind: "cni", AllowedVersions: []string{"v3.30.0"}}},
				},
			},
		},
		{
			Name: "k8s-v1.36-current",
			Match: deliveryapis.PolicyMatch{
				KubernetesVersion:  "v1.36.*",
				KubeClipperVersion: currentKCVersion,
			},
			ComponentSlots: []deliveryapis.ComponentSlotRule{
				{
					Slot:      "cri",
					Selection: deliveryapis.SelectionOneOf,
					Required:  true,
					Default:   deliveryapis.ComponentChoice{Name: "docker", Version: "24.0.0"},
					Options:   []deliveryapis.ComponentOption{{Name: "docker", Kind: "cri", AllowedVersions: []string{"24.0.0"}}},
				},
				{
					Slot:      "cni",
					Selection: deliveryapis.SelectionOneOf,
					Required:  true,
					Default:   deliveryapis.ComponentChoice{Name: "calico", Version: "v3.30.0"},
					Options:   []deliveryapis.ComponentOption{{Name: "calico", Kind: "cni", AllowedVersions: []string{"v3.30.0"}}},
				},
			},
		},
	}
	return policy
}

func handlerPackage(name, kind, version, arch, profile string) deliveryapis.PackageEntry {
	return deliveryapis.PackageEntry{
		Name:           name,
		Kind:           kind,
		Version:        version,
		Arch:           arch,
		ContentProfile: profile,
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    "registry.local:5000/kubeclipper/packages/" + kind + "/" + name + ":" + version,
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
		Contents: deliveryapis.ContentsForProfile(profile),
	}
}

type fakeConfigCatalogIndexer struct {
	catalog      *deliveryapis.PackageInventory
	err          error
	indexCalls   int
	refreshCalls int
}

func (f *fakeConfigCatalogIndexer) Index(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.indexCalls++
	return f.catalog, nil
}

func (f *fakeConfigCatalogIndexer) Refresh(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.refreshCalls++
	return f.catalog, nil
}

func registryHandlerCatalog() *deliveryapis.PackageInventory {
	catalog := deliveryapis.NewPackageInventory("registry")
	catalog.Spec.Packages = []deliveryapis.PackageEntry{
		registryHandlerPackage("k8s", "k8s", "v1.36.0", "amd64", deliveryapis.ContentProfileK8s),
		registryHandlerPackage("containerd", "cri", "2.1.0", "amd64", deliveryapis.ContentProfileRuntime),
		registryHandlerPackage("docker", "cri", "24.0.0", "amd64", deliveryapis.ContentProfileRuntime),
		registryHandlerPackage("calico", "cni", "v3.30.0", "amd64", deliveryapis.ContentProfileAddon),
	}
	return catalog
}

func registryHandlerPackage(name, kind, version, arch, profile string) deliveryapis.PackageEntry {
	entry := handlerPackage(name, kind, version, arch, profile)
	entry.Transport = deliveryapis.TransportRef{
		Type:   deliveryapis.TransportOCI,
		Ref:    "registry.local:5000/kubeclipper/packages/" + kind + "/" + name + ":" + version,
		Digest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
	}
	return entry
}

type fakeConfigCoreOperator struct {
	configMap *v1.ConfigMap
}

func newFakeConfigCoreOperator(t *testing.T, policy *deliveryapis.SupportPolicy) *fakeConfigCoreOperator {
	t.Helper()
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatalf("marshal policy: %v", err)
	}
	return &fakeConfigCoreOperator{
		configMap: &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: deliverycore.DeliveryPolicyConfigMapNameForTest()},
			Data: map[string]string{
				deliverycore.DeliveryPolicyConfigMapKeyForTest(): string(data),
			},
		},
	}
}

func (f *fakeConfigCoreOperator) ListConfigMaps(ctx context.Context, q *query.Query) (*v1.ConfigMapList, error) {
	return &v1.ConfigMapList{}, nil
}

func (f *fakeConfigCoreOperator) WatchConfigMaps(ctx context.Context, q *query.Query) (watch.Interface, error) {
	return nil, nil
}

func (f *fakeConfigCoreOperator) GetConfigMap(ctx context.Context, name string) (*v1.ConfigMap, error) {
	return f.GetConfigMapEx(ctx, name, "")
}

func (f *fakeConfigCoreOperator) GetConfigMapEx(ctx context.Context, name string, resourceVersion string) (*v1.ConfigMap, error) {
	if f.configMap == nil || f.configMap.Name != name {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "core.kubeclipper.io", Resource: "configmaps"}, name)
	}
	return f.configMap, nil
}

func (f *fakeConfigCoreOperator) ListConfigMapsEx(ctx context.Context, q *query.Query) (*models.PageableResponse, error) {
	return &models.PageableResponse{}, nil
}

func (f *fakeConfigCoreOperator) CreateConfigMap(ctx context.Context, configmap *v1.ConfigMap) (*v1.ConfigMap, error) {
	f.configMap = configmap
	return configmap, nil
}

func (f *fakeConfigCoreOperator) UpdateConfigMap(ctx context.Context, configmap *v1.ConfigMap) (*v1.ConfigMap, error) {
	f.configMap = configmap
	return configmap, nil
}

func (f *fakeConfigCoreOperator) DeleteConfigMap(ctx context.Context, name string) error {
	f.configMap = nil
	return nil
}
