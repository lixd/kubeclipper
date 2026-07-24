/*
 *
 *  * Copyright 2024 KubeClipper Authors.
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
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubeclipper/kubeclipper/pkg/component"
	"github.com/kubeclipper/kubeclipper/pkg/constatns"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/models"
	mockplatform "github.com/kubeclipper/kubeclipper/pkg/models/platform/mock"
	"github.com/kubeclipper/kubeclipper/pkg/query"
	serverconfig "github.com/kubeclipper/kubeclipper/pkg/server/config"

	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

func TestWithResolvedArtifactPlan(t *testing.T) {
	h := &handler{
		serverConfig:    &serverconfig.Config{},
		coreOperator:    newFakeDeliveryCoreOperator(t, deliveryPolicy()),
		deliveryIndexer: fakeCatalogIndexer{catalog: registryResolverCatalog()},
	}
	extra := &component.ExtraMetadata{
		Offline: true,
		Masters: component.NodeList{{ID: "master-1", Arch: "amd64"}},
		CRI:     "containerd",
		CNI:     "calico",
	}
	cluster := &v1.Cluster{
		KubernetesVersion:     "v1.36.0",
		ResolvedImageRegistry: "images.example.com/kubernetes",
		ContainerRuntime:      v1.ContainerRuntime{Type: "containerd", Version: "2.1.0"},
		CNI:                   v1.CNI{Type: "calico", Version: "v3.30.0"},
	}

	ctx, err := h.withResolvedArtifactPlan(context.Background(), extra, cluster, v1.ActionInstall)
	if err != nil {
		t.Fatalf("withResolvedArtifactPlan() error: %+v", err)
	}
	plan, ok := component.GetResolvedArtifactPlan(ctx).(*deliveryapis.ResolvedArtifactPlan)
	if !ok {
		t.Fatalf("resolved plan not found in context")
	}
	if plan.KubernetesVersion != "v1.36.0" || plan.Arch != "amd64" {
		t.Fatalf("plan target = %+v", plan)
	}
	if len(plan.Components) != 3 {
		t.Fatalf("components length = %d, want 3", len(plan.Components))
	}
}

func TestWithResolvedArtifactPlanForOnlineCluster(t *testing.T) {
	h := &handler{
		serverConfig:    &serverconfig.Config{},
		coreOperator:    newFakeDeliveryCoreOperator(t, deliveryPolicy()),
		deliveryIndexer: fakeCatalogIndexer{catalog: registryResolverCatalog()},
	}
	extra := &component.ExtraMetadata{
		Offline: false,
		Masters: component.NodeList{{ID: "master-1", Arch: "amd64"}},
		CRI:     "containerd",
		CNI:     "calico",
	}
	cluster := &v1.Cluster{
		KubernetesVersion:     "v1.36.0",
		ResolvedImageRegistry: "images.example.com/kubernetes",
		ContainerRuntime:      v1.ContainerRuntime{Type: "containerd", Version: "2.1.0"},
		CNI:                   v1.CNI{Type: "calico", Version: "v3.30.0"},
	}

	ctx, err := h.withResolvedArtifactPlan(context.Background(), extra, cluster, v1.ActionInstall)
	if err != nil {
		t.Fatalf("withResolvedArtifactPlan() error: %+v", err)
	}
	if _, ok := component.GetResolvedArtifactPlan(ctx).(*deliveryapis.ResolvedArtifactPlan); !ok {
		t.Fatal("online cluster install must still resolve OCI package artifacts")
	}
}

func TestWithResolvedArtifactPlanRequiresDeliverySource(t *testing.T) {
	h := &handler{serverConfig: &serverconfig.Config{}}
	extra := &component.ExtraMetadata{Offline: true, Masters: component.NodeList{{ID: "master-1", Arch: "amd64"}}}
	_, err := h.withResolvedArtifactPlan(context.Background(), extra, &v1.Cluster{KubernetesVersion: "v1.36.0"}, v1.ActionInstall)
	if err == nil {
		t.Fatalf("withResolvedArtifactPlan() expected error")
	}
}

func TestConfigMapPolicyStoreRejectsResourceFields(t *testing.T) {
	operator := newFakeDeliveryCoreOperator(t, deliveryPolicy())
	operator.configMap.Data[deliveryapis.DeliveryPolicyConfigMapKey] = `{
  "apiVersion": "delivery.kubeclipper.io/v1alpha1",
  "kind": "SupportPolicy",
  "metadata": {"name": "default"},
  "spec": {
    "policies": [{
      "name": "k8s-v1.36",
      "match": {"kubernetesVersion": "v1.36.*"},
      "componentSlots": [{
        "slot": "cri",
        "selection": "oneOf",
        "required": true,
        "default": {"name": "containerd", "version": "2.1.0", "ref": "registry.local/pkg:tag"},
        "options": [{"kind": "cri", "name": "containerd", "allowedVersions": ["2.1.0"]}]
      }]
    }]
  }
}`
	store := &configMapPolicyStore{coreOperator: operator}
	_, err := store.Get(context.Background())
	if err == nil {
		t.Fatalf("Get() error = nil, want unknown field error")
	}
	if !strings.Contains(err.Error(), `unknown field "ref"`) {
		t.Fatalf("Get() error = %v", err)
	}
}

func TestConfigMapPolicyStoreUpdatePreservesExistingConfigMapMetadata(t *testing.T) {
	operator := newFakeDeliveryCoreOperator(t, deliveryPolicy())
	operator.configMap.ResourceVersion = "12345"
	operator.configMap.Data["unrelated"] = "keep"
	store := &configMapPolicyStore{coreOperator: operator}

	err := store.Update(context.Background(), func(policy *deliveryapis.SupportPolicy) error {
		policy.Spec.Policies[0].Name = "updated"
		return nil
	})
	if err != nil {
		t.Fatalf("Update() error: %+v", err)
	}
	stored := operator.configMaps[deliveryapis.DeliveryPolicyConfigMapName]
	if stored.ResourceVersion != "12345" {
		t.Fatalf("ResourceVersion = %q, want 12345", stored.ResourceVersion)
	}
	if stored.Data["unrelated"] != "keep" {
		t.Fatalf("unrelated data = %q, want keep", stored.Data["unrelated"])
	}
	if !strings.Contains(stored.Data[deliveryapis.DeliveryPolicyConfigMapKey], `"name": "updated"`) {
		t.Fatalf("policy data was not updated: %s", stored.Data[deliveryapis.DeliveryPolicyConfigMapKey])
	}
}

func TestWithResolvedArtifactPlanRejectsMixedArch(t *testing.T) {
	h := &handler{serverConfig: &serverconfig.Config{}}
	extra := &component.ExtraMetadata{Offline: true, Masters: component.NodeList{{ID: "master-1", Arch: "amd64"}}, Workers: component.NodeList{{ID: "worker-1", Arch: "arm64"}}}
	_, err := h.withResolvedArtifactPlan(context.Background(), extra, &v1.Cluster{KubernetesVersion: "v1.36.0"}, v1.ActionInstall)
	if err == nil {
		t.Fatalf("withResolvedArtifactPlan() expected mixed arch error")
	}
	if !strings.Contains(err.Error(), "single target architecture") {
		t.Fatalf("withResolvedArtifactPlan() error = %v", err)
	}
}

func TestWithResolvedArtifactPlanUsesRegistryDerivedCatalog(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	platformOperator := mockplatform.NewMockOperator(ctrl)
	platformOperator.EXPECT().GetPlatformSetting(gomock.Any()).Return(&v1.PlatformSetting{}, nil).AnyTimes()

	h := &handler{
		serverConfig:     &serverconfig.Config{},
		coreOperator:     newFakeDeliveryCoreOperator(t, deliveryPolicy()),
		platformOperator: platformOperator,
		deliveryIndexer:  fakeCatalogIndexer{catalog: registryResolverCatalog()},
	}
	extra := &component.ExtraMetadata{
		Offline: true,
		Masters: component.NodeList{{ID: "master-1", Arch: "amd64"}},
	}
	cluster := &v1.Cluster{
		KubernetesVersion:     "v1.36.0",
		ResolvedImageRegistry: "images.example.com/kubernetes",
		ContainerRuntime:      v1.ContainerRuntime{Type: "containerd", Version: "2.1.0"},
		CNI:                   v1.CNI{Type: "calico", Version: "v3.30.0"},
	}

	ctx, err := h.withResolvedArtifactPlan(context.Background(), extra, cluster, v1.ActionInstall)
	if err != nil {
		t.Fatalf("withResolvedArtifactPlan() error: %+v", err)
	}
	plan, ok := component.GetResolvedArtifactPlan(ctx).(*deliveryapis.ResolvedArtifactPlan)
	if !ok {
		t.Fatalf("resolved plan not found in context")
	}
	for _, resolved := range plan.Components {
		if resolved.Transport.Type != deliveryapis.TransportOCI {
			t.Fatalf("component transport = %s, want oci", resolved.Transport.Type)
		}
	}
}

func TestResolveDeliverySourceForConfigUsesDeployConfigPackageRegistry(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	platformOperator := mockplatform.NewMockOperator(ctrl)
	platformOperator.EXPECT().GetPlatformSetting(gomock.Any()).Return(&v1.PlatformSetting{}, nil).AnyTimes()

	coreOperator := newFakeDeliveryCoreOperator(t, deliveryPolicy())
	coreOperator.configMaps[constatns.DeployConfigConfigMapName] = &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constatns.DeployConfigConfigMapName},
		Data: map[string]string{
			constatns.DeployConfigConfigMapKey: "packageRegistry: registry.local:5000\n",
		},
	}
	indexer := &recordingCatalogIndexer{catalog: registryResolverCatalog()}

	source, err := ResolveDeliverySourceForConfig(context.Background(), platformOperator, coreOperator, indexer)
	if err != nil {
		t.Fatalf("ResolveDeliverySourceForConfig() error: %+v", err)
	}
	if source.Registry != "registry.local:5000" {
		t.Fatalf("source.Registry = %q, want registry.local:5000", source.Registry)
	}
	if _, err = source.InventoryStore.Get(context.Background()); err != nil {
		t.Fatalf("InventoryStore.Get() error: %+v", err)
	}
	if indexer.registry != "registry.local:5000" {
		t.Fatalf("indexer registry = %q, want registry.local:5000", indexer.registry)
	}
}

func TestResolveDeliverySourceDoesNotUseClusterImageRegistry(t *testing.T) {
	coreOperator := newFakeDeliveryCoreOperator(t, deliveryPolicy())
	indexer := &recordingCatalogIndexer{catalog: registryResolverCatalog()}
	cluster := &v1.Cluster{
		ImageRegistry:         "kubernetes-images",
		ResolvedImageRegistry: "images.example.com/kubernetes",
	}

	source, err := resolveDeliverySource(context.Background(), nil, coreOperator, cluster, indexer)
	if err != nil {
		t.Fatalf("resolveDeliverySource() error: %+v", err)
	}
	if source.registry != "packages.example.com" {
		t.Fatalf("package registry = %q, want packages.example.com", source.registry)
	}
	if _, err = source.inventoryStore.Get(context.Background()); err != nil {
		t.Fatalf("InventoryStore.Get() error: %+v", err)
	}
	if indexer.registry != "packages.example.com" {
		t.Fatalf("indexer registry = %q, want packages.example.com", indexer.registry)
	}
}

func TestWithResolvedArtifactPlanReturnsRegistryErrorWhenIndexerFails(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	platformOperator := mockplatform.NewMockOperator(ctrl)
	platformOperator.EXPECT().GetPlatformSetting(gomock.Any()).Return(&v1.PlatformSetting{}, nil).AnyTimes()

	h := &handler{
		serverConfig:     &serverconfig.Config{},
		coreOperator:     newFakeDeliveryCoreOperator(t, deliveryPolicy()),
		platformOperator: platformOperator,
		deliveryIndexer:  fakeCatalogIndexer{err: os.ErrPermission},
	}
	extra := &component.ExtraMetadata{
		Offline: true,
		Masters: component.NodeList{{ID: "master-1", Arch: "amd64"}},
	}
	cluster := &v1.Cluster{
		KubernetesVersion:     "v1.36.0",
		ResolvedImageRegistry: "images.example.com/kubernetes",
		ContainerRuntime:      v1.ContainerRuntime{Type: "containerd", Version: "2.1.0"},
		CNI:                   v1.CNI{Type: "calico", Version: "v3.30.0"},
	}

	_, err := h.withResolvedArtifactPlan(context.Background(), extra, cluster, v1.ActionInstall)
	if err == nil {
		t.Fatalf("withResolvedArtifactPlan() expected registry error")
	}
	if !os.IsPermission(err) {
		t.Fatalf("error = %v, want permission error", err)
	}
}

func TestWithResolvedArtifactPlanUsesUpgradeTargetVersion(t *testing.T) {
	policy := deliveryapis.NewSupportPolicy("default")
	policy.Spec.Policies = []deliveryapis.KubernetesSupportPolicy{
		{
			Name:  "k8s-v1.34-stable",
			Match: deliveryapis.PolicyMatch{KubernetesVersion: "v1.34.*"},
			ComponentSlots: []deliveryapis.ComponentSlotRule{
				{
					Slot:      "cri",
					Selection: deliveryapis.SelectionOneOf,
					Required:  true,
					Default:   deliveryapis.ComponentChoice{Name: "docker", Version: "20.10.24"},
					Options:   []deliveryapis.ComponentOption{{Name: "docker", Kind: "cri", AllowedVersions: []string{"20.10.24"}}},
				},
			},
		},
		{
			Name:  "k8s-v1.36-stable",
			Match: deliveryapis.PolicyMatch{KubernetesVersion: "v1.36.*"},
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
	}

	h := &handler{
		serverConfig:    &serverconfig.Config{},
		coreOperator:    newFakeDeliveryCoreOperator(t, policy),
		deliveryIndexer: fakeCatalogIndexer{catalog: registryResolverCatalog()},
	}
	extra := &component.ExtraMetadata{
		Offline:     true,
		KubeVersion: "v1.36.0",
		Masters:     component.NodeList{{ID: "master-1", Arch: "amd64"}},
	}
	cluster := &v1.Cluster{
		KubernetesVersion:     "v1.34.0",
		ResolvedImageRegistry: "images.example.com/kubernetes",
		ContainerRuntime:      v1.ContainerRuntime{Type: "containerd", Version: "2.1.0"},
		CNI:                   v1.CNI{Type: "calico", Version: "v3.30.0"},
	}

	ctx, err := h.withResolvedArtifactPlan(context.Background(), extra, cluster, v1.ActionUpgrade)
	if err != nil {
		t.Fatalf("withResolvedArtifactPlan() error: %+v", err)
	}
	plan, ok := component.GetResolvedArtifactPlan(ctx).(*deliveryapis.ResolvedArtifactPlan)
	if !ok {
		t.Fatalf("resolved plan not found in context")
	}
	if plan.KubernetesVersion != "v1.36.0" {
		t.Fatalf("plan kubernetes version = %s, want v1.36.0", plan.KubernetesVersion)
	}
	if len(plan.Components) != 3 {
		t.Fatalf("components length = %d, want 3", len(plan.Components))
	}
}

func TestIndexedInventoryStoreUsesRequestContext(t *testing.T) {
	type contextKey string
	key := contextKey("request-id")
	indexer := &contextRecordingIndexer{catalog: registryResolverCatalog()}
	store := indexedInventoryStore{
		registry: "registry.local:5000",
		indexer:  indexer,
	}

	getCtx := context.WithValue(context.Background(), key, "get")
	if _, err := store.Get(getCtx); err != nil {
		t.Fatalf("Get() error: %+v", err)
	}
	if got := indexer.indexCtx.Value(key); got != "get" {
		t.Fatalf("Index context value = %v, want get", got)
	}

	refreshCtx := context.WithValue(context.Background(), key, "refresh")
	if _, err := store.Refresh(refreshCtx); err != nil {
		t.Fatalf("Refresh() error: %+v", err)
	}
	if got := indexer.refreshCtx.Value(key); got != "refresh" {
		t.Fatalf("Refresh context value = %v, want refresh", got)
	}
}

func TestWithResolvedArtifactPlanIndexesPolicyRepositoriesWithoutCatalog(t *testing.T) {
	indexer := &recordingRepositoryIndexer{catalog: registryResolverCatalog()}
	h := &handler{
		serverConfig:    &serverconfig.Config{},
		coreOperator:    newFakeDeliveryCoreOperator(t, deliveryPolicy()),
		deliveryIndexer: indexer,
	}
	extra := &component.ExtraMetadata{
		Offline: true,
		Masters: component.NodeList{{ID: "master-1", Arch: "amd64"}},
		CRI:     "containerd",
		CNI:     "calico",
	}
	cluster := &v1.Cluster{
		KubernetesVersion: "v1.36.0",
		ContainerRuntime:  v1.ContainerRuntime{Type: "containerd", Version: "2.1.0"},
		CNI:               v1.CNI{Type: "calico", Version: "v3.30.0"},
	}

	if _, err := h.withResolvedArtifactPlan(context.Background(), extra, cluster, v1.ActionInstall); err != nil {
		t.Fatalf("withResolvedArtifactPlan() error: %+v", err)
	}
	if indexer.indexCalls != 0 {
		t.Fatalf("Index() calls = %d, want 0", indexer.indexCalls)
	}
	want := []string{
		"kubeclipper/charts/tigera-operator",
		"kubeclipper/packages/cri/containerd",
		"kubeclipper/packages/k8s/k8s",
	}
	if strings.Join(indexer.repositories, ",") != strings.Join(want, ",") {
		t.Fatalf("repositories = %v, want %v", indexer.repositories, want)
	}
	source, err := resolveDeliverySource(context.Background(), nil, h.coreOperator, cluster, indexer)
	if err != nil {
		t.Fatalf("resolveDeliverySource() error: %+v", err)
	}
	store, ok := source.inventoryStore.(indexedInventoryStore)
	if !ok {
		t.Fatalf("inventory store type = %T, want indexedInventoryStore", source.inventoryStore)
	}
	if _, err = store.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh() error: %+v", err)
	}
	if indexer.indexCalls != 0 {
		t.Fatalf("Index() calls after Refresh = %d, want 0", indexer.indexCalls)
	}
}

func deliveryPolicy() *deliveryapis.SupportPolicy {
	policy := deliveryapis.NewSupportPolicy("default")
	policy.Spec.Policies = []deliveryapis.KubernetesSupportPolicy{{
		Name:  "k8s-v1.36-stable",
		Match: deliveryapis.PolicyMatch{KubernetesVersion: "v1.36.*"},
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
	}}
	return policy
}

type fakeCatalogIndexer struct {
	catalog *deliveryapis.PackageInventory
	err     error
}

func (f fakeCatalogIndexer) Index(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.catalog, nil
}

type recordingCatalogIndexer struct {
	catalog  *deliveryapis.PackageInventory
	registry string
	err      error
}

func (f *recordingCatalogIndexer) Index(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	f.registry = registry
	if f.err != nil {
		return nil, f.err
	}
	return f.catalog, nil
}

type contextRecordingIndexer struct {
	catalog    *deliveryapis.PackageInventory
	indexCtx   context.Context
	refreshCtx context.Context
}

type recordingRepositoryIndexer struct {
	catalog      *deliveryapis.PackageInventory
	indexCalls   int
	repositories []string
}

func (f *recordingRepositoryIndexer) Index(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	f.indexCalls++
	return f.catalog, nil
}

func (f *recordingRepositoryIndexer) IndexRepositories(ctx context.Context, registry string, repositories []string) (*deliveryapis.PackageInventory, error) {
	f.repositories = append([]string(nil), repositories...)
	return f.catalog, nil
}

func (f *contextRecordingIndexer) Index(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	f.indexCtx = ctx
	return f.catalog, nil
}

func (f *contextRecordingIndexer) Refresh(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	f.refreshCtx = ctx
	return f.catalog, nil
}

func registryResolverCatalog() *deliveryapis.PackageInventory {
	catalog := deliveryapis.NewPackageInventory("registry")
	catalog.Spec.Packages = []deliveryapis.PackageEntry{
		registryResolverPackage("k8s", "k8s", "v1.36.0", deliveryapis.ContentProfileK8s),
		registryResolverPackage("containerd", "cri", "2.1.0", deliveryapis.ContentProfileRuntime),
		registryResolverPackage("calico", "cni", "v3.30.0", deliveryapis.ContentProfileAddon),
	}
	return catalog
}

func registryResolverPackage(name, kind, version, profile string) deliveryapis.PackageEntry {
	return deliveryapis.PackageEntry{
		Name:           name,
		Kind:           kind,
		Version:        version,
		Arch:           "amd64",
		ContentProfile: profile,
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    "registry.local:5000/kubeclipper/packages/" + kind + "/" + name + ":" + version,
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
		Contents: deliveryapis.ContentsForProfile(profile),
	}
}

type fakeDeliveryCoreOperator struct {
	configMap  *v1.ConfigMap
	configMaps map[string]*v1.ConfigMap
}

func newFakeDeliveryCoreOperator(t *testing.T, policy *deliveryapis.SupportPolicy) *fakeDeliveryCoreOperator {
	t.Helper()
	data, err := json.Marshal(policy)
	if err != nil {
		t.Fatalf("marshal policy: %+v", err)
	}
	policyConfigMap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: deliveryapis.DeliveryPolicyConfigMapName},
		Data:       map[string]string{deliveryapis.DeliveryPolicyConfigMapKey: string(data)},
	}
	return &fakeDeliveryCoreOperator{
		configMap: policyConfigMap,
		configMaps: map[string]*v1.ConfigMap{
			deliveryapis.DeliveryPolicyConfigMapName: policyConfigMap,
			constatns.DeployConfigConfigMapName: {
				ObjectMeta: metav1.ObjectMeta{Name: constatns.DeployConfigConfigMapName},
				Data: map[string]string{
					constatns.DeployConfigConfigMapKey: "packageRegistry: packages.example.com\n",
				},
			},
		},
	}
}

func (f *fakeDeliveryCoreOperator) ListConfigMaps(ctx context.Context, q *query.Query) (*v1.ConfigMapList, error) {
	return &v1.ConfigMapList{}, nil
}

func (f *fakeDeliveryCoreOperator) WatchConfigMaps(ctx context.Context, q *query.Query) (watch.Interface, error) {
	return nil, nil
}

func (f *fakeDeliveryCoreOperator) GetConfigMap(ctx context.Context, name string) (*v1.ConfigMap, error) {
	return f.GetConfigMapEx(ctx, name, "")
}

func (f *fakeDeliveryCoreOperator) GetConfigMapEx(ctx context.Context, name string, resourceVersion string) (*v1.ConfigMap, error) {
	if f.configMaps != nil {
		if cm, ok := f.configMaps[name]; ok {
			return cm, nil
		}
	}
	if f.configMap == nil || f.configMap.Name != name {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "core.kubeclipper.io", Resource: "configmaps"}, name)
	}
	return f.configMap, nil
}

func (f *fakeDeliveryCoreOperator) ListConfigMapsEx(ctx context.Context, q *query.Query) (*models.PageableResponse, error) {
	return &models.PageableResponse{}, nil
}

func (f *fakeDeliveryCoreOperator) CreateConfigMap(ctx context.Context, configmap *v1.ConfigMap) (*v1.ConfigMap, error) {
	if f.configMaps == nil {
		f.configMaps = make(map[string]*v1.ConfigMap)
	}
	f.configMaps[configmap.Name] = configmap
	if configmap.Name == deliveryapis.DeliveryPolicyConfigMapName {
		f.configMap = configmap
	}
	return configmap, nil
}

func (f *fakeDeliveryCoreOperator) UpdateConfigMap(ctx context.Context, configmap *v1.ConfigMap) (*v1.ConfigMap, error) {
	if f.configMaps == nil {
		f.configMaps = make(map[string]*v1.ConfigMap)
	}
	f.configMaps[configmap.Name] = configmap
	if configmap.Name == deliveryapis.DeliveryPolicyConfigMapName {
		f.configMap = configmap
	}
	return configmap, nil
}

func (f *fakeDeliveryCoreOperator) DeleteConfigMap(ctx context.Context, name string) error {
	if f.configMaps != nil {
		delete(f.configMaps, name)
	}
	if name == deliveryapis.DeliveryPolicyConfigMapName {
		f.configMap = nil
	}
	return nil
}
