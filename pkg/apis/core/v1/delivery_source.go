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
	"context"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/kubeclipper/kubeclipper/pkg/constatns"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryindexer "github.com/kubeclipper/kubeclipper/pkg/delivery/indexer"
	modelscore "github.com/kubeclipper/kubeclipper/pkg/models/core"
	"github.com/kubeclipper/kubeclipper/pkg/models/platform"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

var defaultRegistryPackageInventoryIndexer = deliveryindexer.NewRegistryPackageInventoryIndexer(nil)

type RegistryPackageInventoryIndexer interface {
	Index(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error)
}

type RefreshingRegistryPackageInventoryIndexer interface {
	RegistryPackageInventoryIndexer
	Refresh(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error)
}

type indexedInventoryStore struct {
	registry string
	indexer  RegistryPackageInventoryIndexer
}

func (s indexedInventoryStore) Get(ctx context.Context) (*deliveryapis.PackageInventory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	inventory, err := s.indexer.Index(ctx, s.registry)
	if err == nil {
		return inventory, nil
	}
	return nil, err
}

func (s indexedInventoryStore) Refresh(ctx context.Context) (*deliveryapis.PackageInventory, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if refresher, ok := s.indexer.(RefreshingRegistryPackageInventoryIndexer); ok {
		return refresher.Refresh(ctx, s.registry)
	}
	return s.Get(ctx)
}

type deliverySource struct {
	inventoryStore deliveryapis.InventoryStore
	policyStore    deliveryapis.PolicyStore
	registry       string
}

type DeliverySource struct {
	InventoryStore deliveryapis.InventoryStore
	PolicyStore    deliveryapis.PolicyStore
	Registry       string
}

func resolveDeliverySource(ctx context.Context, platformOperator platform.Operator, coreOperator modelscore.Operator, cluster *corev1.Cluster, indexer RegistryPackageInventoryIndexer) (deliverySource, error) {
	source := deliverySource{}
	if registry := resolveOfflineRegistry(ctx, platformOperator, coreOperator, cluster); registry != "" {
		if indexer == nil {
			indexer = defaultRegistryPackageInventoryIndexer
		}
		source.registry = registry
		source.inventoryStore = indexedInventoryStore{
			registry: registry,
			indexer:  indexer,
		}
	}
	source.policyStore = newConfigMapPolicyStore(coreOperator)
	return source, nil
}

func ResolveDeliverySourceForConfig(ctx context.Context, platformOperator platform.Operator, coreOperator modelscore.Operator, indexer RegistryPackageInventoryIndexer) (DeliverySource, error) {
	source, err := resolveDeliverySource(ctx, platformOperator, coreOperator, nil, indexer)
	if err != nil {
		return DeliverySource{}, err
	}
	return DeliverySource{
		InventoryStore: source.inventoryStore,
		PolicyStore:    source.policyStore,
		Registry:       source.registry,
	}, nil
}

func resolveOfflineRegistry(ctx context.Context, platformOperator platform.Operator, coreOperator modelscore.Operator, cluster *corev1.Cluster) string {
	// Package artifacts are independent from Kubernetes and CRI image registries.
	// The deploy configuration is the single source of truth for package delivery.
	return resolveDeployConfigPackageRegistry(ctx, coreOperator)
}

func resolveDeployConfigPackageRegistry(ctx context.Context, coreOperator modelscore.Operator) string {
	if coreOperator == nil {
		return ""
	}
	cm, err := coreOperator.GetConfigMap(ctx, constatns.DeployConfigConfigMapName)
	if err != nil || cm == nil || cm.Data == nil {
		return ""
	}
	data := strings.TrimSpace(cm.Data[constatns.DeployConfigConfigMapKey])
	if data == "" {
		return ""
	}
	var deployConfig struct {
		PackageRegistry string `json:"packageRegistry" yaml:"packageRegistry"`
	}
	if err = yaml.Unmarshal([]byte(data), &deployConfig); err != nil {
		return ""
	}
	return strings.TrimSpace(deployConfig.PackageRegistry)
}
