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
	"fmt"

	"k8s.io/component-base/version"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func (h *handler) withResolvedArtifactPlan(ctx context.Context, extra *component.ExtraMetadata, cluster *v1.Cluster, action v1.StepAction) (context.Context, error) {
	if (action != v1.ActionInstall && action != v1.ActionUpgrade) || extra == nil || cluster == nil {
		return ctx, nil
	}
	arch, ok := singleTargetArch(extra)
	if !ok {
		return nil, fmt.Errorf("OCI delivery requires a single target architecture")
	}
	source, err := resolveDeliverySource(ctx, h.platformOperator, h.coreOperator, cluster, h.deliveryIndexer)
	if err != nil {
		return nil, err
	}
	if source.inventoryStore == nil || source.policyStore == nil {
		return nil, fmt.Errorf("delivery source requires package inventory and support policy")
	}
	kubernetesVersion := cluster.KubernetesVersion
	if action == v1.ActionUpgrade && extra.KubeVersion != "" {
		kubernetesVersion = extra.KubeVersion
	}
	plan, err := deliveryapis.ResolveArtifactsFromStores(ctx, source.inventoryStore, source.policyStore, deliveryapis.ResolveRequest{
		KubernetesVersion:  kubernetesVersion,
		OS:                 deliveryapis.DefaultPackageOS,
		Arch:               arch,
		KubeClipperVersion: version.Get().GitVersion,
		Components:         resolveComponentChoices(cluster),
	})
	if err != nil {
		return nil, err
	}
	return component.WithResolvedArtifactPlan(ctx, plan), nil
}

func singleTargetArch(extra *component.ExtraMetadata) (string, bool) {
	arch := ""
	for _, node := range extra.GetAllNodes() {
		if node.Arch == "" {
			return "", false
		}
		if arch == "" {
			arch = node.Arch
			continue
		}
		if arch != node.Arch {
			return "", false
		}
	}
	return arch, arch != ""
}

func resolveComponentChoices(cluster *v1.Cluster) map[string]deliveryapis.ComponentChoice {
	components := map[string]deliveryapis.ComponentChoice{}
	if cluster.ContainerRuntime.Type != "" && cluster.ContainerRuntime.Version != "" {
		components["cri"] = deliveryapis.ComponentChoice{Name: cluster.ContainerRuntime.Type, Version: cluster.ContainerRuntime.Version}
	}
	if cluster.CNI.Type != "" && cluster.CNI.Version != "" {
		components["cni"] = deliveryapis.ComponentChoice{Name: cluster.CNI.Type, Version: cluster.CNI.Version}
	}
	if len(components) == 0 {
		return nil
	}
	return components
}
