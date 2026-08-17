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
	"fmt"

	"k8s.io/component-base/version"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	schemecorev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func (h *handler) withResolvedAddonArtifacts(ctx context.Context, cluster *schemecorev1.Cluster, addon component.Interface, action schemecorev1.StepAction) (context.Context, error) {
	if action != schemecorev1.ActionInstall && action != schemecorev1.ActionUpgrade {
		return ctx, nil
	}
	aware, ok := addon.(component.OfflineArtifactAware)
	if !ok || cluster == nil {
		return ctx, nil
	}
	requests := aware.GetOfflineArtifactRequests()
	if len(requests) == 0 {
		return ctx, nil
	}
	source, err := resolveDeliverySource(ctx, h.platformOperator, h.coreOperator, cluster, h.deliveryIndexer)
	if err != nil {
		return nil, err
	}
	if source.inventoryStore == nil || source.policyStore == nil {
		return ctx, nil
	}
	arch, ok := singleTargetArchFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("OCI delivery requires a single target architecture")
	}
	inventory, err := source.inventoryStore.Get(ctx)
	if err != nil {
		return nil, err
	}
	policy, err := source.policyStore.Get(ctx)
	if err != nil {
		return nil, err
	}
	plan, _ := component.GetResolvedArtifactPlan(ctx).(*deliveryapis.ResolvedArtifactPlan)
	merged := &deliveryapis.ResolvedArtifactPlan{}
	if plan != nil {
		merged.KubernetesVersion = plan.KubernetesVersion
		merged.OS = plan.OS
		merged.Arch = plan.Arch
		merged.Components = append(merged.Components, plan.Components...)
	}
	if merged.OS == "" {
		merged.OS = deliveryapis.DefaultPackageOS
	}
	if merged.Arch == "" {
		merged.Arch = arch
	}
	if merged.KubernetesVersion == "" {
		merged.KubernetesVersion = cluster.KubernetesVersion
	}
	for _, request := range requests {
		selected, err := deliveryapis.ResolvePolicyPackage(inventory, policy, deliveryapis.PolicyPackageResolveRequest{
			SlotPrefix:         "addon",
			Kind:               request.Kind,
			Name:               request.Name,
			Version:            request.Version,
			OS:                 merged.OS,
			Arch:               arch,
			KubernetesVersion:  merged.KubernetesVersion,
			KubeClipperVersion: version.Get().GitVersion,
		})
		if err != nil {
			return nil, err
		}
		if err = appendResolvedComponent(merged, selected); err != nil {
			return nil, err
		}
	}
	return component.WithResolvedArtifactPlan(ctx, merged), nil
}

func appendResolvedComponent(plan *deliveryapis.ResolvedArtifactPlan, component deliveryapis.ResolvedComponent) error {
	if plan == nil {
		return fmt.Errorf("resolved artifact plan is nil")
	}
	for _, existing := range plan.Components {
		if existing.Kind == component.Kind && existing.Name == component.Name && existing.Version == component.Version {
			return fmt.Errorf("resolved artifact plan already contains %s/%s:%s from slot %q", component.Kind, component.Name, component.Version, existing.Slot)
		}
	}
	plan.Components = append(plan.Components, component)
	return nil
}

func singleTargetArchFromContext(ctx context.Context) (string, bool) {
	extra := component.GetExtraMetadata(ctx)
	return singleTargetArch(&extra)
}
