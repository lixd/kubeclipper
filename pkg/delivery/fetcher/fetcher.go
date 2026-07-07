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

package fetcher

import (
	"context"
	"fmt"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

type ArtifactFetcher interface {
	Fetch(ctx context.Context, plan *deliveryapis.ResolvedArtifactPlan) (*FetchResult, error)
}

type FetchResult struct {
	Components []ComponentFetchResult `json:"components"`
}

type ComponentFetchResult struct {
	Slot         string                    `json:"slot"`
	Kind         string                    `json:"kind"`
	Name         string                    `json:"name"`
	Version      string                    `json:"version"`
	OS           string                    `json:"os,omitempty"`
	Arch         string                    `json:"arch,omitempty"`
	BaseDir      string                    `json:"baseDir,omitempty"`
	ManifestPath string                    `json:"manifestPath,omitempty"`
	Transport    deliveryapis.TransportRef `json:"transport,omitempty"`
	Files        map[string]string         `json:"files"`
}

func NewForTransport(transportType string, dryRun bool) (ArtifactFetcher, error) {
	switch transportType {
	case deliveryapis.TransportOCI:
		return NewOCIArtifactFetcher(dryRun), nil
	default:
		return nil, fmt.Errorf("unsupported artifact transport %q", transportType)
	}
}

func FetchComponent(ctx context.Context, arch string, component deliveryapis.ResolvedComponent, dryRun bool) (ComponentFetchResult, error) {
	fetcher, err := NewForTransport(component.Transport.Type, dryRun)
	if err != nil {
		return ComponentFetchResult{}, err
	}
	targetArch := ComponentArch(component, arch)
	result, err := fetcher.Fetch(ctx, &deliveryapis.ResolvedArtifactPlan{
		KubernetesVersion: component.Version,
		OS:                deliveryapis.DefaultPackageOS,
		Arch:              targetArch,
		Components:        []deliveryapis.ResolvedComponent{component},
	})
	if err != nil {
		return ComponentFetchResult{}, err
	}
	if len(result.Components) == 0 {
		return ComponentFetchResult{}, fmt.Errorf("no fetch result for %s/%s:%s", component.Kind, component.Name, component.Version)
	}
	return result.Components[0], nil
}

func ComponentArch(component deliveryapis.ResolvedComponent, fallback string) string {
	if component.Arch != "" {
		return component.Arch
	}
	return fallback
}

func validatePlan(plan *deliveryapis.ResolvedArtifactPlan) error {
	if plan == nil {
		return fmt.Errorf("resolved artifact plan is nil")
	}
	if plan.Arch == "" {
		return fmt.Errorf("resolved artifact plan arch is required")
	}
	if plan.OS == "" {
		plan.OS = deliveryapis.DefaultPackageOS
	}
	if len(plan.Components) == 0 {
		return fmt.Errorf("resolved artifact plan components are required")
	}
	seen := make(map[string]struct{}, len(plan.Components))
	for i, component := range plan.Components {
		if component.Kind == "" || component.Name == "" || component.Version == "" {
			return fmt.Errorf("resolved artifact plan component[%d] identity is required", i)
		}
		key := component.Kind + "/" + component.Name + ":" + component.Version
		if _, ok := seen[key]; ok {
			return fmt.Errorf("resolved artifact plan contains duplicate component %s", key)
		}
		seen[key] = struct{}{}
		if component.OS != "" && component.OS != plan.OS {
			return fmt.Errorf("resolved artifact plan component[%d] os %q does not match plan os %q", i, component.OS, plan.OS)
		}
		if component.Arch != "" && component.Arch != plan.Arch {
			return fmt.Errorf("resolved artifact plan component[%d] arch %q does not match plan arch %q", i, component.Arch, plan.Arch)
		}
		if component.Transport.Type != deliveryapis.TransportOCI {
			return fmt.Errorf("resolved artifact plan component[%d] transport %q is not supported", i, component.Transport.Type)
		}
		if err := validateComponentContents(i, component.Contents); err != nil {
			return err
		}
	}
	return nil
}

func validateComponentContents(componentIndex int, contents []deliveryapis.ArtifactContent) error {
	if len(contents) == 0 {
		return fmt.Errorf("resolved artifact plan component[%d] contents are required", componentIndex)
	}
	seen := make(map[string]struct{}, len(contents))
	for j, content := range contents {
		if content.Name == "" {
			return fmt.Errorf("resolved artifact plan component[%d] content[%d] name is required", componentIndex, j)
		}
		if _, ok := seen[content.Name]; ok {
			return fmt.Errorf("resolved artifact plan component[%d] duplicate content %q", componentIndex, content.Name)
		}
		seen[content.Name] = struct{}{}
	}
	return nil
}
