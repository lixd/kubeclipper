/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package common

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryfetcher "github.com/kubeclipper/kubeclipper/pkg/delivery/fetcher"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/utils/strutil"
)

const (
	artifactPrefetchName    = "artifact-prefetch"
	artifactPrefetchVersion = "v1"
)

func init() {
	if err := component.RegisterAgentStep(artifactPrefetchIdentity(), &ArtifactPrefetch{}); err != nil {
		panic(err)
	}
}

type ArtifactPrefetch struct {
	Plan deliveryapis.ResolvedArtifactPlan `json:"plan"`
}

func (p *ArtifactPrefetch) Install(ctx context.Context, opts component.Options) ([]byte, error) {
	for _, resolved := range p.Plan.Components {
		localContents := make([]deliveryapis.ArtifactContent, 0, len(resolved.Contents))
		for _, content := range resolved.Contents {
			if content.Transport.Type == "" {
				localContents = append(localContents, content)
			}
		}
		if len(localContents) > 0 {
			componentToFetch := resolved
			componentToFetch.Contents = localContents
			if _, err := deliveryfetcher.FetchComponent(ctx, p.Plan.Arch, componentToFetch, opts.DryRun); err != nil {
				return nil, fmt.Errorf("prefetch %s/%s:%s: %w", resolved.Kind, resolved.Name, resolved.Version, err)
			}
		}
		for _, content := range resolved.Contents {
			if content.Transport.Type != deliveryapis.TransportHelmOCI {
				continue
			}
			chart := &Chart{
				Kind:      resolved.Kind,
				PkgName:   resolved.Name,
				Version:   resolved.Version,
				Arch:      resolved.Arch,
				Offline:   true,
				Transport: resolved.Transport,
				Contents:  []deliveryapis.ArtifactContent{content},
			}
			if _, err := chart.downloadResolvedChart(ctx, opts); err != nil {
				return nil, fmt.Errorf("prefetch %s/%s:%s Helm chart: %w", resolved.Kind, resolved.Name, resolved.Version, err)
			}
		}
	}
	return nil, nil
}

func (p *ArtifactPrefetch) Uninstall(context.Context, component.Options) ([]byte, error) {
	return nil, nil
}

func (p *ArtifactPrefetch) NewInstance() component.ObjectMeta {
	return &ArtifactPrefetch{}
}

func ArtifactPrefetchSteps(plan *deliveryapis.ResolvedArtifactPlan, nodes []v1.StepNode) ([]v1.Step, error) {
	if plan == nil || len(plan.Components) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(&ArtifactPrefetch{Plan: *plan})
	if err != nil {
		return nil, err
	}
	return []v1.Step{{
		ID:         strutil.GetUUID(),
		Name:       "prefetchOCIArtifacts",
		Nodes:      nodes,
		Action:     v1.ActionInstall,
		Timeout:    metav1.Duration{Duration: 20 * time.Minute},
		RetryTimes: 1,
		Commands: []v1.Command{{
			Type:          v1.CommandCustom,
			Identity:      artifactPrefetchIdentity(),
			CustomCommand: data,
		}},
	}}, nil
}

func artifactPrefetchIdentity() string {
	return fmt.Sprintf(component.RegisterStepKeyFormat, artifactPrefetchName, artifactPrefetchVersion, component.TypeStep)
}
