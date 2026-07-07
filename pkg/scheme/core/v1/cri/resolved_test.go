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

package cri

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestContainerdInitStepAppliesResolvedTransport(t *testing.T) {
	ctx := component.WithExtraMetadata(context.Background(), component.ExtraMetadata{Offline: true, KubeVersion: "v1.36.0"})
	ctx = component.WithResolvedArtifactPlan(ctx, &deliveryapis.ResolvedArtifactPlan{Components: []deliveryapis.ResolvedComponent{
		{
			Kind:    "cri",
			Name:    "containerd",
			Version: "2.1.0",
			Transport: deliveryapis.TransportRef{
				Type:   deliveryapis.TransportOCI,
				Ref:    "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0",
				Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			},
			Contents: []deliveryapis.ArtifactContent{{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"}},
		},
	}})
	cluster := &v1.Cluster{
		KubernetesVersion: "v1.36.0",
		ContainerRuntime:  v1.ContainerRuntime{Type: v1.CRIContainerd, Version: "2.1.0"},
	}
	runnable := &ContainerdRunnable{}
	if err := runnable.InitStep(ctx, cluster, []v1.StepNode{{ID: "master-1"}}, nil); err != nil {
		t.Fatalf("InitStep() error: %+v", err)
	}
	steps := runnable.GetActionSteps(v1.ActionInstall)
	if len(steps) != 1 || len(steps[0].Commands) != 1 {
		t.Fatalf("steps = %+v", steps)
	}
	var decoded ContainerdRunnable
	if err := json.Unmarshal(steps[0].Commands[0].CustomCommand, &decoded); err != nil {
		t.Fatalf("unmarshal containerd command: %+v", err)
	}
	if decoded.Transport.Ref != "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0" {
		t.Fatalf("transport = %+v", decoded.Transport)
	}
	if len(decoded.Contents) != 1 || decoded.Contents[0].Name != deliveryapis.ContentConfigs {
		t.Fatalf("contents = %+v", decoded.Contents)
	}
}

func TestContainerdInitStepDoesNotResolveFromPackageRegistry(t *testing.T) {
	ctx := component.WithExtraMetadata(context.Background(), component.ExtraMetadata{Offline: true, KubeVersion: "v1.36.0"})
	cluster := &v1.Cluster{
		KubernetesVersion: "v1.36.0",
		ContainerRuntime:  v1.ContainerRuntime{Type: v1.CRIContainerd, Version: "2.1.0"},
	}
	runnable := &ContainerdRunnable{}
	if err := runnable.InitStep(ctx, cluster, []v1.StepNode{{ID: "master-1"}}, nil); err != nil {
		t.Fatalf("InitStep() error: %+v", err)
	}
	steps := runnable.GetActionSteps(v1.ActionInstall)
	var decoded ContainerdRunnable
	if err := json.Unmarshal(steps[0].Commands[0].CustomCommand, &decoded); err != nil {
		t.Fatalf("unmarshal containerd command: %+v", err)
	}
	if decoded.Transport.Type != "" {
		t.Fatalf("transport = %+v", decoded.Transport)
	}
}
