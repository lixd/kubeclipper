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
	"encoding/json"
	"testing"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestArtifactPrefetchStepsContainsResolvedPlan(t *testing.T) {
	plan := &deliveryapis.ResolvedArtifactPlan{
		OS:   deliveryapis.DefaultPackageOS,
		Arch: "amd64",
		Components: []deliveryapis.ResolvedComponent{{
			Slot:    "cri",
			Kind:    "cri",
			Name:    "containerd",
			Version: "2.2.4",
			Transport: deliveryapis.TransportRef{
				Type:   deliveryapis.TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/cri/containerd:2.2.4",
				Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			},
		}},
	}
	nodes := []v1.StepNode{{ID: "node-1"}}
	steps, err := ArtifactPrefetchSteps(plan, nodes)
	if err != nil {
		t.Fatalf("ArtifactPrefetchSteps() error: %+v", err)
	}
	if len(steps) != 1 || steps[0].Name != "prefetchOCIArtifacts" {
		t.Fatalf("steps = %+v", steps)
	}
	if len(steps[0].Nodes) != 1 || steps[0].Nodes[0].ID != "node-1" {
		t.Fatalf("step nodes = %+v", steps[0].Nodes)
	}
	if len(steps[0].Commands) != 1 || steps[0].Commands[0].Identity != artifactPrefetchIdentity() {
		t.Fatalf("step commands = %+v", steps[0].Commands)
	}
	var command ArtifactPrefetch
	if err = json.Unmarshal(steps[0].Commands[0].CustomCommand, &command); err != nil {
		t.Fatalf("decode prefetch command: %+v", err)
	}
	if len(command.Plan.Components) != 1 || command.Plan.Components[0].Name != "containerd" {
		t.Fatalf("prefetch plan = %+v", command.Plan)
	}
}

func TestArtifactPrefetchStepsSkipsEmptyPlan(t *testing.T) {
	steps, err := ArtifactPrefetchSteps(&deliveryapis.ResolvedArtifactPlan{}, []v1.StepNode{{ID: "node-1"}})
	if err != nil {
		t.Fatalf("ArtifactPrefetchSteps() error: %+v", err)
	}
	if len(steps) != 0 {
		t.Fatalf("steps = %+v, want none", steps)
	}
}
