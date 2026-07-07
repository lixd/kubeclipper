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

package apis

import "testing"

func TestResolveBootstrapBinary(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		{
			Kind:           "binary",
			Name:           "kubeclipper-agent",
			Version:        "v1.7.0",
			Arch:           "amd64",
			ContentProfile: ContentProfileBinary,
			Transport: TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/binary/kubeclipper-agent:v1.7.0",
				Digest: testDigest,
			},
			Contents: []ArtifactContent{
				{Name: ContentBinary, File: "kubeclipper-agent", MediaType: MediaTypeBinaryLayer},
			},
		},
	}

	component, err := ResolveBootstrapBinary(catalog, bootstrapPolicy(), BootstrapBinaryResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates: []PackageCandidate{
			{Kind: "binary", Name: "kubeclipper-agent"},
		},
	})
	if err != nil {
		t.Fatalf("ResolveBootstrapBinary() error: %+v", err)
	}
	if component.Slot != "bootstrap-kubeclipper-agent" || component.Name != "kubeclipper-agent" {
		t.Fatalf("component = %+v", component)
	}
	if component.Arch != "amd64" {
		t.Fatalf("component arch = %q, want amd64", component.Arch)
	}
	if len(component.Contents) != 1 || component.Contents[0].Name != ContentBinary {
		t.Fatalf("component contents = %+v", component.Contents)
	}
}

func TestResolveBootstrapBinaryRequiresCandidates(t *testing.T) {
	_, err := ResolveBootstrapBinary(NewPackageInventory("default"), bootstrapPolicy(), BootstrapBinaryResolveRequest{Arch: "amd64", KubernetesVersion: "v1.36.0"})
	if err == nil {
		t.Fatalf("ResolveBootstrapBinary() expected error")
	}
}

func bootstrapPolicy() *SupportPolicy {
	policy := NewSupportPolicy("default")
	policy.Spec.Policies = []KubernetesSupportPolicy{{
		Name:  "k8s-v1.36",
		Match: PolicyMatch{KubernetesVersion: "v1.36.*"},
		ComponentSlots: []ComponentSlotRule{{
			Slot:      "bootstrap-kubeclipper-agent",
			Selection: SelectionOneOf,
			Required:  true,
			Default:   ComponentChoice{Name: "kubeclipper-agent", Version: "v1.7.0"},
			Options: []ComponentOption{{
				Kind:            "binary",
				Name:            "kubeclipper-agent",
				AllowedVersions: []string{"v1.7.0"},
			}},
		}},
	}}
	return policy
}
