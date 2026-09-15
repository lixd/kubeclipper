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

func TestResolveExtensionArtifactUsesPolicyDefault(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		extensionPackage("v1.0.0", "amd64"),
		extensionPackage("v2.0.0", "amd64"),
	}

	component, err := ResolveExtensionArtifact(catalog, extensionPolicy(), ExtensionResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates: []ExtensionCandidate{
			{Kind: "extension", Name: "kubectl-terminal"},
		},
	})
	if err != nil {
		t.Fatalf("ResolveExtensionArtifact() error: %+v", err)
	}
	if component.Version != "v1.0.0" {
		t.Fatalf("version = %q, want policy default v1.0.0", component.Version)
	}
	if component.Kind != "extension" || component.Name != "kubectl-terminal" {
		t.Fatalf("component = %+v", component)
	}
	if component.Arch != "amd64" {
		t.Fatalf("component arch = %q, want amd64", component.Arch)
	}
}

func TestResolveExtensionArtifactRejectsInventoryOnlyPackage(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		extensionPackage("v2.0.0", "amd64"),
	}

	_, err := ResolveExtensionArtifact(catalog, extensionPolicy(), ExtensionResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates: []ExtensionCandidate{
			{Kind: "extension", Name: "kubectl-terminal"},
		},
	})
	assertResolverError(t, err, ErrArtifactNotPublished)
}

func TestResolveExtensionArtifactNotSelectedByPolicy(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		extensionPackage("v1.0.0", "amd64"),
	}

	_, err := ResolveExtensionArtifact(catalog, extensionPolicy(), ExtensionResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates: []ExtensionCandidate{
			{Kind: "extension", Name: "other-extension"},
		},
	})
	assertResolverError(t, err, ErrUnsupportedComponentChoice)
}

func extensionPackage(version, arch string) PackageEntry {
	return PackageEntry{
		Kind:           "extension",
		Name:           "kubectl-terminal",
		Version:        version,
		Arch:           arch,
		ContentProfile: ContentProfileExtension,
		Transport: TransportRef{
			Type:   TransportOCI,
			Ref:    "registry.local/kubeclipper/packages/extension/kubectl-terminal:" + version,
			Digest: testDigest,
		},
		Contents: []ArtifactContent{{Name: ContentConfigs, File: "configs.tar.gz", MediaType: MediaTypeConfigsLayer}},
	}
}

func extensionPolicy() *SupportPolicy {
	policy := NewSupportPolicy("default")
	policy.Spec.Policies = []KubernetesSupportPolicy{{
		Name:  "k8s-v1.36",
		Match: PolicyMatch{KubernetesVersion: "v1.36.*"},
		ComponentSlots: []ComponentSlotRule{{
			Slot:      "extension",
			Selection: SelectionOneOf,
			Required:  true,
			Default:   ComponentChoice{Name: "kubectl-terminal", Version: "v1.0.0"},
			Options: []ComponentOption{{
				Kind:            "extension",
				Name:            "kubectl-terminal",
				AllowedVersions: []string{"v1.0.0"},
			}},
		}},
	}}
	return policy
}
