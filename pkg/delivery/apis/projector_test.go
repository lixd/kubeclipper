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

package apis

import "testing"

func TestProjectComponentMeta(t *testing.T) {
	catalog := projectorCatalog()
	policy := projectorPolicy()

	projection, err := ProjectComponentMeta(catalog, policy, ProjectOptions{})
	if err != nil {
		t.Fatalf("ProjectComponentMeta() error: %+v", err)
	}
	if len(projection.Addons) != 5 {
		t.Fatalf("addons length = %d, want 5", len(projection.Addons))
	}
	if len(projection.Rules) != 2 {
		t.Fatalf("rules length = %d, want 2", len(projection.Rules))
	}
	rule := projection.Rules[0]
	if rule["version"] != "v1.36.0" {
		t.Fatalf("first rule version = %v", rule["version"])
	}
	vc := rule["version_control"].(map[string]interface{})
	cri := vc["cri"].([]map[string]interface{})
	if len(cri) != 2 {
		t.Fatalf("cri options length = %d, want 2", len(cri))
	}
}

func TestProjectComponentMetaArchFilter(t *testing.T) {
	projection, err := ProjectComponentMeta(projectorCatalog(), projectorPolicy(), ProjectOptions{Archs: []string{"amd64"}})
	if err != nil {
		t.Fatalf("ProjectComponentMeta() error: %+v", err)
	}
	for _, addon := range projection.Addons {
		if addon.Arch != "amd64" {
			t.Fatalf("addon arch = %q, want amd64", addon.Arch)
		}
	}
	if len(projection.Rules) != 1 {
		t.Fatalf("rules length = %d, want 1", len(projection.Rules))
	}
}

func TestProjectComponentMetaHidesUnsupportedInventoryPackages(t *testing.T) {
	projection, err := ProjectComponentMeta(projectorCatalog(), projectorPolicy(), ProjectOptions{})
	if err != nil {
		t.Fatalf("ProjectComponentMeta() error: %+v", err)
	}
	for _, addon := range projection.Addons {
		if addon.Type == "csi" && addon.Name == "nfs" {
			t.Fatalf("unsupported inventory package was projected: %+v", addon)
		}
	}
}

func TestProjectComponentMetaMultiArchIntersection(t *testing.T) {
	projection, err := ProjectComponentMeta(projectorCatalog(), projectorPolicy(), ProjectOptions{Archs: []string{"amd64", "arm64"}})
	if err != nil {
		t.Fatalf("ProjectComponentMeta() error: %+v", err)
	}
	if len(projection.Rules) != 0 {
		t.Fatalf("rules length = %d, want 0 because required calico arm64 is missing", len(projection.Rules))
	}
}

func TestProjectComponentMetaDoesNotGateClusterVersionsOnBootstrapPackages(t *testing.T) {
	policy := projectorPolicy()
	policy.Spec.Policies[0].ComponentSlots = append(policy.Spec.Policies[0].ComponentSlots, ComponentSlotRule{
		Slot:      "bootstrap-kubeclipper",
		Selection: SelectionOneOf,
		Required:  true,
		Default:   ComponentChoice{Name: "kubeclipper", Version: "v1.8.0"},
		Options: []ComponentOption{{
			Name: "kubeclipper", Kind: "bootstrap", AllowedVersions: []string{"v1.8.0"},
		}},
	})

	projection, err := ProjectComponentMeta(projectorCatalog(), policy, ProjectOptions{Archs: []string{"amd64"}})
	if err != nil {
		t.Fatalf("ProjectComponentMeta() error: %+v", err)
	}
	if len(projection.Rules) != 1 {
		t.Fatalf("rules length = %d, want 1", len(projection.Rules))
	}
	versionControl := projection.Rules[0]["version_control"].(map[string]interface{})
	if _, ok := versionControl["bootstrap-kubeclipper"]; ok {
		t.Fatalf("bootstrap slot leaked into cluster version controls: %+v", versionControl)
	}
	if availability := findUnavailable(projection.Unavailable, "bootstrap-kubeclipper", "bootstrap", "kubeclipper", "v1.8.0", "amd64"); availability != nil {
		t.Fatalf("bootstrap package leaked into cluster availability: %+v", availability)
	}
}

func TestProjectComponentMetaIgnoresInventoryPresentationFields(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		{
			Name:           "k8s",
			Kind:           "k8s",
			Version:        "v1.36.0",
			Arch:           "amd64",
			ContentProfile: ContentProfileK8s,
			Transport: TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/k8s/k8s:v1.36.0",
				Digest: testDigest,
			},
			Contents: ContentsForProfile(ContentProfileK8s),
		},
		{
			Name:           "containerd",
			Kind:           "cri",
			Version:        "2.1.0",
			Arch:           "amd64",
			ContentProfile: ContentProfileRuntime,
			Transport: TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/cri/containerd:2.1.0",
				Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			},
			Contents: ContentsForProfile(ContentProfileRuntime),
		},
		{
			Name:           "calico",
			Kind:           "cni",
			Version:        "v3.30.0",
			Arch:           "amd64",
			ContentProfile: ContentProfileAddon,
			Transport: TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/cni/calico:v3.30.0",
				Digest: testDigest,
			},
			Contents: ContentsForProfile(ContentProfileAddon),
		},
	}

	projection, err := ProjectComponentMeta(catalog, projectorPolicy(), ProjectOptions{Archs: []string{"amd64"}})
	if err != nil {
		t.Fatalf("ProjectComponentMeta() error: %+v", err)
	}
	if len(projection.Rules) != 1 {
		t.Fatalf("rules length = %d, want 1", len(projection.Rules))
	}
	versionControl := projection.Rules[0]["version_control"].(map[string]interface{})
	cri := versionControl["cri"].([]map[string]interface{})
	if len(cri) != 1 || cri[0]["name"] != "containerd" {
		t.Fatalf("cri options = %+v, want containerd from OCI package", cri)
	}
}

func TestProjectComponentMetaReportsPolicyPackageNotPublished(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		projectorPackage("k8s", "k8s", "v1.36.0", "amd64", ContentProfileK8s),
		projectorPackage("containerd", "cri", "2.1.0", "amd64", ContentProfileRuntime),
		projectorPackage("calico", "cni", "v3.30.0", "amd64", ContentProfileAddon),
	}
	policy := projectorPolicy()
	policy.Spec.Policies[0].ComponentSlots = append(policy.Spec.Policies[0].ComponentSlots, ComponentSlotRule{
		Slot:      "csi",
		Selection: SelectionZeroOrOne,
		Required:  false,
		Options: []ComponentOption{
			{Name: "nfs", Kind: "csi", AllowedVersions: []string{"v4.10.0"}},
		},
	})

	projection, err := ProjectComponentMeta(catalog, policy, ProjectOptions{})
	if err != nil {
		t.Fatalf("ProjectComponentMeta() error: %+v", err)
	}
	availability := findUnavailable(projection.Unavailable, "csi", "csi", "nfs", "v4.10.0", "")
	if availability == nil {
		t.Fatalf("unavailable csi/nfs:v4.10.0 not reported: %+v", projection.Unavailable)
	}
	if availability.Reason != "notPublished" {
		t.Fatalf("reason = %q, want notPublished", availability.Reason)
	}
}

func TestProjectComponentMetaReportsPackageArchUnavailable(t *testing.T) {
	projection, err := ProjectComponentMeta(projectorCatalog(), projectorPolicy(), ProjectOptions{Archs: []string{"arm64"}})
	if err != nil {
		t.Fatalf("ProjectComponentMeta() error: %+v", err)
	}
	availability := findUnavailable(projection.Unavailable, "cni", "cni", "calico", "v3.30.0", "arm64")
	if availability == nil {
		t.Fatalf("unavailable cni/calico:v3.30.0 arm64 not reported: %+v", projection.Unavailable)
	}
	if availability.Reason != "archUnavailable" {
		t.Fatalf("reason = %q, want archUnavailable", availability.Reason)
	}
}

func TestProjectComponentMetaSelectsSinglePolicyByKubeClipperVersion(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		projectorPackage("k8s", "k8s", "v1.36.0", "amd64", ContentProfileK8s),
		projectorPackage("containerd", "cri", "2.1.0", "amd64", ContentProfileRuntime),
		projectorPackage("docker", "cri", "24.0.0", "amd64", ContentProfileRuntime),
		projectorPackage("calico", "cni", "v3.30.0", "amd64", ContentProfileAddon),
	}
	policy := NewSupportPolicy("default")
	policy.Spec.Policies = []KubernetesSupportPolicy{
		{
			Name: "k8s-v1.36-kc-v1.7.0",
			Match: PolicyMatch{
				KubernetesVersion:  "v1.36.*",
				KubeClipperVersion: "v1.7.0",
			},
			ComponentSlots: []ComponentSlotRule{
				{
					Slot:      "cri",
					Selection: SelectionOneOf,
					Required:  true,
					Default:   ComponentChoice{Name: "containerd", Version: "2.1.0"},
					Options:   []ComponentOption{{Name: "containerd", Kind: "cri", AllowedVersions: []string{"2.1.0"}}},
				},
				{
					Slot:      "cni",
					Selection: SelectionOneOf,
					Required:  true,
					Default:   ComponentChoice{Name: "calico", Version: "v3.30.0"},
					Options:   []ComponentOption{{Name: "calico", Kind: "cni", AllowedVersions: []string{"v3.30.0"}}},
				},
			},
		},
		{
			Name: "k8s-v1.36-kc-v1.8.0",
			Match: PolicyMatch{
				KubernetesVersion:  "v1.36.*",
				KubeClipperVersion: "v1.8.0",
			},
			ComponentSlots: []ComponentSlotRule{
				{
					Slot:      "cri",
					Selection: SelectionOneOf,
					Required:  true,
					Default:   ComponentChoice{Name: "docker", Version: "24.0.0"},
					Options:   []ComponentOption{{Name: "docker", Kind: "cri", AllowedVersions: []string{"24.0.0"}}},
				},
				{
					Slot:      "cni",
					Selection: SelectionOneOf,
					Required:  true,
					Default:   ComponentChoice{Name: "calico", Version: "v3.30.0"},
					Options:   []ComponentOption{{Name: "calico", Kind: "cni", AllowedVersions: []string{"v3.30.0"}}},
				},
			},
		},
	}

	projection, err := ProjectComponentMeta(catalog, policy, ProjectOptions{KubeClipperVersion: "v1.8.0"})
	if err != nil {
		t.Fatalf("ProjectComponentMeta() error: %+v", err)
	}
	if len(projection.Rules) != 1 {
		t.Fatalf("rules length = %d, want 1", len(projection.Rules))
	}
	versionControl := projection.Rules[0]["version_control"].(map[string]interface{})
	cri := versionControl["cri"].([]map[string]interface{})
	if len(cri) != 1 {
		t.Fatalf("cri options length = %d, want 1", len(cri))
	}
	if cri[0]["name"] != "docker" || cri[0]["default"] != true {
		t.Fatalf("cri option = %+v, want default docker only", cri[0])
	}
}

func findUnavailable(items []ComponentAvailability, slot, kind, name, version, arch string) *ComponentAvailability {
	for i := range items {
		item := &items[i]
		if item.Slot == slot && item.Kind == kind && item.Name == name && item.Version == version && item.Arch == arch {
			return item
		}
	}
	return nil
}

func projectorCatalog() *PackageInventory {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		projectorPackage("k8s", "k8s", "v1.36.0", "amd64", ContentProfileK8s),
		projectorPackage("k8s", "k8s", "v1.36.0", "arm64", ContentProfileK8s),
		projectorPackage("containerd", "cri", "2.1.0", "amd64", ContentProfileRuntime),
		projectorPackage("containerd", "cri", "2.1.0", "arm64", ContentProfileRuntime),
		projectorPackage("calico", "cni", "v3.30.0", "amd64", ContentProfileAddon),
		{
			Name:           "nfs",
			Kind:           "csi",
			Version:        "v4.1.0",
			Arch:           "amd64",
			ContentProfile: ContentProfileAddon,
			Transport: TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/csi/nfs:v4.1.0",
				Digest: testDigest,
			},
			Contents: ContentsForProfile(ContentProfileAddon),
		},
	}
	return catalog
}

func projectorPackage(name, kind, version, arch, profile string) PackageEntry {
	return PackageEntry{
		Name:           name,
		Kind:           kind,
		Version:        version,
		Arch:           arch,
		ContentProfile: profile,
		Transport: TransportRef{
			Type:   TransportOCI,
			Ref:    "registry.local/kubeclipper/packages/" + kind + "/" + name + ":" + version,
			Digest: testDigest,
		},
		Contents: ContentsForProfile(profile),
	}
}

func projectorPolicy() *SupportPolicy {
	policy := NewSupportPolicy("default")
	policy.Spec.Policies = []KubernetesSupportPolicy{
		{
			Name: "k8s-v1.36-stable",
			Match: PolicyMatch{
				KubernetesVersion: "v1.36.*",
			},
			ComponentSlots: []ComponentSlotRule{
				{
					Slot:      "cri",
					Selection: SelectionOneOf,
					Required:  true,
					Default: ComponentChoice{
						Name:    "containerd",
						Version: "2.1.0",
					},
					Options: []ComponentOption{
						{Name: "containerd", Kind: "cri", AllowedVersions: []string{"2.1.0"}},
					},
				},
				{
					Slot:      "cni",
					Selection: SelectionOneOf,
					Required:  true,
					Default: ComponentChoice{
						Name:    "calico",
						Version: "v3.30.0",
					},
					Options: []ComponentOption{
						{Name: "calico", Kind: "cni", AllowedVersions: []string{"v3.30.0"}},
					},
				},
			},
		},
	}
	return policy
}
