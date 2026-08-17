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

func TestResolveArtifactsDefaultComponents(t *testing.T) {
	plan, err := ResolveArtifacts(resolverCatalog(), resolverPolicy(), ResolveRequest{
		KubernetesVersion: "v1.36.0",
		Arch:              "amd64",
	})
	if err != nil {
		t.Fatalf("ResolveArtifacts() error: %+v", err)
	}
	if plan.KubernetesVersion != "v1.36.0" || plan.OS != DefaultPackageOS || plan.Arch != "amd64" {
		t.Fatalf("plan target = %s/%s/%s", plan.KubernetesVersion, plan.OS, plan.Arch)
	}
	if len(plan.Components) != 3 {
		t.Fatalf("components length = %d, want 3", len(plan.Components))
	}
	if plan.Components[0].Slot != "k8s" || plan.Components[0].Name != "k8s" || plan.Components[0].Version != "v1.36.0" {
		t.Fatalf("first component = %+v", plan.Components[0])
	}
	if plan.Components[0].Arch != "amd64" || plan.Components[1].Arch != "amd64" {
		t.Fatalf("component archs = %q/%q, want amd64/amd64", plan.Components[0].Arch, plan.Components[1].Arch)
	}
	if plan.Components[1].Slot != "cri" || plan.Components[1].Name != "containerd" || plan.Components[1].Version != "2.1.0" {
		t.Fatalf("second component = %+v", plan.Components[1])
	}
}

func TestResolveArtifactsExplicitComponent(t *testing.T) {
	plan, err := ResolveArtifacts(resolverCatalog(), resolverPolicy(), ResolveRequest{
		KubernetesVersion: "v1.36.0",
		Arch:              "amd64",
		Components: map[string]ComponentChoice{
			"cri": {Name: "containerd", Version: "2.1.1"},
		},
	})
	if err != nil {
		t.Fatalf("ResolveArtifacts() error: %+v", err)
	}
	if plan.Components[1].Version != "2.1.1" {
		t.Fatalf("cri version = %q, want 2.1.1", plan.Components[1].Version)
	}
}

func TestResolveArtifactsErrors(t *testing.T) {
	for _, tt := range []struct {
		name string
		req  ResolveRequest
		code string
	}{
		{
			name: "unsupported k8s",
			req:  ResolveRequest{KubernetesVersion: "v1.35.0", Arch: "amd64"},
			code: ErrUnsupportedKubernetesVersion,
		},
		{
			name: "unknown slot",
			req: ResolveRequest{KubernetesVersion: "v1.36.0", Arch: "amd64", Components: map[string]ComponentChoice{
				"gpu": {Name: "nvidia", Version: "1.0.0"},
			}},
			code: ErrUnsupportedComponentSlot,
		},
		{
			name: "unknown choice",
			req: ResolveRequest{KubernetesVersion: "v1.36.0", Arch: "amd64", Components: map[string]ComponentChoice{
				"cri": {Name: "docker", Version: "20.10.24"},
			}},
			code: ErrUnsupportedComponentChoice,
		},
		{
			name: "unsupported version",
			req: ResolveRequest{KubernetesVersion: "v1.36.0", Arch: "amd64", Components: map[string]ComponentChoice{
				"cri": {Name: "containerd", Version: "2.0.0"},
			}},
			code: ErrUnsupportedComponentVersion,
		},
		{
			name: "arch unavailable",
			req:  ResolveRequest{KubernetesVersion: "v1.36.0", Arch: "arm64"},
			code: ErrArtifactArchUnavailable,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ResolveArtifacts(resolverCatalog(), resolverPolicy(), tt.req)
			assertResolverError(t, err, tt.code)
		})
	}
}

func TestResolveArtifactsMatchesPackageIdentity(t *testing.T) {
	catalog := resolverCatalog()
	plan, err := ResolveArtifacts(catalog, resolverPolicy(), ResolveRequest{
		KubernetesVersion: "v1.36.0",
		Arch:              "amd64",
	})
	if err != nil {
		t.Fatalf("ResolveArtifacts() error: %+v", err)
	}
	if plan.Components[0].Transport.Ref != "registry.local/kubeclipper/packages/k8s/k8s:v1.36.0" {
		t.Fatalf("k8s transport ref = %q", plan.Components[0].Transport.Ref)
	}
}

func TestResolveArtifactsRejectsDuplicatePackageIdentity(t *testing.T) {
	catalog := resolverCatalog()
	catalog.Spec.Packages = append(catalog.Spec.Packages, PackageEntry{
		Name:           "k8s",
		Kind:           "k8s",
		Version:        "v1.36.0",
		Arch:           "amd64",
		ContentProfile: ContentProfileK8s,
		Transport: TransportRef{
			Type:   TransportOCI,
			Ref:    "registry.local/kubeclipper/packages/k8s/k8s:v1.36.0",
			Digest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
		},
		Contents: ContentsForProfile(ContentProfileK8s),
	})

	_, err := ResolveArtifacts(catalog, resolverPolicy(), ResolveRequest{
		KubernetesVersion: "v1.36.0",
		Arch:              "amd64",
	})
	if err == nil {
		t.Fatalf("ResolveArtifacts() expected duplicate package validation error")
	}
}

func TestResolveArtifactsOutputsDigestPinnedOCIPlan(t *testing.T) {
	catalog := resolverCatalog()
	for i := range catalog.Spec.Packages {
		if catalog.Spec.Packages[i].Kind == "cri" && catalog.Spec.Packages[i].Name == "containerd" && catalog.Spec.Packages[i].Version == "2.1.0" && catalog.Spec.Packages[i].Arch == "amd64" {
			catalog.Spec.Packages[i].Transport = TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/cri/containerd:2.1.0",
				Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			}
		}
	}

	plan, err := ResolveArtifacts(catalog, resolverPolicy(), ResolveRequest{
		KubernetesVersion: "v1.36.0",
		Arch:              "amd64",
	})
	if err != nil {
		t.Fatalf("ResolveArtifacts() error: %+v", err)
	}
	if plan.Components[1].Transport.Type != TransportOCI {
		t.Fatalf("cri transport type = %q, want %q", plan.Components[1].Transport.Type, TransportOCI)
	}
}

func TestResolveArtifactsConstraintViolation(t *testing.T) {
	policy := resolverPolicy()
	policy.Spec.Policies[0].Constraints = []VersionRelation{
		{
			When: RelationSelector{Kind: "cri", Name: "containerd", Version: "2.1.0"},
			Forbids: []RelationSelector{
				{Kind: "cni", Name: "calico", Version: "v3.30.0"},
			},
		},
	}
	_, err := ResolveArtifacts(resolverCatalog(), policy, ResolveRequest{KubernetesVersion: "v1.36.0", Arch: "amd64"})
	assertResolverError(t, err, ErrComponentConstraintViolation)
}

func TestResolveArtifactsRejectsDuplicateResolvedComponent(t *testing.T) {
	policy := resolverPolicy()
	policy.Spec.Policies[0].ComponentSlots = append(policy.Spec.Policies[0].ComponentSlots, ComponentSlotRule{
		Slot:      "network-plugin",
		Selection: SelectionOneOf,
		Required:  true,
		Default:   ComponentChoice{Name: "calico", Version: "v3.30.0"},
		Options: []ComponentOption{
			{Name: "calico", Kind: "cni", AllowedVersions: []string{"v3.30.0"}},
		},
	})

	_, err := ResolveArtifacts(resolverCatalog(), policy, ResolveRequest{
		KubernetesVersion: "v1.36.0",
		Arch:              "amd64",
	})
	assertResolverError(t, err, ErrDuplicateResolvedComponent)
}

func assertResolverError(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error code %s", code)
	}
	resolverErr, ok := err.(*ResolverError)
	if !ok {
		t.Fatalf("error type = %T, want *ResolverError: %v", err, err)
	}
	if resolverErr.Code != code {
		t.Fatalf("error code = %s, want %s: %v", resolverErr.Code, code, err)
	}
}

func resolverCatalog() *PackageInventory {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		resolverPackage("k8s", "k8s", "v1.36.0", "amd64", ContentProfileK8s),
		resolverPackage("containerd", "cri", "2.1.0", "amd64", ContentProfileRuntime),
		resolverPackage("containerd", "cri", "2.1.1", "amd64", ContentProfileRuntime),
		resolverPackage("containerd", "cri", "2.1.0", "s390x", ContentProfileRuntime),
		resolverPackage("calico", "cni", "v3.30.0", "amd64", ContentProfileAddon),
	}
	return catalog
}

func resolverPackage(name, kind, version, arch, profile string) PackageEntry {
	return PackageEntry{
		Name:           name,
		Kind:           kind,
		Version:        version,
		OS:             DefaultPackageOS,
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

func resolverPolicy() *SupportPolicy {
	policy := NewSupportPolicy("default")
	policy.Spec.Policies = []KubernetesSupportPolicy{
		{
			Name:  "k8s-v1.36-stable",
			Match: PolicyMatch{KubernetesVersion: "v1.36.*"},
			ComponentSlots: []ComponentSlotRule{
				{
					Slot:      "cri",
					Selection: SelectionOneOf,
					Required:  true,
					Default:   ComponentChoice{Name: "containerd", Version: "2.1.0"},
					Options: []ComponentOption{
						{Name: "containerd", Kind: "cri", AllowedVersions: []string{"2.1.0", "2.1.1"}},
					},
				},
				{
					Slot:      "cni",
					Selection: SelectionOneOf,
					Required:  true,
					Default:   ComponentChoice{Name: "calico", Version: "v3.30.0"},
					Options: []ComponentOption{
						{Name: "calico", Kind: "cni", AllowedVersions: []string{"v3.30.0"}},
					},
				},
				{
					Slot:      "bootstrap-kubeclipper",
					Selection: SelectionOneOf,
					Required:  true,
					Default:   ComponentChoice{Name: "kubeclipper", Version: "v1.7.0"},
					Options: []ComponentOption{
						{Name: "kubeclipper", Kind: "bootstrap", AllowedVersions: []string{"v1.7.0"}},
					},
				},
				{
					Slot:      "extension",
					Selection: SelectionOneOf,
					Required:  true,
					Default:   ComponentChoice{Name: "kubectl-terminal", Version: "v1.0.0"},
					Options: []ComponentOption{
						{Name: "kubectl-terminal", Kind: "extension", AllowedVersions: []string{"v1.0.0"}},
					},
				},
			},
		},
	}
	return policy
}
