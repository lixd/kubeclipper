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

func TestPackageInventoryValidate(t *testing.T) {
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
				Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			},
			Contents: []ArtifactContent{
				{Name: ContentConfigs, File: "configs.tar.gz"},
				{Name: ContentImages, File: "images.tar.gz"},
				{Name: ContentCharts, File: "charts.tgz"},
			},
		},
	}
	if err := catalog.Validate(); err != nil {
		t.Fatalf("Validate() error: %+v", err)
	}
}

func TestPackageInventoryValidateRejectsDuplicatePackageIdentity(t *testing.T) {
	catalog := NewPackageInventory("default")
	pkg := PackageEntry{
		Name:           "containerd",
		Kind:           "cri",
		Version:        "2.1.0",
		Arch:           "amd64",
		ContentProfile: ContentProfileRuntime,
		Transport: TransportRef{
			Type:   TransportOCI,
			Ref:    "registry.local/kubeclipper/packages/cri/containerd:2.1.0",
			Digest: testDigest,
		},
		Contents: []ArtifactContent{
			{Name: ContentConfigs, File: "configs.tar.gz"},
			{Name: ContentImages, File: "images.tar.gz"},
		},
	}
	catalog.Spec.Packages = []PackageEntry{pkg, pkg}
	if err := catalog.Validate(); err == nil {
		t.Fatalf("Validate() expected duplicate package identity error")
	}
}

func TestPackageInventoryValidateTreatsOSAsPackageIdentity(t *testing.T) {
	catalog := NewPackageInventory("default")
	base := PackageEntry{
		Name:           "containerd",
		Kind:           "cri",
		Version:        "2.1.0",
		OS:             "linux",
		Arch:           "amd64",
		ContentProfile: ContentProfileRuntime,
		Transport: TransportRef{
			Type:   TransportOCI,
			Ref:    "registry.local/kubeclipper/packages/cri/containerd:2.1.0",
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
		Contents: []ArtifactContent{
			{Name: ContentConfigs, File: "configs.tar.gz"},
			{Name: ContentImages, File: "images.tar.gz"},
		},
	}
	otherOS := base
	otherOS.OS = "darwin"
	otherOS.Transport.Digest = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
	catalog.Spec.Packages = []PackageEntry{base, otherOS}
	if err := catalog.Validate(); err != nil {
		t.Fatalf("Validate() error: %+v", err)
	}
}

func TestPackageInventoryValidateRejectsDuplicatePackageIdentityWithDifferentDigests(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
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
			Contents: []ArtifactContent{
				{Name: ContentConfigs, File: "configs.tar.gz", MediaType: MediaTypeConfigsLayer},
				{Name: ContentImages, File: "images.tar.gz", MediaType: MediaTypeImagesLayer},
			},
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
				Digest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
			},
			Contents: []ArtifactContent{
				{Name: ContentConfigs, File: "configs.tar.gz", MediaType: MediaTypeConfigsLayer},
				{Name: ContentImages, File: "images.tar.gz", MediaType: MediaTypeImagesLayer},
			},
		},
	}

	if err := catalog.Validate(); err == nil {
		t.Fatalf("Validate() expected duplicate package identity error")
	}
}

func TestPackageInventoryValidateRejectsDuplicatePackageIdentityWithSameDigest(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		{
			Name:           "containerd",
			Kind:           "cri",
			Version:        "2.1.0",
			Arch:           "amd64",
			ContentProfile: ContentProfileRuntime,
			Transport: TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/cri/containerd:2.1.0",
				Digest: testDigest,
			},
			Contents: []ArtifactContent{
				{Name: ContentConfigs, File: "configs.tar.gz"},
				{Name: ContentImages, File: "images.tar.gz"},
			},
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
				Digest: testDigest,
			},
			Contents: []ArtifactContent{
				{Name: ContentConfigs, File: "configs.tar.gz"},
				{Name: ContentImages, File: "images.tar.gz"},
			},
		},
	}

	if err := catalog.Validate(); err == nil {
		t.Fatalf("Validate() expected duplicate package error")
	}
}

func TestPackageInventoryValidateContentProfile(t *testing.T) {
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
			Contents: []ArtifactContent{
				{Name: ContentConfigs, File: "configs.tar.gz"},
			},
		},
	}
	if err := catalog.Validate(); err != nil {
		t.Fatalf("Validate() error: %+v", err)
	}
}

func TestPackageInventoryValidateRejectsDuplicateContents(t *testing.T) {
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
			Contents: []ArtifactContent{
				{Name: ContentConfigs, File: "configs.tar.gz"},
				{Name: ContentConfigs, File: "configs-copy.tar.gz"},
				{Name: ContentImages, File: "images.tar.gz"},
			},
		},
	}
	if err := catalog.Validate(); err == nil {
		t.Fatalf("Validate() expected duplicate content error")
	}
}

func TestPackageInventoryValidateRejectsNonOCITransport(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		{
			Name:           "containerd",
			Kind:           "cri",
			Version:        "2.1.0",
			Arch:           "amd64",
			ContentProfile: ContentProfileRuntime,
			Transport: TransportRef{
				Type: "http",
				Ref:  "http://127.0.0.1/containerd",
			},
			Contents: []ArtifactContent{
				{Name: ContentConfigs, File: "configs.tar.gz"},
				{Name: ContentImages, File: "images.tar.gz"},
			},
		},
	}
	if err := catalog.Validate(); err == nil {
		t.Fatalf("Validate() expected unsupported transport error")
	}
}

func TestPackageInventoryValidateRejectsLatestVersion(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		{
			Name:           "containerd",
			Kind:           "cri",
			Version:        "latest",
			Arch:           "amd64",
			ContentProfile: ContentProfileRuntime,
			Transport: TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/cri/containerd:latest",
				Digest: testDigest,
			},
			Contents: []ArtifactContent{
				{Name: ContentConfigs, File: "configs.tar.gz"},
				{Name: ContentImages, File: "images.tar.gz"},
			},
		},
	}
	if err := catalog.Validate(); err == nil {
		t.Fatalf("Validate() expected latest version error")
	}
}

func TestPackageInventoryValidateAllowsLatestKubeClipperBootstrap(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		{
			Name:           "kubeclipper",
			Kind:           "bootstrap",
			Version:        "latest",
			Arch:           "amd64",
			ContentProfile: ContentProfileBinary,
			Transport: TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/bootstrap/kubeclipper:latest",
				Digest: testDigest,
			},
			Contents: []ArtifactContent{
				{Name: "kubeclipper-server", File: "kubeclipper-server", Digest: testDigest},
				{Name: "kubeclipper-agent", File: "kubeclipper-agent", Digest: testDigest},
			},
		},
	}
	if err := catalog.Validate(); err != nil {
		t.Fatalf("Validate() error = %+v", err)
	}
}

func TestPackageInventoryValidateRejectsMismatchedPinnedRefDigest(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		{
			Name:           "containerd",
			Kind:           "cri",
			Version:        "2.1.0",
			Arch:           "amd64",
			ContentProfile: ContentProfileRuntime,
			Transport: TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/cri/containerd:2.1.0@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			},
			Contents: []ArtifactContent{
				{Name: ContentConfigs, File: "configs.tar.gz"},
				{Name: ContentImages, File: "images.tar.gz"},
			},
		},
	}
	if err := catalog.Validate(); err == nil {
		t.Fatalf("Validate() expected mismatched pinned ref digest error")
	}
}

func TestPackageInventoryValidateAddonProfileAllowsConfigsImagesOrCharts(t *testing.T) {
	catalog := NewPackageInventory("default")
	base := PackageEntry{
		Name:           "metallb",
		Kind:           "app",
		Version:        "v0.14.9",
		Arch:           "amd64",
		ContentProfile: ContentProfileAddon,
		Transport: TransportRef{
			Type:   TransportOCI,
			Ref:    "registry.local/kubeclipper/packages/app/metallb:v0.14.9",
			Digest: testDigest,
		},
	}
	catalog.Spec.Packages = []PackageEntry{{
		Name:           base.Name,
		Kind:           base.Kind,
		Version:        base.Version,
		Arch:           base.Arch,
		ContentProfile: base.ContentProfile,
		Transport:      base.Transport,
		Contents:       []ArtifactContent{{Name: ContentCharts, File: "charts.tgz"}},
	}}
	if err := catalog.Validate(); err != nil {
		t.Fatalf("Validate() chart-only addon error: %+v", err)
	}

	catalog.Spec.Packages[0].Contents = []ArtifactContent{{Name: ContentConfigs, File: "configs.tar.gz"}}
	if err := catalog.Validate(); err != nil {
		t.Fatalf("Validate() config-only addon error: %+v", err)
	}

	catalog.Spec.Packages[0].Contents = []ArtifactContent{{
		Name:      ContentCharts,
		File:      "tigera-operator-v3.31.5.tgz",
		MediaType: MediaTypeChartsLayer,
		Transport: TransportRef{
			Type:   TransportHelmOCI,
			Ref:    "registry.local/kubeclipper/charts/tigera-operator",
			Digest: testDigest,
		},
	}}
	if err := catalog.Validate(); err != nil {
		t.Fatalf("Validate() external helm chart addon error: %+v", err)
	}

	catalog.Spec.Packages[0].Contents = nil
	if err := catalog.Validate(); err == nil {
		t.Fatalf("Validate() expected addon without configs, images or charts error")
	}
}

func TestContentProfileForKindExtension(t *testing.T) {
	if got := ContentProfileForKind("extension"); got != ContentProfileExtension {
		t.Fatalf("ContentProfileForKind(extension) = %q, want %q", got, ContentProfileExtension)
	}
}

func TestPackageInventoryValidateBinaryProfile(t *testing.T) {
	catalog := NewPackageInventory("default")
	catalog.Spec.Packages = []PackageEntry{
		{
			Kind:           "bootstrap",
			Name:           "kubeclipper",
			Version:        "v1.7.0",
			Arch:           "amd64",
			ContentProfile: ContentProfileBinary,
			Transport: TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.local/kubeclipper/packages/bootstrap/kubeclipper:v1.7.0",
				Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			},
		},
	}

	if err := catalog.Validate(); err == nil {
		t.Fatalf("Validate() expected binary profile contents error")
	}

	catalog.Spec.Packages[0].Contents = []ArtifactContent{
		{Name: "kubeclipper-agent", File: "kubeclipper-agent", MediaType: MediaTypeBinaryLayer},
	}
	if err := catalog.Validate(); err != nil {
		t.Fatalf("Validate() error: %+v", err)
	}
}

func TestSupportPolicyValidateSlotDefault(t *testing.T) {
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
						{Name: "containerd", Kind: "cri", AllowedVersions: []string{"2.1.0", "2.1.1"}},
					},
				},
			},
		},
	}
	if err := policy.Validate(); err != nil {
		t.Fatalf("Validate() error: %+v", err)
	}

	policy.Spec.Policies[0].ComponentSlots[0].Default.Version = "2.0.0"
	if err := policy.Validate(); err == nil {
		t.Fatalf("Validate() expected default version error")
	}
}

func TestSupportPolicyValidateRejectsDuplicatePolicyName(t *testing.T) {
	policy := validSupportPolicyForValidation()
	policy.Spec.Policies = append(policy.Spec.Policies, policy.Spec.Policies[0])
	if err := policy.Validate(); err == nil {
		t.Fatalf("Validate() expected duplicate policy error")
	}
}

func TestSupportPolicyValidateRejectsDuplicateOptionName(t *testing.T) {
	policy := validSupportPolicyForValidation()
	policy.Spec.Policies[0].ComponentSlots[0].Options = append(policy.Spec.Policies[0].ComponentSlots[0].Options, ComponentOption{
		Name:            "containerd",
		Kind:            "cri",
		AllowedVersions: []string{"2.1.1"},
	})
	if err := policy.Validate(); err == nil {
		t.Fatalf("Validate() expected duplicate option error")
	}
}

func TestSupportPolicyValidateRejectsInvalidAllowedVersions(t *testing.T) {
	for _, tt := range []struct {
		name            string
		allowedVersions []string
	}{
		{name: "empty", allowedVersions: []string{""}},
		{name: "duplicate", allowedVersions: []string{"2.1.0", "2.1.0"}},
		{name: "latest", allowedVersions: []string{"latest"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := validSupportPolicyForValidation()
			policy.Spec.Policies[0].ComponentSlots[0].Options[0].AllowedVersions = tt.allowedVersions
			if err := policy.Validate(); err == nil {
				t.Fatalf("Validate() expected allowedVersions error")
			}
		})
	}
}

func TestSupportPolicyValidateRejectsUnsupportedSelectionMany(t *testing.T) {
	policy := validSupportPolicyForValidation()
	policy.Spec.Policies[0].ComponentSlots[0].Selection = SelectionMany
	if err := policy.Validate(); err == nil {
		t.Fatalf("Validate() expected unsupported many selection error")
	}
}

func TestSupportPolicyValidateRejectsRequiredZeroOrOneSelection(t *testing.T) {
	policy := validSupportPolicyForValidation()
	policy.Spec.Policies[0].ComponentSlots[0].Selection = SelectionZeroOrOne
	policy.Spec.Policies[0].ComponentSlots[0].Required = true
	if err := policy.Validate(); err == nil {
		t.Fatalf("Validate() expected required zeroOrOne error")
	}
}

func TestSupportPolicyValidateAllowsDisjointKubeClipperMatches(t *testing.T) {
	policy := validSupportPolicyForValidation()
	other := policy.Spec.Policies[0]
	other.Name = "k8s-v1.36-kc-v1.8"
	other.Match.KubeClipperVersion = "v1.8.0"
	policy.Spec.Policies[0].Match.KubeClipperVersion = "v1.7.0"
	policy.Spec.Policies = append(policy.Spec.Policies, other)
	if err := policy.Validate(); err != nil {
		t.Fatalf("Validate() error: %+v", err)
	}
}

func TestSupportPolicyValidateRejectsOverlappingPolicyMatches(t *testing.T) {
	for _, tt := range []struct {
		name  string
		match PolicyMatch
	}{
		{
			name:  "same kubernetes",
			match: PolicyMatch{KubernetesVersion: "v1.36.*"},
		},
		{
			name:  "generic overlaps specific kubeclipper",
			match: PolicyMatch{KubernetesVersion: "v1.36.*", KubeClipperVersion: "v1.8.0"},
		},
		{
			name:  "prefix overlaps exact",
			match: PolicyMatch{KubernetesVersion: "v1.36.1"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			policy := validSupportPolicyForValidation()
			other := policy.Spec.Policies[0]
			other.Name = "overlap"
			other.Match = tt.match
			policy.Spec.Policies = append(policy.Spec.Policies, other)
			if err := policy.Validate(); err == nil {
				t.Fatalf("Validate() expected overlapping policy error")
			}
		})
	}
}

func TestSupportPolicyValidateRejectsInvalidKubernetesVersionWildcard(t *testing.T) {
	for _, pattern := range []string{"v1.*.0", "v1.*.*"} {
		t.Run(pattern, func(t *testing.T) {
			policy := validSupportPolicyForValidation()
			policy.Spec.Policies[0].Match.KubernetesVersion = pattern
			if err := policy.Validate(); err == nil {
				t.Fatalf("Validate() expected wildcard error")
			}
		})
	}
}

func TestSupportPolicyValidateRejectsCompatibilityPolicyKind(t *testing.T) {
	policy := NewSupportPolicy("default")
	policy.Kind = "CompatibilityPolicy"
	if err := policy.Validate(); err == nil {
		t.Fatalf("Validate() expected kind error")
	}
}

func validSupportPolicyForValidation() *SupportPolicy {
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
			},
		},
	}
	return policy
}

func TestCompareVersions(t *testing.T) {
	for _, tt := range []struct {
		name  string
		left  string
		right string
		want  int
		ok    bool
	}{
		{name: "greater", left: "v3.30.0", right: "v3.29.1", want: 1, ok: true},
		{name: "less", left: "1.6.3", right: "1.6.4", want: -1, ok: true},
		{name: "equal", left: "v1.36.0", right: "1.36.0", want: 0, ok: true},
		{name: "invalid", left: "latest", right: "1.0.0", want: 0, ok: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := CompareVersions(tt.left, tt.right)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("CompareVersions() = (%d, %v), want (%d, %v)", got, ok, tt.want, tt.ok)
			}
		})
	}
}
