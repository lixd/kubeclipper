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

func TestSelectBestPackage(t *testing.T) {
	inventory := NewPackageInventory("default")
	inventory.Spec.Packages = []PackageEntry{
		selectorPackage("extension", "kubectl-terminal", "v1.0.0", "amd64", ContentProfileExtension),
		selectorPackage("extension", "kubectl-terminal", "v1.1.0", "arm64", ContentProfileExtension),
	}

	selected, err := SelectBestPackage(inventory, SelectPackageRequest{
		Version: "v1.0.0",
		Arch:    "amd64",
		Candidates: []PackageCandidate{
			{Kind: "extension", Name: "kubectl-terminal"},
		},
	})
	if err != nil {
		t.Fatalf("SelectBestPackage() error: %+v", err)
	}
	if selected.Version != "v1.0.0" || selected.Transport.Type != TransportOCI || selected.Transport.Digest == "" {
		t.Fatalf("selected = %+v", selected)
	}
}

func TestSelectBestPackageRejectsInvalidInventoryTransport(t *testing.T) {
	inventory := NewPackageInventory("default")
	inventory.Spec.Packages = []PackageEntry{
		selectorPackage("binary", "kubeclipper-agent", "v1.7.0", "amd64", ContentProfileBinary),
	}
	inventory.Spec.Packages[0].Transport.Type = "http"

	_, err := SelectBestPackage(inventory, SelectPackageRequest{
		Version: "v1.7.0",
		Arch:    "amd64",
		Candidates: []PackageCandidate{
			{Kind: "binary", Name: "kubeclipper-agent"},
		},
	})
	if err == nil {
		t.Fatalf("SelectBestPackage() expected invalid inventory transport error")
	}
}

func TestMatchUniquePackageSupportsWildcardKind(t *testing.T) {
	inventory := NewPackageInventory("default")
	inventory.Spec.Packages = []PackageEntry{
		selectorPackage("csi", "nfs", "v4.1.0", "amd64", ContentProfileAddon),
	}

	selected, err := MatchUniquePackage(inventory, MatchPackageRequest{
		Version: "v4.1.0",
		Arch:    "amd64",
		Candidates: []PackageCandidate{
			{Name: "nfs"},
		},
	})
	if err != nil {
		t.Fatalf("MatchUniquePackage() error: %+v", err)
	}
	if selected.Kind != "csi" || selected.Name != "nfs" || selected.Transport.Digest == "" {
		t.Fatalf("selected = %+v", selected)
	}
}

func TestMatchUniquePackageRejectsAmbiguousInventory(t *testing.T) {
	inventory := NewPackageInventory("default")
	inventory.Spec.Packages = []PackageEntry{
		selectorPackage("csi", "nfs", "v4.1.0", "amd64", ContentProfileAddon),
		selectorPackage("addon", "nfs", "v4.1.0", "amd64", ContentProfileAddon),
	}

	_, err := MatchUniquePackage(inventory, MatchPackageRequest{
		Version: "v4.1.0",
		Arch:    "amd64",
		Candidates: []PackageCandidate{
			{Name: "nfs"},
		},
	})
	if err == nil {
		t.Fatalf("MatchUniquePackage() expected multiple matches error")
	}
}

func selectorPackage(kind, name, version, arch, profile string) PackageEntry {
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
