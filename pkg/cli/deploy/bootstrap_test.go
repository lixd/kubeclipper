/*
 *
 *  * Copyright 2026 KubeClipper Authors.
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

package deploy

import (
	"testing"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

const testBootstrapRevision = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestResolveBootstrapAssetComponentsReportsMissing(t *testing.T) {
	inventory := deliveryapis.NewPackageInventory("registry")
	inventory.Spec.Packages = []deliveryapis.PackageEntry{
		bootstrapPackage("kubeclipper-agent", "v1.0.0"),
	}

	components, missing := resolveBootstrapAssetComponents(inventory, []bootstrapAsset{
		{PackageName: "kubeclipper", Name: "kubeclipper-agent"},
		{PackageName: "kubeclipper", Name: "kubeclipper-server"},
	}, "amd64", testBootstrapRevision)

	if len(components) != 0 {
		t.Fatalf("components = %#v", components)
	}
	if len(missing) != 2 || missing[0] != "bootstrap/kubeclipper:kubeclipper-agent" || missing[1] != "bootstrap/kubeclipper:kubeclipper-server" {
		t.Fatalf("missing = %#v", missing)
	}
}

func TestSelectBootstrapPackageUsesNewestVersion(t *testing.T) {
	inventory := deliveryapis.NewPackageInventory("registry")
	inventory.Spec.Packages = []deliveryapis.PackageEntry{
		bootstrapPackage("kubeclipper-agent", "v1.0.0"),
		bootstrapPackage("kubeclipper-agent", "v1.2.0"),
	}

	pkg, ok := selectBootstrapPackage(inventory, "kubeclipper", []bootstrapAsset{{PackageName: "kubeclipper", Name: "kubeclipper-agent"}}, "amd64", testBootstrapRevision)
	if !ok {
		t.Fatal("selectBootstrapPackage() ok = false")
	}
	if pkg.Version != "v1.2.0" {
		t.Fatalf("selected version = %q, want v1.2.0", pkg.Version)
	}
}

func TestSelectBootstrapPackageUsesNewestPrereleaseNumber(t *testing.T) {
	inventory := deliveryapis.NewPackageInventory("registry")
	inventory.Spec.Packages = []deliveryapis.PackageEntry{
		bootstrapPackage("kubeclipper-agent", "v2.0.3-rc.9"),
		bootstrapPackage("kubeclipper-agent", "v2.0.3-rc.26"),
	}

	pkg, ok := selectBootstrapPackage(inventory, "kubeclipper", []bootstrapAsset{{PackageName: "kubeclipper", Name: "kubeclipper-agent"}}, "amd64", testBootstrapRevision)
	if !ok {
		t.Fatal("selectBootstrapPackage() ok = false")
	}
	if pkg.Version != "v2.0.3-rc.26" {
		t.Fatalf("selected version = %q, want v2.0.3-rc.26", pkg.Version)
	}
}

func TestSelectBootstrapPackageRequiresExactSourceRevision(t *testing.T) {
	inventory := deliveryapis.NewPackageInventory("registry")
	legacy := bootstrapPackage("kubeclipper-agent", "v2.0.3-rc.5-1")
	legacy.SourceRevision = ""
	current := bootstrapPackage("kubeclipper-agent", "v2.0.3-rc.29")
	current.SourceRevision = "cff7e1a075ae6563a1da5202b4bf502e313ac49a"
	newer := bootstrapPackage("kubeclipper-agent", "v2.0.3-rc.30")
	newer.SourceRevision = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if cmp, ok := deliveryapis.CompareVersions(legacy.Version, current.Version); !ok || cmp <= 0 {
		t.Fatalf("fixture must put legacy tag %q above %q", legacy.Version, current.Version)
	}
	inventory.Spec.Packages = []deliveryapis.PackageEntry{newer, current, legacy}

	pkg, ok := selectBootstrapPackage(inventory, "kubeclipper", []bootstrapAsset{{PackageName: "kubeclipper", Name: "kubeclipper-agent"}}, "amd64", current.SourceRevision)
	if !ok {
		t.Fatal("selectBootstrapPackage() ok = false")
	}
	if pkg.Version != current.Version {
		t.Fatalf("selected version = %q, want exact-revision version %q", pkg.Version, current.Version)
	}

	if pkg, ok := selectBootstrapPackage(inventory, "kubeclipper", []bootstrapAsset{{PackageName: "kubeclipper", Name: "kubeclipper-agent"}}, "amd64", "cccccccccccccccccccccccccccccccccccccccc"); ok {
		t.Fatalf("selected package %#v without an exact source revision match", pkg)
	}
}

func TestSelectBootstrapPackageDoesNotPinIndependentPackageRevision(t *testing.T) {
	inventory := deliveryapis.NewPackageInventory("registry")
	older := bootstrapPackage("etcd", "v3.5.20")
	older.Name = "etcd"
	older.SourceRevision = "etcd-source-revision"
	newer := bootstrapPackage("etcd", "v3.5.21")
	newer.Name = "etcd"
	newer.SourceRevision = "another-etcd-source-revision"
	inventory.Spec.Packages = []deliveryapis.PackageEntry{older, newer}

	pkg, ok := selectBootstrapPackage(inventory, "etcd", []bootstrapAsset{{PackageName: "etcd", Name: "etcd"}}, "amd64", testBootstrapRevision)
	if !ok {
		t.Fatal("selectBootstrapPackage() ok = false")
	}
	if pkg.Version != newer.Version {
		t.Fatalf("selected version = %q, want newest independent package %q", pkg.Version, newer.Version)
	}
}

func bootstrapPackage(name, version string) deliveryapis.PackageEntry {
	return deliveryapis.PackageEntry{
		Kind:           bootstrapKind,
		Name:           "kubeclipper",
		Version:        version,
		OS:             deliveryapis.DefaultPackageOS,
		Arch:           "amd64",
		ContentProfile: deliveryapis.ContentProfileBinary,
		SourceRevision: testBootstrapRevision,
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    "registry.local:5000/kubeclipper/packages/bootstrap/kubeclipper:" + version,
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
		Contents: []deliveryapis.ArtifactContent{{
			Name:      name,
			File:      name,
			MediaType: deliveryapis.MediaTypeBinaryLayer,
		}},
	}
}
