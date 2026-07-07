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

func TestResolveBootstrapAssetComponentsReportsMissing(t *testing.T) {
	inventory := deliveryapis.NewPackageInventory("registry")
	inventory.Spec.Packages = []deliveryapis.PackageEntry{
		bootstrapPackage("kubeclipper-agent", "v1.0.0"),
	}

	components, missing := resolveBootstrapAssetComponents(inventory, []bootstrapAsset{
		{Name: "kubeclipper-agent"},
		{Name: "kubeclipper-server"},
	}, "amd64")

	if len(components) != 1 || components[0].Name != "kubeclipper-agent" {
		t.Fatalf("components = %#v", components)
	}
	if len(missing) != 1 || missing[0] != "binary/kubeclipper-server" {
		t.Fatalf("missing = %#v", missing)
	}
}

func TestSelectBootstrapPackageUsesNewestVersion(t *testing.T) {
	inventory := deliveryapis.NewPackageInventory("registry")
	inventory.Spec.Packages = []deliveryapis.PackageEntry{
		bootstrapPackage("kubeclipper-agent", "v1.0.0"),
		bootstrapPackage("kubeclipper-agent", "v1.2.0"),
	}

	pkg, ok := selectBootstrapPackage(inventory, "kubeclipper-agent", "amd64")
	if !ok {
		t.Fatal("selectBootstrapPackage() ok = false")
	}
	if pkg.Version != "v1.2.0" {
		t.Fatalf("selected version = %q, want v1.2.0", pkg.Version)
	}
}

func bootstrapPackage(name, version string) deliveryapis.PackageEntry {
	return deliveryapis.PackageEntry{
		Kind:           bootstrapKind,
		Name:           name,
		Version:        version,
		OS:             deliveryapis.DefaultPackageOS,
		Arch:           "amd64",
		ContentProfile: deliveryapis.ContentProfileBinary,
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    "registry.local:5000/kubeclipper/packages/binary/" + name + ":" + version,
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
		Contents: []deliveryapis.ArtifactContent{{
			Name:      deliveryapis.ContentBinary,
			File:      name,
			MediaType: deliveryapis.MediaTypeBinaryLayer,
		}},
	}
}
