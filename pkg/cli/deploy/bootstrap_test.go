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
	"fmt"
	"reflect"
	"testing"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"
)

func TestResolveBootstrapAssetComponentsReportsMissing(t *testing.T) {
	inventory := deliveryapis.NewPackageInventory("registry")
	inventory.Spec.Packages = []deliveryapis.PackageEntry{
		bootstrapPackage("kubeclipper-agent", "v1.0.0"),
	}

	components, missing := resolveBootstrapAssetComponents(inventory, []bootstrapAsset{
		{PackageName: "kubeclipper", Name: "kubeclipper-agent"},
		{PackageName: "kubeclipper", Name: "kubeclipper-server"},
	}, "amd64", "rev-a")

	if len(components) != 0 {
		t.Fatalf("components = %#v", components)
	}
	if len(missing) != 2 || missing[0] != "bootstrap/kubeclipper:kubeclipper-agent" || missing[1] != "bootstrap/kubeclipper:kubeclipper-server" {
		t.Fatalf("missing = %#v", missing)
	}
}

func TestSelectBootstrapPackageUsesSourceRevision(t *testing.T) {
	inventory := deliveryapis.NewPackageInventory("registry")
	oldPackage := bootstrapPackage("kubeclipper-agent", "v1.0.0")
	oldPackage.SourceRevision = "rev-old"
	wantedPackage := bootstrapPackage("kubeclipper-agent", "v1.1.0")
	wantedPackage.SourceRevision = "rev-wanted"
	newPackage := bootstrapPackage("kubeclipper-agent", "v1.2.0")
	newPackage.SourceRevision = "rev-new"
	inventory.Spec.Packages = []deliveryapis.PackageEntry{oldPackage, wantedPackage, newPackage}

	pkg, ok := selectBootstrapPackage(inventory, "kubeclipper", []bootstrapAsset{{PackageName: "kubeclipper", Name: "kubeclipper-agent"}}, "amd64", "rev-wanted")
	if !ok {
		t.Fatal("selectBootstrapPackage() ok = false")
	}
	if pkg.Version != "v1.1.0" {
		t.Fatalf("selected version = %q, want v1.1.0", pkg.Version)
	}
}

func TestSelectBootstrapPackageRejectsMissingSourceRevision(t *testing.T) {
	inventory := deliveryapis.NewPackageInventory("registry")
	inventory.Spec.Packages = []deliveryapis.PackageEntry{bootstrapPackage("kubeclipper-agent", "v1.2.0")}

	if _, ok := selectBootstrapPackage(inventory, "kubeclipper", []bootstrapAsset{{PackageName: "kubeclipper", Name: "kubeclipper-agent"}}, "amd64", "rev-wanted"); ok {
		t.Fatal("selectBootstrapPackage() unexpectedly selected package without matching source revision")
	}
}

func TestSelectBootstrapPackageUsesPinnedDependencyVersion(t *testing.T) {
	inventory := deliveryapis.NewPackageInventory("registry")
	oldPackage := bootstrapPackage("etcd", "3.5.20")
	oldPackage.Name = "etcd"
	wantedPackage := bootstrapPackage("etcd", bootstrapEtcdVersion)
	wantedPackage.Name = "etcd"
	newPackage := bootstrapPackage("etcd", "3.6.0")
	newPackage.Name = "etcd"
	inventory.Spec.Packages = []deliveryapis.PackageEntry{oldPackage, wantedPackage, newPackage}

	pkg, ok := selectBootstrapPackage(inventory, "etcd", []bootstrapAsset{{PackageName: "etcd", Name: "etcd"}}, "amd64", "rev-wanted")
	if !ok {
		t.Fatal("selectBootstrapPackage() ok = false")
	}
	if pkg.Version != bootstrapEtcdVersion {
		t.Fatalf("selected version = %q, want %s", pkg.Version, bootstrapEtcdVersion)
	}
}

func TestDeployBootstrapAssetsExcludeRegistry(t *testing.T) {
	for _, asset := range deployBootstrapAssets {
		if asset.PackageName == "registry" || asset.Name == "registry" {
			t.Fatalf("kcctl deploy must not fetch registry asset: %+v", asset)
		}
	}
}

func TestBootstrapPackageRepositoriesAreExplicitAndDeduplicated(t *testing.T) {
	got := bootstrapPackageRepositories(deployBootstrapAssets)
	want := []string{
		"kubeclipper/packages/bootstrap/kubeclipper",
		"kubeclipper/packages/bootstrap/etcd",
		"kubeclipper/packages/bootstrap/console",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("bootstrapPackageRepositories() = %#v, want %#v", got, want)
	}
}

func TestNormalizeBootstrapArchitecture(t *testing.T) {
	for input, want := range map[string]string{
		"x86_64\n": "amd64",
		"amd64":    "amd64",
		"aarch64":  "arm64",
		"arm64\n":  "arm64",
	} {
		got, err := normalizeBootstrapArchitecture(input)
		if err != nil || got != want {
			t.Fatalf("normalizeBootstrapArchitecture(%q) = %q, %v; want %q", input, got, err, want)
		}
	}
	if _, err := normalizeBootstrapArchitecture("s390x"); err == nil {
		t.Fatal("normalizeBootstrapArchitecture(s390x) unexpectedly succeeded")
	}
}

func TestGroupBootstrapHostsByArchitectureUsesTargetSSHRunner(t *testing.T) {
	machines := map[string]string{
		"node-a": "x86_64\n",
		"node-b": "aarch64\n",
		"node-c": "amd64\n",
	}
	runner := func(_ *sshutils.SSH, host, command string) (sshutils.Result, error) {
		if command != "uname -m" {
			return sshutils.Result{}, fmt.Errorf("unexpected command %q", command)
		}
		machine, ok := machines[host]
		if !ok {
			return sshutils.Result{}, fmt.Errorf("unexpected host %q", host)
		}
		return sshutils.Result{Stdout: machine}, nil
	}

	got, err := groupBootstrapHostsByArchitecture(runner, sshutils.NewSSH(), []string{"node-a", "node-b", "node-c"})
	if err != nil {
		t.Fatalf("groupBootstrapHostsByArchitecture() error: %v", err)
	}
	want := map[string][]string{"amd64": {"node-a", "node-c"}, "arm64": {"node-b"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("groups = %#v, want %#v", got, want)
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
