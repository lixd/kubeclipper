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

package resource

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/printer"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/spf13/cobra"
)

func TestNewCmdResourceOnlyExposesInventoryCommands(t *testing.T) {
	cmd := NewCmdResource(options.IOStreams{Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}})
	subcommands := map[string]bool{}
	for _, sub := range cmd.Commands() {
		subcommands[sub.Name()] = true
	}
	if !subcommands["list"] || !subcommands["inspect"] || !subcommands["refresh"] {
		t.Fatalf("resource subcommands = %+v, want list, inspect and refresh", subcommands)
	}
	for _, removed := range []string{"push", "delete"} {
		if subcommands[removed] {
			t.Fatalf("resource subcommands = %+v, should not expose %q", subcommands, removed)
		}
	}
}

func TestResourceCommandDoesNotExposeTransportFlag(t *testing.T) {
	cmd := NewCmdResource(options.IOStreams{Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}})
	for _, sub := range cmd.Commands() {
		if sub.Flags().Lookup("transport") != nil {
			t.Fatalf("%s should not expose legacy --transport flag", sub.Name())
		}
	}
}

func TestResourceOptionsValidateArgsListRequiresRegistry(t *testing.T) {
	o := ResourceOptions{}
	if err := o.ValidateArgsList(&cobra.Command{}); err == nil {
		t.Fatalf("ValidateArgs() expected registry error")
	}
	o.Registry = "registry.local:5000"
	if err := o.ValidateArgsList(&cobra.Command{}); err != nil {
		t.Fatalf("ValidateArgsList() error: %+v", err)
	}
}

func TestResourceListFromRegistry(t *testing.T) {
	var out bytes.Buffer
	o := &ResourceOptions{
		IOStreams:  options.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}},
		PrintFlags: printer.NewPrintFlags(),
		Registry:   "registry.local:5000",
		indexer: &fakeResourceIndexer{
			catalog: &deliveryapis.PackageInventory{
				Spec: deliveryapis.PackageInventorySpec{
					Packages: []deliveryapis.PackageEntry{
						{
							Kind:    "cri",
							Name:    "containerd",
							Version: "2.1.0",
							Arch:    "amd64",
							Transport: deliveryapis.TransportRef{
								Type:   deliveryapis.TransportOCI,
								Ref:    "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0",
								Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
							},
							Contents: []deliveryapis.ArtifactContent{
								{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"},
							},
						},
					},
				},
			},
		},
	}
	if err := o.ResourceListFromRegistry(); err != nil {
		t.Fatalf("ResourceListFromRegistry() error: %+v", err)
	}
	if !strings.Contains(out.String(), "containerd") {
		t.Fatalf("output %q does not contain package name", out.String())
	}
}

func TestResourceListFromRegistryRefreshesWhenRequested(t *testing.T) {
	var out bytes.Buffer
	indexer := &fakeResourceIndexer{
		catalog: &deliveryapis.PackageInventory{
			Spec: deliveryapis.PackageInventorySpec{
				Packages: []deliveryapis.PackageEntry{{
					Kind:    "cri",
					Name:    "containerd",
					Version: "2.1.0",
					Arch:    "amd64",
					Transport: deliveryapis.TransportRef{
						Type: deliveryapis.TransportOCI,
						Ref:  "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0",
					},
				}},
			},
		},
	}
	o := &ResourceOptions{
		IOStreams:  options.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}},
		PrintFlags: printer.NewPrintFlags(),
		Registry:   "registry.local:5000",
		Refresh:    true,
		indexer:    indexer,
	}
	if err := o.ResourceListFromRegistry(); err != nil {
		t.Fatalf("ResourceListFromRegistry() error: %+v", err)
	}
	if indexer.refreshCalls != 1 {
		t.Fatalf("refreshCalls = %d, want 1", indexer.refreshCalls)
	}
	if indexer.indexCalls != 0 {
		t.Fatalf("indexCalls = %d, want 0", indexer.indexCalls)
	}
}

func TestResourceInspect(t *testing.T) {
	var out bytes.Buffer
	o := &ResourceOptions{
		IOStreams:  options.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}},
		PrintFlags: printer.NewPrintFlags(),
		Name:       "containerd",
		Version:    "2.1.0",
		Registry:   "registry.local:5000",
		indexer: &fakeResourceIndexer{
			catalog: &deliveryapis.PackageInventory{
				Spec: deliveryapis.PackageInventorySpec{
					Packages: []deliveryapis.PackageEntry{
						{
							Kind:    "cri",
							Name:    "containerd",
							Version: "2.1.0",
							Arch:    "amd64",
							Transport: deliveryapis.TransportRef{
								Type:   deliveryapis.TransportOCI,
								Ref:    "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0",
								Digest: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
							},
							Contents: []deliveryapis.ArtifactContent{
								{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"},
							},
						},
					},
				},
			},
		},
	}
	if err := o.ResourceInspect(); err != nil {
		t.Fatalf("ResourceInspect() error: %+v", err)
	}
	if !strings.Contains(out.String(), "containerd") || !strings.Contains(out.String(), "configs") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestResourceInspectNotFound(t *testing.T) {
	o := &ResourceOptions{
		Name:     "containerd",
		Version:  "2.1.0",
		Registry: "registry.local:5000",
		indexer: &fakeResourceIndexer{
			catalog: deliveryapis.NewPackageInventory("empty"),
		},
	}
	if err := o.ResourceInspect(); err == nil {
		t.Fatalf("ResourceInspect() expected error")
	}
}

func TestResourceRefreshFromRegistry(t *testing.T) {
	var out bytes.Buffer
	indexer := &fakeResourceIndexer{
		catalog: &deliveryapis.PackageInventory{
			Spec: deliveryapis.PackageInventorySpec{
				Packages: []deliveryapis.PackageEntry{{
					Kind:    "cri",
					Name:    "containerd",
					Version: "2.1.0",
					Arch:    "amd64",
					Transport: deliveryapis.TransportRef{
						Type:   deliveryapis.TransportOCI,
						Ref:    "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0",
						Digest: "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
					},
					Contents: []deliveryapis.ArtifactContent{
						{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"},
					},
				}},
			},
		},
	}
	o := &ResourceOptions{
		IOStreams: options.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}},
		Registry:  "registry.local:5000",
		indexer:   indexer,
	}
	if err := o.ResourceRefresh(); err != nil {
		t.Fatalf("ResourceRefresh() error: %+v", err)
	}
	if indexer.refreshCalls != 1 {
		t.Fatalf("refreshCalls = %d, want 1", indexer.refreshCalls)
	}
	if indexer.indexCalls != 0 {
		t.Fatalf("indexCalls = %d, want 0", indexer.indexCalls)
	}
	if !strings.Contains(out.String(), "refreshed 1 OCI packages from registry.local:5000") {
		t.Fatalf("output = %q", out.String())
	}
}

type fakeResourceIndexer struct {
	catalog      *deliveryapis.PackageInventory
	err          error
	indexCalls   int
	refreshCalls int
}

func (f *fakeResourceIndexer) Index(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.indexCalls++
	return f.catalog, nil
}

func (f *fakeResourceIndexer) Refresh(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.refreshCalls++
	return f.catalog, nil
}

func TestResourceListUsesRegistryInventory(t *testing.T) {
	o := &ResourceOptions{
		Registry: "registry.local:5000",
		indexer: &fakeResourceIndexer{
			catalog: &deliveryapis.PackageInventory{
				Spec: deliveryapis.PackageInventorySpec{
					Packages: []deliveryapis.PackageEntry{
						{
							Kind:    "cri",
							Name:    "containerd",
							Version: "2.1.0",
							Arch:    "amd64",
							Transport: deliveryapis.TransportRef{
								Type:   deliveryapis.TransportOCI,
								Ref:    "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0",
								Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
							},
							Contents: []deliveryapis.ArtifactContent{
								{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"},
							},
						},
						{
							Kind:    "cni",
							Name:    "calico",
							Version: "v3.30.0",
							Arch:    "arm64",
							Transport: deliveryapis.TransportRef{
								Type:   deliveryapis.TransportOCI,
								Ref:    "registry.local:5000/kubeclipper/packages/cni/calico:v3.30.0",
								Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
							},
							Contents: []deliveryapis.ArtifactContent{
								{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"},
							},
						},
					},
				},
			},
		},
	}

	resources, err := o.resourceList()
	if err != nil {
		t.Fatalf("resourceList() error: %+v", err)
	}
	if len(resources) != 2 {
		t.Fatalf("resource count = %d, want 2", len(resources))
	}
	if resources[0].Name != "containerd" || resources[1].Name != "calico" {
		t.Fatalf("resources = %+v", resources)
	}
}

func TestResourceListFiltersRegistryResults(t *testing.T) {
	o := &ResourceOptions{
		Registry: "registry.local:5000",
		Type:     "cri",
		Arch:     "amd64",
		indexer: &fakeResourceIndexer{
			catalog: &deliveryapis.PackageInventory{
				Spec: deliveryapis.PackageInventorySpec{
					Packages: []deliveryapis.PackageEntry{
						{
							Kind:    "cri",
							Name:    "containerd",
							Version: "2.1.0",
							Arch:    "amd64",
							Transport: deliveryapis.TransportRef{
								Type:   deliveryapis.TransportOCI,
								Ref:    "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0",
								Digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
							},
							Contents: []deliveryapis.ArtifactContent{
								{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"},
							},
						},
						{
							Kind:    "cni",
							Name:    "calico",
							Version: "v3.30.0",
							Arch:    "amd64",
							Transport: deliveryapis.TransportRef{
								Type:   deliveryapis.TransportOCI,
								Ref:    "registry.local:5000/kubeclipper/packages/cni/calico:v3.30.0",
								Digest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
							},
							Contents: []deliveryapis.ArtifactContent{
								{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"},
							},
						},
					},
				},
			},
		},
	}

	names := o.listName("")
	if len(names) != 1 || names[0] != "containerd" {
		t.Fatalf("listName() = %+v, want [containerd]", names)
	}
	versions := o.listVersion("")
	if len(versions) != 1 || versions[0] != "2.1.0" {
		t.Fatalf("listVersion() = %+v, want [2.1.0]", versions)
	}
	archs := o.listArch("")
	if len(archs) != 1 || archs[0] != "amd64" {
		t.Fatalf("listArch() = %+v, want [amd64]", archs)
	}
	types := o.listType("")
	if len(types) != 1 || types[0] != "cri" {
		t.Fatalf("listType() = %+v, want [cri]", types)
	}
}

func TestComponentMetasFromInventoryKeepsOCIPackageDespiteDeletedState(t *testing.T) {
	o := &ResourceOptions{}
	catalog := deliveryapis.NewPackageInventory("registry")
	catalog.Spec.Packages = []deliveryapis.PackageEntry{
		{
			Kind:    "cri",
			Name:    "containerd",
			Version: "2.1.0",
			Arch:    "amd64",
			Transport: deliveryapis.TransportRef{
				Type:   deliveryapis.TransportOCI,
				Ref:    "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0",
				Digest: "sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			},
			Contents: []deliveryapis.ArtifactContent{
				{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"},
			},
		},
	}

	metas := o.ComponentMetasFromInventory("registry.local:5000", catalog)
	if len(metas.PackageMetadata.Addons) != 1 {
		t.Fatalf("addons length = %d, want 1", len(metas.PackageMetadata.Addons))
	}
	if metas.PackageMetadata.Addons[0].Name != "containerd" {
		t.Fatalf("addons = %+v", metas.PackageMetadata.Addons)
	}
}
