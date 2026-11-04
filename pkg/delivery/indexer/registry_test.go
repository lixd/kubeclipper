/*
 *
 *  * Copyright 2024 KubeClipper Authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package indexer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

const (
	testDigest  = "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	testDigest2 = "sha256:2222222222222222222222222222222222222222222222222222222222222222"
)

func TestRegistryPackageIndexerIndex(t *testing.T) {
	img := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           "cri",
		Name:           "containerd",
		Version:        "2.1.0",
		ContentProfile: deliveryapis.ContentProfileRuntime,
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: "amd64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
		},
	})
	client := &fakeRegistryClient{
		repositories: []string{
			"package-containerd",
			"library/nginx",
		},
		tags: map[string][]string{
			"package-containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/package-containerd:2.1.0": {{
				Digest: testDigest,
				Platform: &v1.Platform{
					OS:           "linux",
					Architecture: "amd64",
				},
				Image: img,
			}},
		},
	}
	catalog, err := NewRegistryPackageInventoryIndexer(client).Index(context.Background(), "registry.local")
	if err != nil {
		t.Fatalf("Index() error: %+v", err)
	}
	if catalog.Spec.Registry != "registry.local" {
		t.Fatalf("inventory registry = %q, want registry.local", catalog.Spec.Registry)
	}
	if len(catalog.Spec.Packages) != 1 {
		t.Fatalf("package count = %d, want 1", len(catalog.Spec.Packages))
	}
	pkg := catalog.Spec.Packages[0]
	if pkg.Kind != "cri" || pkg.Name != "containerd" || pkg.Version != "2.1.0" || pkg.Arch != "amd64" {
		t.Fatalf("package identity = %+v", pkg)
	}
	if pkg.Transport.Ref != "registry.local/package-containerd:2.1.0" {
		t.Fatalf("transport ref = %q", pkg.Transport.Ref)
	}
	if pkg.Transport.Digest == "" {
		t.Fatalf("transport digest is empty")
	}
	if len(pkg.Contents) != 1 {
		t.Fatalf("content count = %d, want 1", len(pkg.Contents))
	}
	if pkg.Contents[0].Digest == "" {
		t.Fatalf("content digest is empty")
	}
}

func TestRegistryPackageIndexerIndexesKnownRepositoriesWithoutCatalog(t *testing.T) {
	img := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           "bootstrap",
		Name:           "kubeclipper",
		Version:        "v2.0.0",
		SourceRevision: "revision",
		ContentProfile: deliveryapis.ContentProfileBinary,
		Platform:       deliveryapis.PackageManifestPlatform{OS: "linux", Arch: "amd64"},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: "kubeclipper-agent", File: "kubeclipper-agent", Digest: testDigest},
		},
	})
	repository := "team-a/package-bootstrap-kubeclipper"
	client := &fakeRegistryClient{
		tags: map[string][]string{repository: {"v2.0.0"}},
		artifacts: map[string][]RegistryPackageArtifact{
			"harbor.example.com/team-a/package-bootstrap-kubeclipper:v2.0.0": {{
				Digest:   testDigest,
				Platform: &v1.Platform{OS: "linux", Architecture: "amd64"},
				Image:    img,
			}},
		},
	}

	inventory, err := NewRegistryPackageInventoryIndexer(client).IndexPackageRepositories(
		context.Background(),
		"harbor.example.com/team-a",
		[]string{"package-bootstrap-kubeclipper", "package-bootstrap-kubeclipper"},
	)
	if err != nil {
		t.Fatalf("IndexPackageRepositories() error: %+v", err)
	}
	if client.catalogCalls != 0 {
		t.Fatalf("Catalog() calls = %d, want 0", client.catalogCalls)
	}
	if client.tagCalls != 1 || client.resolveCalls != 1 {
		t.Fatalf("client calls = tags:%d resolve:%d, want 1/1", client.tagCalls, client.resolveCalls)
	}
	if len(inventory.Spec.Packages) != 1 {
		t.Fatalf("package count = %d, want 1", len(inventory.Spec.Packages))
	}
}

func TestRegistryPackageIndexerRejectsUnknownExplicitRepository(t *testing.T) {
	_, err := NewRegistryPackageInventoryIndexer(&fakeRegistryClient{}).IndexPackageRepositories(
		context.Background(), "registry.local", []string{"library/nginx"},
	)
	if err == nil {
		t.Fatal("IndexPackageRepositories() error = nil, want invalid package repository")
	}
}

func TestRegistryPackageIndexerIndexesKnownHelmRepositoryWithoutCatalog(t *testing.T) {
	repository := "team-a/kubeclipper/charts/tigera-operator"
	client := &fakeRegistryClient{
		tags: map[string][]string{repository: {"v3.31.5"}},
		artifacts: map[string][]RegistryPackageArtifact{
			"harbor.example.com/team-a/kubeclipper/charts/tigera-operator:v3.31.5": {{
				Digest: testDigest,
				Image:  empty.Image,
			}},
		},
	}

	inventory, err := NewRegistryPackageInventoryIndexer(client).IndexRepositories(
		context.Background(),
		"harbor.example.com/team-a",
		[]string{"kubeclipper/charts/tigera-operator"},
	)
	if err != nil {
		t.Fatalf("IndexRepositories() error: %+v", err)
	}
	if client.catalogCalls != 0 {
		t.Fatalf("Catalog() calls = %d, want 0", client.catalogCalls)
	}
	if len(inventory.Spec.Packages) != 2 {
		t.Fatalf("package count = %d, want amd64 and arm64", len(inventory.Spec.Packages))
	}
	for _, pkg := range inventory.Spec.Packages {
		if pkg.Kind != "cni" || pkg.Name != "calico" || pkg.Version != "v3.31.5" {
			t.Fatalf("package identity = %+v", pkg)
		}
	}
}

func TestRegistryPackageIndexerScopesProjectPrefix(t *testing.T) {
	img := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           "cri",
		Name:           "containerd",
		Version:        "2.1.0",
		ContentProfile: deliveryapis.ContentProfileRuntime,
		Platform:       deliveryapis.PackageManifestPlatform{OS: "linux", Arch: "amd64"},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
		},
	})
	client := &fakeRegistryClient{
		repositories: []string{
			"team-a/package-containerd",
			"team-b/package-containerd",
		},
		tags: map[string][]string{
			"team-a/package-containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"harbor.example.com/team-a/package-containerd:2.1.0": {{
				Digest: testDigest,
				Image:  img,
			}},
		},
	}
	inventory, err := NewRegistryPackageInventoryIndexer(client).Index(context.Background(), "harbor.example.com/team-a")
	if err != nil {
		t.Fatalf("Index() error = %v", err)
	}
	if len(inventory.Spec.Packages) != 1 {
		t.Fatalf("package count = %d, want 1", len(inventory.Spec.Packages))
	}
	if got := inventory.Spec.Packages[0].Transport.Ref; got != "harbor.example.com/team-a/package-containerd:2.1.0" {
		t.Fatalf("transport ref = %q", got)
	}
	if client.tagCalls != 1 || client.resolveCalls != 1 {
		t.Fatalf("scoped calls = tags:%d resolve:%d, want 1/1", client.tagCalls, client.resolveCalls)
	}
}

func TestRegistryPackageIndexerProjectsMultiArchArtifacts(t *testing.T) {
	amd64Image := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           "cri",
		Name:           "containerd",
		Version:        "2.1.0",
		ContentProfile: deliveryapis.ContentProfileRuntime,
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: "amd64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
		},
	})
	arm64Image := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           "cri",
		Name:           "containerd",
		Version:        "2.1.0",
		ContentProfile: deliveryapis.ContentProfileRuntime,
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: "arm64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", Digest: testDigest2},
		},
	})
	client := &fakeRegistryClient{
		repositories: []string{"package-containerd"},
		tags: map[string][]string{
			"package-containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/package-containerd:2.1.0": {
				{
					Digest: testDigest,
					Platform: &v1.Platform{
						OS:           "linux",
						Architecture: "amd64",
					},
					Image: amd64Image,
				},
				{
					Digest: testDigest2,
					Platform: &v1.Platform{
						OS:           "linux",
						Architecture: "arm64",
					},
					Image: arm64Image,
				},
			},
		},
	}
	inventory, err := NewRegistryPackageInventoryIndexer(client).Index(context.Background(), "registry.local")
	if err != nil {
		t.Fatalf("Index() error: %+v", err)
	}
	if len(inventory.Spec.Packages) != 2 {
		t.Fatalf("package count = %d, want 2", len(inventory.Spec.Packages))
	}
	byArch := map[string]deliveryapis.PackageEntry{}
	for _, pkg := range inventory.Spec.Packages {
		byArch[pkg.Arch] = pkg
	}
	if byArch["amd64"].Transport.Digest != testDigest {
		t.Fatalf("amd64 digest = %q", byArch["amd64"].Transport.Digest)
	}
	if byArch["arm64"].Transport.Digest != testDigest2 {
		t.Fatalf("arm64 digest = %q", byArch["arm64"].Transport.Digest)
	}
}

func TestRegistryPackageIndexerProjectsKnownHelmChartAsComponent(t *testing.T) {
	client := &fakeRegistryClient{
		repositories: []string{
			"kubeclipper/charts/tigera-operator",
		},
		tags: map[string][]string{
			"kubeclipper/charts/tigera-operator": {"v3.31.5"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/kubeclipper/charts/tigera-operator:v3.31.5": {{
				Digest: testDigest,
				Image:  empty.Image,
			}},
		},
	}
	inventory, err := NewRegistryPackageInventoryIndexer(client).Index(context.Background(), "registry.local")
	if err != nil {
		t.Fatalf("Index() error: %+v", err)
	}
	if len(inventory.Spec.Packages) != 2 {
		t.Fatalf("package count = %d, want 2", len(inventory.Spec.Packages))
	}
	byArch := map[string]deliveryapis.PackageEntry{}
	for _, pkg := range inventory.Spec.Packages {
		byArch[pkg.Arch] = pkg
	}
	for _, arch := range []string{"amd64", "arm64"} {
		pkg, ok := byArch[arch]
		if !ok {
			t.Fatalf("missing projected calico chart package for arch %s", arch)
		}
		if pkg.Kind != "cni" || pkg.Name != "calico" || pkg.Version != "v3.31.5" {
			t.Fatalf("package identity = %+v", pkg)
		}
		if pkg.Transport.Ref != "registry.local/kubeclipper/charts/tigera-operator:v3.31.5" {
			t.Fatalf("transport ref = %q", pkg.Transport.Ref)
		}
		if len(pkg.Contents) != 1 {
			t.Fatalf("content count = %d, want 1", len(pkg.Contents))
		}
		content := pkg.Contents[0]
		if content.Name != deliveryapis.ContentCharts || content.Transport.Type != deliveryapis.TransportHelmOCI {
			t.Fatalf("chart content = %+v", content)
		}
		if content.Transport.Ref != "registry.local/kubeclipper/charts/tigera-operator" {
			t.Fatalf("chart transport ref = %q", content.Transport.Ref)
		}
	}
}

func TestRegistryPackageIndexerSkipsInvalidArtifact(t *testing.T) {
	validImage := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           "cri",
		Name:           "containerd",
		Version:        "2.1.0",
		ContentProfile: deliveryapis.ContentProfileRuntime,
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: "amd64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
		},
	})
	client := &fakeRegistryClient{
		repositories: []string{"package-containerd"},
		tags: map[string][]string{
			"package-containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/package-containerd:2.1.0": {
				{
					Digest: testDigest2,
					Image:  empty.Image,
				},
				{
					Digest: testDigest,
					Platform: &v1.Platform{
						OS:           "linux",
						Architecture: "amd64",
					},
					Image: validImage,
				},
			},
		},
	}
	inventory, err := NewRegistryPackageInventoryIndexer(client).Index(context.Background(), "registry.local")
	if err != nil {
		t.Fatalf("Index() error: %+v", err)
	}
	if len(inventory.Spec.Packages) != 1 {
		t.Fatalf("package count = %d, want 1", len(inventory.Spec.Packages))
	}
	if inventory.Spec.Packages[0].Transport.Digest != testDigest {
		t.Fatalf("selected digest = %q", inventory.Spec.Packages[0].Transport.Digest)
	}
}

func TestRegistryPackageIndexerDoesNotDownloadPayloads(t *testing.T) {
	img := testPackageImageWithoutPayloads(t, deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           "cri",
		Name:           "containerd",
		Version:        "2.1.0",
		ContentProfile: deliveryapis.ContentProfileRuntime,
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: "amd64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", MediaType: deliveryapis.MediaTypeConfigsLayer, Digest: testDigest},
		},
	})
	client := &fakeRegistryClient{
		repositories: []string{"package-containerd"},
		tags: map[string][]string{
			"package-containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/package-containerd:2.1.0": {{
				Digest: testDigest,
				Image:  img,
			}},
		},
	}
	inventory, err := NewRegistryPackageInventoryIndexer(client).Index(context.Background(), "registry.local")
	if err != nil {
		t.Fatalf("Index() error: %+v", err)
	}
	if len(inventory.Spec.Packages) != 1 {
		t.Fatalf("package count = %d, want 1", len(inventory.Spec.Packages))
	}
}

func TestRegistryPackageIndexerSkipsUnresolvableTag(t *testing.T) {
	validImage := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           "cri",
		Name:           "containerd",
		Version:        "2.1.0",
		ContentProfile: deliveryapis.ContentProfileRuntime,
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: "amd64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
		},
	})
	client := &fakeRegistryClient{
		repositories: []string{"package-containerd"},
		tags: map[string][]string{
			"package-containerd": {"broken", "2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/package-containerd:2.1.0": {{
				Digest: testDigest,
				Image:  validImage,
			}},
		},
	}
	inventory, err := NewRegistryPackageInventoryIndexer(client).Index(context.Background(), "registry.local")
	if err != nil {
		t.Fatalf("Index() error: %+v", err)
	}
	if len(inventory.Spec.Packages) != 1 {
		t.Fatalf("package count = %d, want 1", len(inventory.Spec.Packages))
	}
	if inventory.Spec.Packages[0].Version != "2.1.0" {
		t.Fatalf("package version = %q, want 2.1.0", inventory.Spec.Packages[0].Version)
	}
}

func TestRegistryPackageIndexerFailsOnDuplicateIdentity(t *testing.T) {
	first := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           "cri",
		Name:           "containerd",
		Version:        "2.1.0",
		ContentProfile: deliveryapis.ContentProfileRuntime,
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: "amd64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
		},
	})
	second := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           "cri",
		Name:           "containerd",
		Version:        "2.1.0",
		ContentProfile: deliveryapis.ContentProfileRuntime,
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: "amd64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", Digest: testDigest2},
		},
	})
	client := &fakeRegistryClient{
		repositories: []string{"package-containerd"},
		tags: map[string][]string{
			"package-containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/package-containerd:2.1.0": {
				{
					Digest: testDigest,
					Image:  first,
				},
				{
					Digest: testDigest2,
					Image:  second,
				},
			},
		},
	}
	if _, err := NewRegistryPackageInventoryIndexer(client).Index(context.Background(), "registry.local"); err == nil {
		t.Fatalf("Index() expected duplicate identity error")
	}
}

func TestReadPackageManifest(t *testing.T) {
	img := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion: 1,
		Kind:          "k8s",
		Name:          "k8s",
		Version:       "v1.36.0",
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: "amd64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
			{Name: deliveryapis.ContentCharts, File: "charts.tgz", Digest: testDigest},
		},
	})
	manifest, err := ReadPackageManifest(img)
	if err != nil {
		t.Fatalf("ReadPackageManifest() error: %+v", err)
	}
	if manifest.Kind != "k8s" || manifest.Platform.Arch != "amd64" {
		t.Fatalf("manifest = %+v", manifest)
	}
}

func testPackageImage(t *testing.T, manifest deliveryapis.PackageManifest) v1.Image {
	t.Helper()
	manifest, payloads := testPayloadFiles(t, manifest)
	layerPath := testPackageRootFSLayer(t, manifest, payloads)
	layer, err := tarball.LayerFromFile(layerPath, tarball.WithMediaType(types.OCILayer))
	if err != nil {
		t.Fatalf("LayerFromFile() error: %+v", err)
	}
	img := mutate.MediaType(empty.Image, types.OCIManifestSchema1)
	img = mutate.ConfigMediaType(img, types.OCIConfigJSON)
	img, err = mutate.Append(img, mutate.Addendum{
		Layer:     layer,
		MediaType: types.OCILayer,
	})
	if err != nil {
		t.Fatalf("mutate.Append() error: %+v", err)
	}
	return img
}

func testPackageImageWithoutPayloads(t *testing.T, manifest deliveryapis.PackageManifest) v1.Image {
	t.Helper()
	layerPath := testPackageRootFSLayer(t, manifest, nil)
	layer, err := tarball.LayerFromFile(layerPath, tarball.WithMediaType(types.OCILayer))
	if err != nil {
		t.Fatalf("LayerFromFile() error: %+v", err)
	}
	img := mutate.MediaType(empty.Image, types.OCIManifestSchema1)
	img = mutate.ConfigMediaType(img, types.OCIConfigJSON)
	img, err = mutate.Append(img, mutate.Addendum{
		Layer:     layer,
		MediaType: types.OCILayer,
	})
	if err != nil {
		t.Fatalf("mutate.Append() error: %+v", err)
	}
	return img
}

func testPayloadFiles(t *testing.T, manifest deliveryapis.PackageManifest) (deliveryapis.PackageManifest, map[string][]byte) {
	t.Helper()
	payloads := make(map[string][]byte, len(manifest.Contents))
	for i := range manifest.Contents {
		content := &manifest.Contents[i]
		if content.File == "" {
			content.File = deliveryapis.ContentFile(content.Name)
		}
		if content.MediaType == "" {
			content.MediaType = deliveryapis.MediaTypeForContent(content.Name)
		}
		payloadContent := fmt.Sprintf("%s/%s:%s/%s/%s", manifest.Kind, manifest.Name, manifest.Version, manifest.Platform.Arch, content.Name)
		payload := []byte(payloadContent)
		if content.Name != deliveryapis.ContentBinary {
			payload = testGzipArchive(t, payloadContent)
		}
		digest, err := packageFilePayloadDigest(payload, content.Name)
		if err != nil {
			t.Fatalf("packageFilePayloadDigest() error: %+v", err)
		}
		content.Digest = digest
		payloads[content.File] = payload
	}
	return manifest, payloads
}

func testPackageRootFSLayer(t *testing.T, manifest deliveryapis.PackageManifest, payloads map[string][]byte) string {
	t.Helper()
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %+v", err)
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, dir := range []string{"opt/", "opt/kubeclipper/", "opt/kubeclipper/resource/"} {
		if err = tw.WriteHeader(&tar.Header{Name: dir, Typeflag: tar.TypeDir, Mode: 0755}); err != nil {
			t.Fatalf("write package dir: %+v", err)
		}
	}
	if err = tw.WriteHeader(&tar.Header{
		Name: "opt/kubeclipper/resource/kc-package-manifest.json",
		Mode: 0644,
		Size: int64(len(data)),
	}); err != nil {
		t.Fatalf("write header: %+v", err)
	}
	if _, err = tw.Write(data); err != nil {
		t.Fatalf("write manifest: %+v", err)
	}
	for file, payload := range payloads {
		if err = tw.WriteHeader(&tar.Header{
			Name: "opt/kubeclipper/resource/" + file,
			Mode: 0644,
			Size: int64(len(payload)),
		}); err != nil {
			t.Fatalf("write payload header: %+v", err)
		}
		if _, err = tw.Write(payload); err != nil {
			t.Fatalf("write payload: %+v", err)
		}
	}
	if err = tw.Close(); err != nil {
		t.Fatalf("close tar: %+v", err)
	}
	path := t.TempDir() + "/manifest.tar"
	if err = os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		t.Fatalf("write layer: %+v", err)
	}
	return path
}

func testGzipArchive(t *testing.T, content string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	data := []byte(content)
	if err := tw.WriteHeader(&tar.Header{Name: "payload.txt", Mode: 0644, Size: int64(len(data))}); err != nil {
		t.Fatalf("write gzip payload header: %+v", err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatalf("write gzip payload: %+v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close gzip tar: %+v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("close gzip: %+v", err)
	}
	return buf.Bytes()
}

type fakeRegistryClient struct {
	repositories []string
	tags         map[string][]string
	tagErrors    map[string]error
	artifacts    map[string][]RegistryPackageArtifact
	catalogCalls int
	tagCalls     int
	resolveCalls int
}

func (f *fakeRegistryClient) Catalog(ctx context.Context, registry string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.catalogCalls++
	return f.repositories, nil
}

func (f *fakeRegistryClient) ListTags(ctx context.Context, registry, repository string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.tagCalls++
	if err, ok := f.tagErrors[repository]; ok {
		return nil, err
	}
	return f.tags[repository], nil
}

func (f *fakeRegistryClient) Resolve(ctx context.Context, ref string) ([]RegistryPackageArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.resolveCalls++
	artifacts, ok := f.artifacts[ref]
	if !ok {
		return nil, fmt.Errorf("artifact %q not found", ref)
	}
	return artifacts, nil
}

func TestRegistryPackageIndexerCachesResultsUntilRefresh(t *testing.T) {
	img := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion: 1,
		Kind:          "cri",
		Name:          "containerd",
		Version:       "2.1.0",
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: "amd64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
		},
	})
	client := &fakeRegistryClient{
		repositories: []string{"package-containerd"},
		tags: map[string][]string{
			"package-containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/package-containerd:2.1.0": {{
				Digest: testDigest,
				Image:  img,
			}},
		},
	}
	indexer := NewRegistryPackageInventoryIndexer(client)
	indexer.CacheTTL = time.Hour
	ctx := context.Background()
	first, err := indexer.Index(ctx, "registry.local")
	if err != nil {
		t.Fatalf("Index() error: %+v", err)
	}
	second, err := indexer.Index(ctx, "registry.local")
	if err != nil {
		t.Fatalf("Index() second error: %+v", err)
	}
	if first != second {
		t.Fatalf("expected cached catalog pointer reuse")
	}
	if client.catalogCalls != 1 || client.tagCalls != 1 || client.resolveCalls != 1 {
		t.Fatalf("client calls = catalog:%d tags:%d resolve:%d, want 1/1/1", client.catalogCalls, client.tagCalls, client.resolveCalls)
	}
	if _, err = indexer.Refresh(ctx, "registry.local"); err != nil {
		t.Fatalf("Refresh() error: %+v", err)
	}
	if client.catalogCalls != 2 || client.tagCalls != 2 || client.resolveCalls != 2 {
		t.Fatalf("client calls after refresh = catalog:%d tags:%d resolve:%d, want 2/2/2", client.catalogCalls, client.tagCalls, client.resolveCalls)
	}
}

func TestIndexRepositoriesToleratesMissingRepositories(t *testing.T) {
	nameUnknown := &transport.Error{
		StatusCode: http.StatusNotFound,
		Errors: []transport.Diagnostic{
			{Code: transport.NameUnknownErrorCode, Message: "repository name not known to registry"},
		},
	}
	img := testPackageImage(t, deliveryapis.PackageManifest{
		SchemaVersion: 1,
		Kind:          "k8s",
		Name:          "k8s",
		Version:       "v1.37.0",
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: "amd64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
		},
	})
	client := &fakeRegistryClient{
		tags: map[string][]string{
			"package-k8s": {"v1.37.0"},
		},
		tagErrors: map[string]error{
			"package-k8s-extension": nameUnknown,
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/package-k8s:v1.37.0": {{
				Digest: testDigest,
				Image:  img,
			}},
		},
	}
	indexer := NewRegistryPackageInventoryIndexer(client)
	inventory, err := indexer.IndexRepositories(context.Background(), "registry.local", []string{
		"package-k8s",
		"package-k8s-extension",
	})
	if err != nil {
		t.Fatalf("IndexRepositories() error: %+v", err)
	}
	if len(inventory.Spec.Packages) != 1 {
		t.Fatalf("indexed packages = %d, want 1 (missing repository must be indexed as empty)", len(inventory.Spec.Packages))
	}
	if inventory.Spec.Packages[0].Name != "k8s" || inventory.Spec.Packages[0].Version != "v1.37.0" {
		t.Fatalf("unexpected package entry %+v", inventory.Spec.Packages[0])
	}
}

func TestIndexRepositoriesPropagatesRegistryErrors(t *testing.T) {
	unauthorized := &transport.Error{
		StatusCode: http.StatusUnauthorized,
		Errors: []transport.Diagnostic{
			{Code: transport.ErrorCode("UNAUTHORIZED"), Message: "authentication required"},
		},
	}
	client := &fakeRegistryClient{
		tagErrors: map[string]error{
			"package-k8s": unauthorized,
		},
	}
	indexer := NewRegistryPackageInventoryIndexer(client)
	_, err := indexer.IndexRepositories(context.Background(), "registry.local", []string{"package-k8s"})
	if err == nil {
		t.Fatalf("IndexRepositories() expected error for unauthorized registry")
	}
	var terr *transport.Error
	if !errors.As(err, &terr) || terr.StatusCode != http.StatusUnauthorized {
		t.Fatalf("IndexRepositories() error = %+v, want unauthorized transport error", err)
	}
}

func TestPolicyRepositoryInventoryUsesScopedCache(t *testing.T) {
	client := &fakeRegistryClient{}
	indexer := NewRegistryPackageInventoryIndexer(client)
	ctx := context.Background()
	repos := []string{"package-k8s", "package-containerd"}
	first, err := indexer.IndexRepositories(ctx, "registry.local", repos)
	if err != nil {
		t.Fatal(err)
	}
	_, err = indexer.IndexRepositories(ctx, "registry.local", []string{repos[1], repos[0], repos[0]})
	if err != nil {
		t.Fatal(err)
	}
	if client.tagCalls != 2 {
		t.Fatalf("tag requests = %d, want 2 across repeated policy reads", client.tagCalls)
	}
	_, err = indexer.IndexRepositories(ctx, "registry.local", repos[:1])
	if err != nil {
		t.Fatal(err)
	}
	if client.tagCalls != 3 {
		t.Fatal("different repository selection reused an incompatible inventory")
	}
	indexer.CacheTTL = time.Nanosecond
	time.Sleep(time.Millisecond)
	if _, err = indexer.IndexRepositories(ctx, "registry.local", repos); err != nil {
		t.Fatal(err)
	}
	if client.tagCalls != 5 {
		t.Fatal("expired repository inventory was reused")
	}
	if first == nil {
		t.Fatal("missing inventory")
	}
	if _, err = indexer.RefreshRepositories(ctx, "registry.local", repos); err != nil {
		t.Fatal(err)
	}
	if client.tagCalls != 7 {
		t.Fatal("explicit refresh reused stale repository inventory")
	}
}

func TestPolicyRepositoryInventoryFailureIsNotCached(t *testing.T) {
	client := &fakeRegistryClient{tagErrors: map[string]error{"package-k8s": errors.New("registry unavailable")}}
	indexer := NewRegistryPackageInventoryIndexer(client)
	repositories := []string{"package-k8s"}
	if _, err := indexer.IndexRepositories(context.Background(), "registry.local", repositories); err == nil {
		t.Fatal("registry failure was hidden")
	}
	delete(client.tagErrors, repositories[0])
	if _, err := indexer.IndexRepositories(context.Background(), "registry.local", repositories); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := indexer.IndexRepositories(ctx, "registry.local", repositories); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached response ignored cancellation: %v", err)
	}
}

type blockedRepositoryClient struct {
	RegistryClient
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (c *blockedRepositoryClient) ListTags(ctx context.Context, _, _ string) ([]string, error) {
	if c.calls.Add(1) == 1 {
		close(c.started)
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.release:
		return nil, nil
	}
}

func TestRepositoryReadsCoalesceAndWaitersCancel(t *testing.T) {
	client := &blockedRepositoryClient{started: make(chan struct{}), release: make(chan struct{})}
	indexer := NewRegistryPackageInventoryIndexer(client)
	repositories := []string{"package-k8s"}
	results := make(chan error, 9)
	go func() {
		_, err := indexer.IndexRepositories(context.Background(), "registry.local", repositories)
		results <- err
	}()
	<-client.started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := indexer.IndexRepositories(ctx, "registry.local", repositories); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked waiter did not cancel: %v", err)
	}
	for n := 0; n < 8; n++ {
		go func() {
			_, err := indexer.IndexRepositories(context.Background(), "registry.local", repositories)
			results <- err
		}()
	}
	close(client.release)
	for n := 0; n < 9; n++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if calls := client.calls.Load(); calls != 1 {
		t.Fatalf("concurrent tag reads = %d, want 1", calls)
	}
}

type cancelingRepositoryClient struct {
	RegistryClient
	cancel context.CancelFunc
	calls  atomic.Int32
}

func (c *cancelingRepositoryClient) ListTags(context.Context, string, string) ([]string, error) {
	c.calls.Add(1)
	c.cancel()
	return nil, nil
}

func TestCanceledRepositoryScanDoesNotPublishCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &cancelingRepositoryClient{cancel: cancel}
	indexer := NewRegistryPackageInventoryIndexer(client)
	repositories := []string{"package-k8s"}
	if _, err := indexer.IndexRepositories(ctx, "registry.local", repositories); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled scan returned %v", err)
	}
	// A following request may first join the canceled flight; it must then
	// perform a new scan instead of using the canceled result as a cache hit.
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if _, err = indexer.IndexRepositories(context.Background(), "registry.local", repositories); err == nil {
			break
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	if err != nil || client.calls.Load() != 2 {
		t.Fatalf("canceled scan poisoned cache: calls=%d err=%v", client.calls.Load(), err)
	}
}
