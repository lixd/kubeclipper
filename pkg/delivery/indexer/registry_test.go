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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
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
			"kubeclipper/packages/cri/containerd",
			"library/nginx",
		},
		tags: map[string][]string{
			"kubeclipper/packages/cri/containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/kubeclipper/packages/cri/containerd:2.1.0": {{
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
	if pkg.Transport.Ref != "registry.local/kubeclipper/packages/cri/containerd:2.1.0" {
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
			"team-a/kubeclipper/packages/cri/containerd",
			"team-b/kubeclipper/packages/cri/containerd",
		},
		tags: map[string][]string{
			"team-a/kubeclipper/packages/cri/containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"harbor.example.com/team-a/kubeclipper/packages/cri/containerd:2.1.0": {{
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
	if got := inventory.Spec.Packages[0].Transport.Ref; got != "harbor.example.com/team-a/kubeclipper/packages/cri/containerd:2.1.0" {
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
		repositories: []string{"kubeclipper/packages/cri/containerd"},
		tags: map[string][]string{
			"kubeclipper/packages/cri/containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/kubeclipper/packages/cri/containerd:2.1.0": {
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
		repositories: []string{"kubeclipper/packages/cri/containerd"},
		tags: map[string][]string{
			"kubeclipper/packages/cri/containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/kubeclipper/packages/cri/containerd:2.1.0": {
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
		repositories: []string{"kubeclipper/packages/cri/containerd"},
		tags: map[string][]string{
			"kubeclipper/packages/cri/containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/kubeclipper/packages/cri/containerd:2.1.0": {{
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
		repositories: []string{"kubeclipper/packages/cri/containerd"},
		tags: map[string][]string{
			"kubeclipper/packages/cri/containerd": {"broken", "2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/kubeclipper/packages/cri/containerd:2.1.0": {{
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
		repositories: []string{"kubeclipper/packages/cri/containerd"},
		tags: map[string][]string{
			"kubeclipper/packages/cri/containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/kubeclipper/packages/cri/containerd:2.1.0": {
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
		repositories: []string{"kubeclipper/packages/cri/containerd"},
		tags: map[string][]string{
			"kubeclipper/packages/cri/containerd": {"2.1.0"},
		},
		artifacts: map[string][]RegistryPackageArtifact{
			"registry.local/kubeclipper/packages/cri/containerd:2.1.0": {{
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
