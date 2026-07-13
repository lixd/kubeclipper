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

package fetcher

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	containerv1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/types"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
)

func TestOCIReference(t *testing.T) {
	for _, tt := range []struct {
		name      string
		transport deliveryapis.TransportRef
		want      string
		wantErr   bool
	}{
		{name: "empty", transport: deliveryapis.TransportRef{}, wantErr: true},
		{name: "tag only", transport: deliveryapis.TransportRef{Ref: "registry.local/kc/pkg:v1"}, wantErr: true},
		{name: "tag and digest", transport: deliveryapis.TransportRef{Ref: "registry.local/kc/pkg:v1", Digest: "sha256:abc"}, want: "registry.local/kc/pkg:v1@sha256:abc"},
		{name: "already pinned matching digest", transport: deliveryapis.TransportRef{Ref: "registry.local/kc/pkg@sha256:abc", Digest: "sha256:abc"}, want: "registry.local/kc/pkg@sha256:abc"},
		{name: "already pinned mismatched digest", transport: deliveryapis.TransportRef{Ref: "registry.local/kc/pkg@sha256:abc", Digest: "sha256:def"}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ociReference(tt.transport)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("ociReference() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ociReference() error: %+v", err)
			}
			if got != tt.want {
				t.Fatalf("ociReference() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOCIArtifactFetcherRejectsTagOnlyReference(t *testing.T) {
	fetcher := NewOCIArtifactFetcher(false)
	_, err := fetcher.Fetch(context.Background(), &deliveryapis.ResolvedArtifactPlan{
		KubernetesVersion: "v1.36.0",
		OS:                deliveryapis.DefaultPackageOS,
		Arch:              "amd64",
		Components: []deliveryapis.ResolvedComponent{{
			Kind:    "cni",
			Name:    "calico",
			Version: "v3.31.5",
			Transport: deliveryapis.TransportRef{
				Type: deliveryapis.TransportOCI,
				Ref:  "registry.local:5000/kubeclipper/packages/cni/calico:v3.31.5",
			},
			Contents: []deliveryapis.ArtifactContent{{Name: deliveryapis.ContentCharts}},
		}},
	})
	if err == nil {
		t.Fatalf("Fetch() error = nil, want digest required error")
	}
}

func TestOCIArtifactFetcherRejectsMismatchedPinnedReference(t *testing.T) {
	fetcher := NewOCIArtifactFetcher(false)
	fetcher.PullImage = func(ref string) (containerv1.Image, error) {
		t.Fatalf("PullImage() should not be called for mismatched pinned reference")
		return nil, nil
	}
	_, err := fetcher.Fetch(context.Background(), &deliveryapis.ResolvedArtifactPlan{
		KubernetesVersion: "v1.36.0",
		OS:                deliveryapis.DefaultPackageOS,
		Arch:              "amd64",
		Components: []deliveryapis.ResolvedComponent{{
			Kind:    "cni",
			Name:    "calico",
			Version: "v3.31.5",
			Transport: deliveryapis.TransportRef{
				Type:   deliveryapis.TransportOCI,
				Ref:    "registry.local:5000/kubeclipper/packages/cni/calico:v3.31.5@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			},
			Contents: []deliveryapis.ArtifactContent{{Name: deliveryapis.ContentCharts}},
		}},
	})
	if err == nil {
		t.Fatalf("Fetch() error = nil, want mismatched digest error")
	}
}

func TestContentFile(t *testing.T) {
	for _, tt := range []struct {
		content deliveryapis.ArtifactContent
		want    string
	}{
		{content: deliveryapis.ArtifactContent{Name: deliveryapis.ContentConfigs}, want: downloader.ConfigFilename},
		{content: deliveryapis.ArtifactContent{Name: deliveryapis.ContentCharts}, want: downloader.ChartFilename},
		{content: deliveryapis.ArtifactContent{Name: "descriptor", File: "descriptor.json"}, want: "descriptor.json"},
	} {
		if got := contentFile(tt.content); got != tt.want {
			t.Fatalf("contentFile(%+v) = %q, want %q", tt.content, got, tt.want)
		}
	}
}

func TestOCIArtifactFetcherDryRun(t *testing.T) {
	fetcher := NewOCIArtifactFetcher(true)
	result, err := fetcher.Fetch(context.Background(), &deliveryapis.ResolvedArtifactPlan{
		KubernetesVersion: "v1.36.0",
		OS:                deliveryapis.DefaultPackageOS,
		Arch:              "amd64",
		Components: []deliveryapis.ResolvedComponent{
			{
				Slot:    "cni",
				Kind:    "cni",
				Name:    "calico",
				Version: "v3.31.5",
				Transport: deliveryapis.TransportRef{
					Type:   deliveryapis.TransportOCI,
					Ref:    "registry.local:5000/kubeclipper/packages/cni/calico:v3.31.5",
					Digest: "sha256:4444444444444444444444444444444444444444444444444444444444444444",
				},
				Contents: []deliveryapis.ArtifactContent{
					{Name: deliveryapis.ContentCharts, File: downloader.ChartFilename, MediaType: deliveryapis.MediaTypeChartsLayer},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("Fetch() error: %+v", err)
	}
	if len(result.Components) != 1 {
		t.Fatalf("components length = %d, want 1", len(result.Components))
	}
	if result.Components[0].OS != deliveryapis.DefaultPackageOS {
		t.Fatalf("component os = %q", result.Components[0].OS)
	}
	if got := result.Components[0].Files[deliveryapis.ContentCharts]; got != "/tmp/kc-downloader/packages/cni/calico/v3.31.5/linux-amd64/contents/charts.tgz" {
		t.Fatalf("chart path = %q", got)
	}
	if result.Components[0].BaseDir != "/tmp/kc-downloader/packages/cni/calico/v3.31.5/linux-amd64" {
		t.Fatalf("base dir = %q", result.Components[0].BaseDir)
	}
}

func TestOCIArtifactFetcherValidatesPackageManifest(t *testing.T) {
	payload := testGzipPayload(t, "chart payload")
	payloadDigest := mustPackageDigest(t, payload, deliveryapis.ContentCharts)
	manifest := fetcherPackageManifest("calico", "v3.31.5", payloadDigest)
	image := fetcherPackageImage(t, manifest, newStubLayer(payload, deliveryapis.MediaTypeChartsLayer))
	manifestDigest := imageDigestString(t, image)
	fetcher := NewOCIArtifactFetcher(false)
	fetcher.PullImage = func(ref string) (containerv1.Image, error) {
		if ref != "registry.local:5000/kubeclipper/packages/cni/calico:v3.31.5@"+manifestDigest {
			t.Fatalf("pull ref = %q", ref)
		}
		return image, nil
	}
	defer os.RemoveAll(downloader.PackageDir("cni", "calico", "v3.31.5", "linux-amd64"))

	result, err := fetcher.Fetch(context.Background(), fetcherPlanWithTransportDigest("calico", "v3.31.5", payloadDigest, manifestDigest))
	if err != nil {
		t.Fatalf("Fetch() error: %+v", err)
	}
	if len(result.Components) != 1 {
		t.Fatalf("components length = %d, want 1", len(result.Components))
	}
	path := result.Components[0].Files[deliveryapis.ContentCharts]
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error: %+v", err)
	}
	if string(data) != string(payload) {
		t.Fatalf("payload = %q", string(data))
	}
}

func TestOCIArtifactFetcherRejectsPulledDigestMismatch(t *testing.T) {
	payload := testGzipPayload(t, "chart payload")
	payloadDigest := mustPackageDigest(t, payload, deliveryapis.ContentCharts)
	manifest := fetcherPackageManifest("calico", "v3.31.5", payloadDigest)
	image := fetcherPackageImage(t, manifest, newStubLayer(payload, deliveryapis.MediaTypeChartsLayer))
	fetcher := NewOCIArtifactFetcher(false)
	fetcher.PullImage = func(ref string) (containerv1.Image, error) {
		return image, nil
	}
	defer os.RemoveAll(downloader.PackageDir("cni", "calico", "v3.31.5", "linux-amd64"))

	if _, err := fetcher.Fetch(context.Background(), fetcherPlan("calico", "v3.31.5", payloadDigest)); err == nil {
		t.Fatalf("Fetch() expected pulled digest mismatch error")
	}
}

func TestOCIArtifactFetcherRejectsMismatchedPackageManifest(t *testing.T) {
	payload := testGzipPayload(t, "chart payload")
	payloadDigest := mustPackageDigest(t, payload, deliveryapis.ContentCharts)
	manifest := fetcherPackageManifest("flannel", "v3.31.5", payloadDigest)
	image := fetcherPackageImage(t, manifest, newStubLayer(payload, deliveryapis.MediaTypeChartsLayer))
	manifestDigest := imageDigestString(t, image)
	fetcher := NewOCIArtifactFetcher(false)
	fetcher.PullImage = func(ref string) (containerv1.Image, error) {
		return image, nil
	}
	defer os.RemoveAll(downloader.PackageDir("cni", "calico", "v3.31.5", "linux-amd64"))

	if _, err := fetcher.Fetch(context.Background(), fetcherPlanWithTransportDigest("calico", "v3.31.5", payloadDigest, manifestDigest)); err == nil {
		t.Fatalf("Fetch() expected manifest identity error")
	}
}

func TestWritePackageFileValidatesDigest(t *testing.T) {
	payload := testGzipPayload(t, "configs")
	digest := mustPackageDigest(t, payload, deliveryapis.ContentConfigs)
	manifest := fetcherPackageManifest("calico", "v3.31.5", digest)
	manifest.Contents[0].Name = deliveryapis.ContentConfigs
	manifest.Contents[0].File = downloader.ConfigFilename
	layer := packageRootFSLayer(t, manifest, payload)
	target := filepath.Join(t.TempDir(), "configs.tar.gz")
	if err := writePackageFile([]containerv1.Layer{layer}, target, deliveryapis.ArtifactContent{
		Name:   deliveryapis.ContentConfigs,
		File:   downloader.ConfigFilename,
		Digest: digest,
	}); err != nil {
		t.Fatalf("writePackageFile() error: %+v", err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile() error: %+v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("payload was not preserved")
	}
}

func TestWritePackageFileRejectsDigestMismatch(t *testing.T) {
	payload := testGzipPayload(t, "configs")
	digest := mustPackageDigest(t, payload, deliveryapis.ContentConfigs)
	manifest := fetcherPackageManifest("calico", "v3.31.5", digest)
	manifest.Contents[0].Name = deliveryapis.ContentConfigs
	manifest.Contents[0].File = downloader.ConfigFilename
	layer := packageRootFSLayer(t, manifest, payload)
	target := filepath.Join(t.TempDir(), "configs.tar.gz")
	if err := writePackageFile([]containerv1.Layer{layer}, target, deliveryapis.ArtifactContent{
		Name:   deliveryapis.ContentConfigs,
		File:   downloader.ConfigFilename,
		Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}); err == nil {
		t.Fatalf("writePackageFile() expected digest mismatch error")
	}
}

func TestWritePackageFileWritesBinaryPayload(t *testing.T) {
	payload := []byte("binary-payload")
	digest := mustPackageDigest(t, payload, deliveryapis.ContentBinary)
	manifest := fetcherPackageManifest("kubeclipper-agent", "v1.8.0", digest)
	manifest.Kind = "binary"
	manifest.Name = "kubeclipper-agent"
	manifest.Contents[0].Name = deliveryapis.ContentBinary
	manifest.Contents[0].File = "kubeclipper-agent"
	layer := packageRootFSLayer(t, manifest, payload)
	target := filepath.Join(t.TempDir(), "kubeclipper-agent")
	if err := writePackageFile([]containerv1.Layer{layer}, target, deliveryapis.ArtifactContent{
		Name:   deliveryapis.ContentBinary,
		File:   "kubeclipper-agent",
		Digest: digest,
	}); err != nil {
		t.Fatalf("writePackageFile() error: %+v", err)
	}
	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile() error: %+v", err)
	}
	if string(data) != "binary-payload" {
		t.Fatalf("payload = %q, want binary-payload", string(data))
	}
}

type stubLayer struct {
	data           []byte
	compressedData []byte
	mediaType      types.MediaType
	digest         containerv1.Hash
}

func newStubLayer(data []byte, mediaType string) stubLayer {
	sum := sha256.Sum256(data)
	return stubLayer{
		data:      append([]byte(nil), data...),
		mediaType: types.MediaType(mediaType),
		digest: containerv1.Hash{
			Algorithm: "sha256",
			Hex:       hex.EncodeToString(sum[:]),
		},
	}
}

func (l stubLayer) Digest() (containerv1.Hash, error) {
	return l.digest, nil
}

func (l stubLayer) DiffID() (containerv1.Hash, error) {
	return l.digest, nil
}

func (l stubLayer) Compressed() (io.ReadCloser, error) {
	if l.compressedData != nil {
		return io.NopCloser(bytes.NewReader(l.compressedData)), nil
	}
	return io.NopCloser(bytes.NewReader(l.data)), nil
}

func (l stubLayer) Uncompressed() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.data)), nil
}

func (l stubLayer) Size() (int64, error) {
	return int64(len(l.data)), nil
}

func (l stubLayer) MediaType() (types.MediaType, error) {
	return l.mediaType, nil
}

func mustPackageDigest(t *testing.T, data []byte, contentName string) string {
	t.Helper()
	digest, err := packageFilePayloadDigest(data, contentName)
	if err != nil {
		t.Fatalf("packageFilePayloadDigest() error: %+v", err)
	}
	return digest
}

func testGzipPayload(t *testing.T, content string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gzw)
	if err := tw.WriteHeader(&tar.Header{
		Name: "payload.txt",
		Mode: 0644,
		Size: int64(len(content)),
	}); err != nil {
		t.Fatalf("write payload header: %+v", err)
	}
	if _, err := tw.Write([]byte(content)); err != nil {
		t.Fatalf("write payload: %+v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("close tar: %+v", err)
	}
	if err := gzw.Close(); err != nil {
		t.Fatalf("close gzip: %+v", err)
	}
	return buf.Bytes()
}

func fetcherPlan(name, version, digest string) *deliveryapis.ResolvedArtifactPlan {
	return fetcherPlanWithTransportDigest(name, version, digest, "sha256:4444444444444444444444444444444444444444444444444444444444444444")
}

func fetcherPlanWithTransportDigest(name, version, digest, transportDigest string) *deliveryapis.ResolvedArtifactPlan {
	return &deliveryapis.ResolvedArtifactPlan{
		KubernetesVersion: "v1.36.0",
		OS:                deliveryapis.DefaultPackageOS,
		Arch:              "amd64",
		Components: []deliveryapis.ResolvedComponent{
			{
				Slot:    "cni",
				Kind:    "cni",
				Name:    name,
				Version: version,
				OS:      deliveryapis.DefaultPackageOS,
				Transport: deliveryapis.TransportRef{
					Type:   deliveryapis.TransportOCI,
					Ref:    "registry.local:5000/kubeclipper/packages/cni/" + name + ":" + version,
					Digest: transportDigest,
				},
				Contents: []deliveryapis.ArtifactContent{
					{
						Name:      deliveryapis.ContentCharts,
						File:      downloader.ChartFilename,
						Digest:    digest,
						MediaType: deliveryapis.MediaTypeChartsLayer,
					},
				},
			},
		},
	}
}

func imageDigestString(t *testing.T, image containerv1.Image) string {
	t.Helper()
	digest, err := image.Digest()
	if err != nil {
		t.Fatalf("image Digest() error: %+v", err)
	}
	return digest.String()
}

func fetcherPackageManifest(name, version, digest string) deliveryapis.PackageManifest {
	return deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           "cni",
		Name:           name,
		Version:        version,
		ContentProfile: deliveryapis.ContentProfileAddon,
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   deliveryapis.DefaultPackageOS,
			Arch: "amd64",
		},
		Contents: []deliveryapis.PackageManifestFile{
			{
				Name:      deliveryapis.ContentCharts,
				File:      downloader.ChartFilename,
				Digest:    digest,
				MediaType: deliveryapis.MediaTypeChartsLayer,
			},
		},
	}
}

func fetcherPackageImage(t *testing.T, manifest deliveryapis.PackageManifest, payload stubLayer) containerv1.Image {
	t.Helper()
	layer := packageRootFSLayer(t, manifest, payload.data)
	img, err := mutate.Append(empty.Image, mutate.Addendum{
		Layer:     layer,
		MediaType: types.OCILayer,
	})
	if err != nil {
		t.Fatalf("append package rootfs layer: %+v", err)
	}
	return img
}

func packageRootFSLayer(t *testing.T, manifest deliveryapis.PackageManifest, payload []byte) stubLayer {
	t.Helper()
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %+v", err)
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, dir := range []string{"opt/", "opt/kubeclipper/", "opt/kubeclipper/resource/"} {
		if err = tw.WriteHeader(&tar.Header{Name: dir, Typeflag: tar.TypeDir, Mode: 0755}); err != nil {
			t.Fatalf("write dir: %+v", err)
		}
	}
	if err = tw.WriteHeader(&tar.Header{
		Name: "opt/kubeclipper/resource/kc-package-manifest.json",
		Mode: 0644,
		Size: int64(len(manifestData)),
	}); err != nil {
		t.Fatalf("write manifest header: %+v", err)
	}
	if _, err = tw.Write(manifestData); err != nil {
		t.Fatalf("write manifest: %+v", err)
	}
	file := downloader.ChartFilename
	if len(manifest.Contents) > 0 && manifest.Contents[0].File != "" {
		file = manifest.Contents[0].File
	}
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
	if err = tw.Close(); err != nil {
		t.Fatalf("close rootfs tar: %+v", err)
	}
	return newStubLayer(buf.Bytes(), string(types.OCILayer))
}
