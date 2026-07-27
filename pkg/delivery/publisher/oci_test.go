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

package publisher

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	containerregistry "github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
)

func TestRepositoryRef(t *testing.T) {
	got := repositoryRef("registry.local:5000/", "cri", "containerd", "2.1.0")
	want := "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0"
	if got != want {
		t.Fatalf("repositoryRef() = %q, want %q", got, want)
	}
}

func TestIsStableArtifactVersion(t *testing.T) {
	tests := map[string]bool{
		"v2.0.0":      true,
		"2.2.4":       true,
		"v1":          true,
		"latest":      false,
		"release-2.0": false,
	}
	for version, want := range tests {
		if got := isStableArtifactVersion(version); got != want {
			t.Errorf("isStableArtifactVersion(%q) = %t, want %t", version, got, want)
		}
	}
}

func TestWritePackageManifest(t *testing.T) {
	path, err := writePackageManifest(t.TempDir(), PublishRequest{
		Kind:           "cri",
		Name:           "containerd",
		Version:        "2.1.0",
		Arch:           "amd64",
		SourceRevision: "abc123",
	}, deliveryapis.ContentProfileRuntime, []payloadFile{
		{name: deliveryapis.ContentConfigs, file: "configs.tar.gz", digest: "sha256:aaaa"},
	}, nil)
	if err != nil {
		t.Fatalf("writePackageManifest() error: %+v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %+v", err)
	}
	var manifest deliveryapis.PackageManifest
	if err = json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("unmarshal manifest: %+v", err)
	}
	if manifest.Kind != "cri" || manifest.Name != "containerd" || manifest.Version != "2.1.0" {
		t.Fatalf("manifest identity = %+v", manifest)
	}
	if manifest.Platform.Arch != "amd64" {
		t.Fatalf("platform arch = %q", manifest.Platform.Arch)
	}
	if manifest.SourceRevision != "abc123" {
		t.Fatalf("source revision = %q", manifest.SourceRevision)
	}
	if len(manifest.Contents) != 1 {
		t.Fatalf("content count = %d, want 1", len(manifest.Contents))
	}
	if manifest.Contents[0].MediaType != deliveryapis.MediaTypeConfigsLayer {
		t.Fatalf("content mediaType = %q", manifest.Contents[0].MediaType)
	}
}

func TestBuildPackageIndexReplacesSameArch(t *testing.T) {
	index := buildPackageIndex(empty.Index, empty.Image, "amd64")
	index = buildPackageIndex(index, empty.Image, "arm64")
	index = buildPackageIndex(index, empty.Image, "amd64")

	manifest, err := index.IndexManifest()
	if err != nil {
		t.Fatalf("IndexManifest() error: %+v", err)
	}
	if len(manifest.Manifests) != 2 {
		t.Fatalf("manifest count = %d, want 2", len(manifest.Manifests))
	}
	archs := map[string]bool{}
	for _, descriptor := range manifest.Manifests {
		if descriptor.Platform == nil {
			t.Fatalf("descriptor platform is nil")
		}
		archs[descriptor.Platform.Architecture] = true
	}
	if !archs["amd64"] || !archs["arm64"] {
		t.Fatalf("archs = %+v, want amd64 and arm64", archs)
	}
}

func TestExistingPackageIndexTreatsOnlyNotFoundAsEmpty(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		server := httptest.NewServer(containerregistry.New())
		defer server.Close()
		registry := strings.TrimPrefix(server.URL, "http://")
		config := &deliveryregistry.Config{Registry: registry, Scheme: deliveryregistry.SchemeHTTP}
		ref, options := testRemoteReference(t, registry+"/team/package:v1.0.0", config)

		index, err := existingPackageIndex(ref, options...)
		if err != nil {
			t.Fatalf("existingPackageIndex() 404 error = %v", err)
		}
		manifest, err := index.IndexManifest()
		if err != nil {
			t.Fatalf("IndexManifest() error = %v", err)
		}
		if len(manifest.Manifests) != 0 {
			t.Fatalf("404 index manifest count = %d, want 0", len(manifest.Manifests))
		}
	})

	t.Run("forbidden", func(t *testing.T) {
		var manifestWrites atomic.Int64
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v2/" {
				w.WriteHeader(http.StatusOK)
				return
			}
			if r.Method == http.MethodPut {
				manifestWrites.Add(1)
			}
			http.Error(w, "forbidden", http.StatusForbidden)
		}))
		defer server.Close()
		registry := strings.TrimPrefix(server.URL, "http://")
		config := &deliveryregistry.Config{Registry: registry, Scheme: deliveryregistry.SchemeHTTP}
		ref, options := testRemoteReference(t, registry+"/team/package:v1.0.0", config)

		if _, err := existingPackageIndex(ref, options...); err == nil || !strings.Contains(err.Error(), "403") {
			t.Fatalf("existingPackageIndex() forbidden error = %v", err)
		}
		if err := pushPackageIndex(t.Context(), ref.Name(), testPackageImage(t, "forbidden"), "amd64", config); err == nil || !strings.Contains(err.Error(), "403") {
			t.Fatalf("pushPackageIndex() forbidden error = %v", err)
		}
		if got := manifestWrites.Load(); got != 0 {
			t.Fatalf("pushPackageIndex() wrote %d manifests after a forbidden read", got)
		}
	})
}

func TestPushPackageIndexStableTagIsIdempotentAndRejectsConflict(t *testing.T) {
	var manifestWrites atomic.Int64
	registryHandler := containerregistry.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/manifests/") {
			manifestWrites.Add(1)
		}
		registryHandler.ServeHTTP(w, r)
	}))
	defer server.Close()
	registry := strings.TrimPrefix(server.URL, "http://")
	config := &deliveryregistry.Config{Registry: registry, Scheme: deliveryregistry.SchemeHTTP}
	target := registry + "/kubeclipper/packages/cri/containerd:2.2.4"
	firstImage := testPackageImage(t, "first")
	firstDigest, err := firstImage.Digest()
	if err != nil {
		t.Fatal(err)
	}

	if err = pushPackageIndex(t.Context(), target, firstImage, "amd64", config); err != nil {
		t.Fatalf("first pushPackageIndex() error = %v", err)
	}
	writesAfterFirstPush := manifestWrites.Load()
	if writesAfterFirstPush == 0 {
		t.Fatal("first package push did not write a manifest")
	}
	if err = pushPackageIndex(t.Context(), target, firstImage, "amd64", config); err != nil {
		t.Fatalf("idempotent pushPackageIndex() error = %v", err)
	}
	if got := manifestWrites.Load(); got != writesAfterFirstPush {
		t.Fatalf("idempotent push wrote manifests: got %d writes, want %d", got, writesAfterFirstPush)
	}

	conflictingImage := testPackageImage(t, "conflict")
	err = pushPackageIndex(t.Context(), target, conflictingImage, "amd64", config)
	if err == nil || !strings.Contains(err.Error(), "package tag conflict") {
		t.Fatalf("conflicting pushPackageIndex() error = %v", err)
	}
	if got := manifestWrites.Load(); got != writesAfterFirstPush {
		t.Fatalf("conflicting push wrote manifests: got %d writes, want %d", got, writesAfterFirstPush)
	}

	ref, options := testRemoteReference(t, target, config)
	index, err := existingPackageIndex(ref, options...)
	if err != nil {
		t.Fatalf("existingPackageIndex() error = %v", err)
	}
	actualDigest, found, err := packagePlatformDigest(index, "linux", "amd64")
	if err != nil {
		t.Fatalf("packagePlatformDigest() error = %v", err)
	}
	if !found || actualDigest != firstDigest {
		t.Fatalf("published amd64 digest = %s, found=%t, want %s", actualDigest, found, firstDigest)
	}
}

func TestPushPackageIndexConcurrentArchitectures(t *testing.T) {
	server := httptest.NewServer(containerregistry.New())
	defer server.Close()
	registry := strings.TrimPrefix(server.URL, "http://")
	config := &deliveryregistry.Config{Registry: registry, Scheme: deliveryregistry.SchemeHTTP}
	target := registry + "/kubeclipper/packages/k8s/k8s:v1.36.1"
	images := map[string]v1.Image{
		"amd64": testPackageImage(t, "amd64"),
		"arm64": testPackageImage(t, "arm64"),
	}

	start := make(chan struct{})
	errorsCh := make(chan error, len(images))
	var wg sync.WaitGroup
	for arch, img := range images {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errorsCh <- pushPackageIndex(t.Context(), target, img, arch, config)
		}()
	}
	close(start)
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent pushPackageIndex() error = %v", err)
		}
	}

	ref, options := testRemoteReference(t, target, config)
	index, err := existingPackageIndex(ref, options...)
	if err != nil {
		t.Fatalf("existingPackageIndex() error = %v", err)
	}
	manifest, err := index.IndexManifest()
	if err != nil {
		t.Fatalf("IndexManifest() error = %v", err)
	}
	if len(manifest.Manifests) != len(images) {
		t.Fatalf("manifest count = %d, want %d", len(manifest.Manifests), len(images))
	}
	for arch, img := range images {
		want, err := img.Digest()
		if err != nil {
			t.Fatal(err)
		}
		got, found, err := packagePlatformDigest(index, "linux", arch)
		if err != nil {
			t.Fatalf("packagePlatformDigest(%s) error = %v", arch, err)
		}
		if !found || got != want {
			t.Fatalf("platform %s digest = %s, found=%t, want %s", arch, got, found, want)
		}
	}
}

func TestPushPackageIndexAuthenticatedTLSMergesArchitectures(t *testing.T) {
	server := httptest.NewTLSServer(testBasicAuth("robot$kc", "token", containerregistry.New()))
	defer server.Close()
	registry := strings.TrimPrefix(server.URL, "https://") + "/team-a"
	config := &deliveryregistry.Config{
		Registry: registry,
		Scheme:   deliveryregistry.SchemeHTTPS,
		Username: "robot$kc",
		Password: "token",
		CA:       testServerCertificatePEM(t, server),
	}
	target := registry + "/kubeclipper/packages/cri/containerd:2.2.4"
	for _, arch := range []string{"amd64", "arm64"} {
		if err := pushPackageIndex(t.Context(), target, empty.Image, arch, config); err != nil {
			t.Fatalf("pushPackageIndex(%s) error = %v", arch, err)
		}
	}
	opts, err := config.CraneOptions(t.Context())
	if err != nil {
		t.Fatalf("CraneOptions() error = %v", err)
	}
	desc, err := crane.Get(target, opts...)
	if err != nil {
		t.Fatalf("crane.Get() error = %v", err)
	}
	index, err := desc.ImageIndex()
	if err != nil {
		t.Fatalf("ImageIndex() error = %v", err)
	}
	manifest, err := index.IndexManifest()
	if err != nil {
		t.Fatalf("IndexManifest() error = %v", err)
	}
	if len(manifest.Manifests) != 2 {
		t.Fatalf("manifest count = %d, want 2", len(manifest.Manifests))
	}
}

func TestPushPackageIndexRejectsMismatchedRegistryConfig(t *testing.T) {
	config := &deliveryregistry.Config{Registry: "harbor.example.com/team-a", Scheme: deliveryregistry.SchemeHTTPS}
	if err := config.ValidateRegistry("harbor.example.com/team-b"); err == nil {
		t.Fatal("ValidateRegistry() mismatch error = nil")
	}
}

func TestInspectPackageContentsRejectsEmbeddedRuntimeImages(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "k8s", "v1.36.0", "amd64")
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatalf("mkdir base: %+v", err)
	}
	for _, file := range []string{"configs.tar.gz", "images.tar.gz"} {
		if err := writePayloadArchive(filepath.Join(base, file), file); err != nil {
			t.Fatalf("write %s: %+v", file, err)
		}
	}

	if _, err := inspectPackageContents(root, "k8s", "v1.36.0", "amd64", deliveryapis.ContentProfileK8s); err == nil {
		t.Fatalf("inspectPackageContents() expected embedded runtime image archive error")
	}
}

func TestWritePackageManifestExternalHelmChart(t *testing.T) {
	path, err := writePackageManifest(t.TempDir(), PublishRequest{
		Kind:    "cni",
		Name:    "calico",
		Version: "v3.31.5",
		Arch:    "amd64",
	}, deliveryapis.ContentProfileAddon, nil, []deliveryapis.ArtifactContent{{
		Name:      deliveryapis.ContentCharts,
		File:      "tigera-operator-v3.31.5.tgz",
		MediaType: deliveryapis.MediaTypeChartsLayer,
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportHelmOCI,
			Ref:    "registry.local/kubeclipper/charts/tigera-operator",
			Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		},
	}})
	if err != nil {
		t.Fatalf("writePackageManifest() error: %+v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read manifest: %+v", err)
	}
	var manifest deliveryapis.PackageManifest
	if err = json.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("unmarshal manifest: %+v", err)
	}
	if len(manifest.Contents) != 1 || manifest.Contents[0].Transport.Type != deliveryapis.TransportHelmOCI {
		t.Fatalf("manifest contents = %+v", manifest.Contents)
	}
}

func TestBuildArtifactImageUsesStandardRootFSLayer(t *testing.T) {
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, packageManifestFile)
	if err := os.WriteFile(manifestPath, []byte(`{"schemaVersion":1}`), 0644); err != nil {
		t.Fatalf("write manifest: %+v", err)
	}
	payloadPath := filepath.Join(dir, "configs.tar.gz")
	if err := writePayloadArchive(payloadPath, "configs"); err != nil {
		t.Fatalf("write payload: %+v", err)
	}
	img, err := buildArtifactImage(manifestPath, []payloadFile{{
		name: deliveryapis.ContentConfigs,
		path: payloadPath,
		file: "configs.tar.gz",
	}}, PublishRequest{Kind: "k8s", Name: "k8s", Version: "v1.36.1", SourceRevision: "abc123"})
	if err != nil {
		t.Fatalf("buildArtifactImage() error: %+v", err)
	}
	manifest, err := img.Manifest()
	if err != nil {
		t.Fatalf("Manifest() error: %+v", err)
	}
	if string(manifest.MediaType) != string(types.OCIManifestSchema1) {
		t.Fatalf("manifest mediaType = %q", manifest.MediaType)
	}
	if len(manifest.Layers) != 1 || manifest.Layers[0].MediaType != types.OCILayer {
		t.Fatalf("layers = %+v", manifest.Layers)
	}
	config, err := img.ConfigFile()
	if err != nil {
		t.Fatalf("ConfigFile() error: %+v", err)
	}
	if config.Config.Labels["org.opencontainers.image.revision"] != "abc123" {
		t.Fatalf("revision label = %q", config.Config.Labels["org.opencontainers.image.revision"])
	}
	if config.Config.Labels["org.opencontainers.image.version"] != "v1.36.1" {
		t.Fatalf("version label = %q", config.Config.Labels["org.opencontainers.image.version"])
	}
	layers, err := img.Layers()
	if err != nil {
		t.Fatalf("Layers() error: %+v", err)
	}
	reader, err := layers[0].Uncompressed()
	if err != nil {
		t.Fatalf("Uncompressed() error: %+v", err)
	}
	defer reader.Close()
	files := map[string]bool{}
	tr := tar.NewReader(reader)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read rootfs tar: %+v", err)
		}
		files[header.Name] = true
	}
	if !files["opt/kubeclipper/resource/kc-package-manifest.json"] || !files["opt/kubeclipper/resource/configs.tar.gz"] {
		t.Fatalf("rootfs files = %+v", files)
	}
}

func TestHelmChartImageUsesHelmOCIMediaTypes(t *testing.T) {
	var buf bytes.Buffer
	if err := writeTestArchiveTo(&buf, map[string]string{
		"tigera-operator/Chart.yaml":  "apiVersion: v2\nname: tigera-operator\nversion: v3.31.5\n",
		"tigera-operator/values.yaml": "installation: {}\n",
	}); err != nil {
		t.Fatalf("write chart archive: %+v", err)
	}
	configData, metadata, err := helmChartConfig(buf.Bytes())
	if err != nil {
		t.Fatalf("helmChartConfig() error: %+v", err)
	}
	if metadata.Name != "tigera-operator" || metadata.Version != "v3.31.5" {
		t.Fatalf("metadata = %+v", metadata)
	}
	img := newHelmChartImage(configData, buf.Bytes())
	manifest, err := img.Manifest()
	if err != nil {
		t.Fatalf("Manifest() error: %+v", err)
	}
	if string(manifest.Config.MediaType) != deliveryapis.MediaTypeHelmConfig {
		t.Fatalf("config mediaType = %q", manifest.Config.MediaType)
	}
	if len(manifest.Layers) != 1 || string(manifest.Layers[0].MediaType) != deliveryapis.MediaTypeHelmChartLayer {
		t.Fatalf("layers = %+v", manifest.Layers)
	}
	chartHash, _, err := v1.SHA256(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("chart digest: %+v", err)
	}
	if manifest.Layers[0].Digest != chartHash {
		t.Fatalf("chart layer digest = %s, want %s", manifest.Layers[0].Digest, chartHash)
	}
}

func TestPublishHelmChartStableTagIsIdempotentAndRejectsConflict(t *testing.T) {
	var manifestWrites atomic.Int64
	registryHandler := containerregistry.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.Contains(r.URL.Path, "/manifests/") {
			manifestWrites.Add(1)
		}
		registryHandler.ServeHTTP(w, r)
	}))
	defer server.Close()
	registry := strings.TrimPrefix(server.URL, "http://")
	config := &deliveryregistry.Config{Registry: registry, Scheme: deliveryregistry.SchemeHTTP}
	chart := filepath.Join(t.TempDir(), "tigera-operator-v3.31.5.tgz")
	if err := writeTestArchive(chart, map[string]string{
		"tigera-operator/Chart.yaml":  "apiVersion: v2\nname: tigera-operator\nversion: v3.31.5\n",
		"tigera-operator/values.yaml": "installation: {}\n",
	}); err != nil {
		t.Fatal(err)
	}
	request := HelmChartPublishRequest{
		ChartPath:        chart,
		Registry:         registry,
		RepositoryPrefix: deliveryapis.ChartRepositoryPrefix,
		Name:             "tigera-operator",
		RegistryConfig:   config,
	}

	first, err := PublishHelmChart(request)
	if err != nil {
		t.Fatalf("first PublishHelmChart() error = %v", err)
	}
	writesAfterFirstPush := manifestWrites.Load()
	second, err := PublishHelmChart(request)
	if err != nil {
		t.Fatalf("idempotent PublishHelmChart() error = %v", err)
	}
	if second.Digest != first.Digest {
		t.Fatalf("idempotent chart digest = %s, want %s", second.Digest, first.Digest)
	}
	if got := manifestWrites.Load(); got != writesAfterFirstPush {
		t.Fatalf("idempotent chart push wrote manifests: got %d writes, want %d", got, writesAfterFirstPush)
	}

	conflictingChart := filepath.Join(t.TempDir(), "tigera-operator-conflict.tgz")
	if err = writeTestArchive(conflictingChart, map[string]string{
		"tigera-operator/Chart.yaml":  "apiVersion: v2\nname: tigera-operator\nversion: v3.31.5\n",
		"tigera-operator/values.yaml": "installation:\n  enabled: false\n",
	}); err != nil {
		t.Fatal(err)
	}
	request.ChartPath = conflictingChart
	if _, err = PublishHelmChart(request); err == nil || !strings.Contains(err.Error(), "helm chart tag conflict") {
		t.Fatalf("conflicting PublishHelmChart() error = %v", err)
	}
	if got := manifestWrites.Load(); got != writesAfterFirstPush {
		t.Fatalf("conflicting chart push wrote manifests: got %d writes, want %d", got, writesAfterFirstPush)
	}

	craneOptions, err := config.CraneOptions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	actual, err := crane.Digest(first.Ref+":"+first.Version, craneOptions...)
	if err != nil {
		t.Fatalf("read published chart digest: %v", err)
	}
	if actual != first.Digest {
		t.Fatalf("published chart digest = %s, want %s", actual, first.Digest)
	}
}

func TestInspectPackageContentsRejectsInvalidArchivePayload(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "containerd", "2.2.4", "amd64")
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatalf("mkdir base: %+v", err)
	}
	if err := os.WriteFile(filepath.Join(base, "configs.tar.gz"), []byte("not a gzip archive"), 0644); err != nil {
		t.Fatalf("write invalid payload: %+v", err)
	}

	_, err := inspectPackageContents(root, "containerd", "2.2.4", "amd64", deliveryapis.ContentProfileRuntime)
	if err == nil {
		t.Fatalf("inspectPackageContents() expected invalid gzip error")
	}
}

func TestInspectPackageContentsBinary(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "kubeclipper", "v1.7.0", "amd64")
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatalf("mkdir base: %+v", err)
	}
	if err := os.WriteFile(filepath.Join(base, "kubeclipper-agent"), []byte("binary"), 0755); err != nil {
		t.Fatalf("write binary: %+v", err)
	}

	payloads, err := inspectPackageContents(root, "kubeclipper", "v1.7.0", "amd64", deliveryapis.ContentProfileBinary)
	if err != nil {
		t.Fatalf("inspectPackageContents() error: %+v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("payload count = %d, want 1", len(payloads))
	}
	if payloads[0].name != "kubeclipper-agent" {
		t.Fatalf("payload name = %q", payloads[0].name)
	}
	if payloads[0].file != "kubeclipper-agent" {
		t.Fatalf("payload file = %q", payloads[0].file)
	}
	if payloads[0].mediaType != deliveryapis.MediaTypeBinaryLayer {
		t.Fatalf("payload mediaType = %q", payloads[0].mediaType)
	}
}

func TestInspectPackageContentsBinaryAcceptsMultipleFiles(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "kubeclipper", "v1.7.0", "amd64")
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatalf("mkdir base: %+v", err)
	}
	for _, file := range []string{"kubeclipper-agent", "etcdctl"} {
		if err := os.WriteFile(filepath.Join(base, file), []byte(file), 0755); err != nil {
			t.Fatalf("write %s: %+v", file, err)
		}
	}

	payloads, err := inspectPackageContents(root, "kubeclipper", "v1.7.0", "amd64", deliveryapis.ContentProfileBinary)
	if err != nil {
		t.Fatalf("inspectPackageContents() error: %+v", err)
	}
	if len(payloads) != 2 {
		t.Fatalf("payload count = %d, want 2", len(payloads))
	}
}

func TestInspectPackageContentsBinaryIgnoresMetadataFiles(t *testing.T) {
	root := t.TempDir()
	base := filepath.Join(root, "etcd", "3.5.21", "arm64")
	if err := os.MkdirAll(base, 0755); err != nil {
		t.Fatalf("mkdir base: %+v", err)
	}
	for file, data := range map[string]string{
		"etcd":      "binary",
		"._etcd":    "appledouble metadata",
		".DS_Store": "finder metadata",
	} {
		if err := os.WriteFile(filepath.Join(base, file), []byte(data), 0755); err != nil {
			t.Fatalf("write %s: %+v", file, err)
		}
	}

	payloads, err := inspectPackageContents(root, "etcd", "3.5.21", "arm64", deliveryapis.ContentProfileBinary)
	if err != nil {
		t.Fatalf("inspectPackageContents() error: %+v", err)
	}
	if len(payloads) != 1 {
		t.Fatalf("payload count = %d, want 1", len(payloads))
	}
	if payloads[0].name != "etcd" {
		t.Fatalf("payload name = %q", payloads[0].name)
	}
}

func TestExtractTarGz(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "pkg.tar.gz")
	if err := writeTestArchive(archive, map[string]string{
		"v1.36.0/amd64/configs.tar.gz": "configs",
	}); err != nil {
		t.Fatalf("writeTestArchive() error: %+v", err)
	}

	dst := filepath.Join(dir, "out")
	if err := extractTarGz(archive, dst); err != nil {
		t.Fatalf("extractTarGz() error: %+v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "v1.36.0", "amd64", "configs.tar.gz")); err != nil {
		t.Fatalf("extracted configs missing: %+v", err)
	}
}

func TestValidatePublishContents(t *testing.T) {
	err := validatePublishContents(deliveryapis.ContentProfileRuntime, []deliveryapis.ArtifactContent{})
	if err == nil {
		t.Fatalf("expected validation error for incomplete runtime profile")
	}
}

func TestValidatePublishContentsBinary(t *testing.T) {
	err := validatePublishContents(deliveryapis.ContentProfileBinary, []deliveryapis.ArtifactContent{
		{Name: "kubeclipper-agent", File: "kubeclipper-agent", MediaType: deliveryapis.MediaTypeBinaryLayer},
	})
	if err != nil {
		t.Fatalf("validatePublishContents() error: %+v", err)
	}
}

func writeTestArchive(path string, files map[string]string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return writeTestArchiveTo(f, files)
}

func writeTestArchiveTo(w io.Writer, files map[string]string) error {
	gzw := gzip.NewWriter(w)
	defer gzw.Close()
	tw := tar.NewWriter(gzw)
	defer tw.Close()
	for name, content := range files {
		if err := tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0644,
			Size: int64(len(content)),
		}); err != nil {
			return err
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			return err
		}
	}
	return nil
}

func writePayloadArchive(path, payloadName string) error {
	return writeTestArchive(path, map[string]string{
		"payload/" + payloadName + ".txt": payloadName,
	})
}

func testPackageImage(t *testing.T, label string) v1.Image {
	t.Helper()
	img := mutate.MediaType(empty.Image, types.OCIManifestSchema1)
	img = mutate.ConfigMediaType(img, types.OCIConfigJSON)
	config, err := img.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	config.Config.Labels = map[string]string{"test.kubeclipper.io/content": label}
	img, err = mutate.ConfigFile(img, config)
	if err != nil {
		t.Fatal(err)
	}
	return img
}

func testRemoteReference(t *testing.T, target string, config *deliveryregistry.Config) (name.Reference, []remote.Option) {
	t.Helper()
	craneOptions, err := config.CraneOptions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	parsedOptions := crane.GetOptions(craneOptions...)
	ref, err := name.ParseReference(target, parsedOptions.Name...)
	if err != nil {
		t.Fatal(err)
	}
	return ref, parsedOptions.Remote
}

func testBasicAuth(username, password string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != username || pass != password {
			w.Header().Set("WWW-Authenticate", `Basic realm="registry"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func testServerCertificatePEM(t *testing.T, server *httptest.Server) string {
	t.Helper()
	certificate := server.Certificate()
	if certificate == nil {
		t.Fatal("test TLS server certificate is nil")
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}))
}
