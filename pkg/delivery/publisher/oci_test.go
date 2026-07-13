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
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/types"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

func TestRepositoryRef(t *testing.T) {
	got := repositoryRef("registry.local:5000/", "cri", "containerd", "2.1.0")
	want := "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0"
	if got != want {
		t.Fatalf("repositoryRef() = %q, want %q", got, want)
	}
}

func TestWritePackageManifest(t *testing.T) {
	path, err := writePackageManifest(t.TempDir(), PublishRequest{
		Kind:    "cri",
		Name:    "containerd",
		Version: "2.1.0",
		Arch:    "amd64",
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
	}})
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
