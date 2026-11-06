/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package releasemanifest

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	registryserver "github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/stretchr/testify/require"

	registryconfig "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
)

type forbiddenSourceTransport struct{ t *testing.T }

func (f forbiddenSourceTransport) RoundTrip(*http.Request) (*http.Response, error) {
	f.t.Error("offline sync attempted to contact the source registry")
	return nil, errors.New("source registry access is forbidden")
}

func bundleTestImage(t *testing.T, arch string) v1.Image {
	t.Helper()
	var payload bytes.Buffer
	tw := tar.NewWriter(&payload)
	body := []byte("payload for " + arch)
	if err := tw.WriteHeader(&tar.Header{Name: "payload", Mode: 0600, Size: int64(len(body))}); err != nil {
		t.Fatal(err)
	}
	_, err := tw.Write(body)
	require.NoError(t, err)
	_ = tw.Close()
	layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(payload.Bytes())), nil })
	require.NoError(t, err)
	mediaType := types.DockerLayer
	if arch == ArchARM64 {
		mediaType = types.DockerForeignLayer
	}
	image, err := mutate.Append(empty.Image, mutate.Addendum{Layer: layer, MediaType: mediaType})
	require.NoError(t, err)
	config, configErr := image.ConfigFile()
	require.NoError(t, configErr)
	config.OS, config.Architecture = "linux", arch
	config.Config.Labels = map[string]string{"org.opencontainers.image.revision": "bundle-revision"}
	image, err = mutate.ConfigFile(image, config)
	require.NoError(t, err)
	return image
}

func exportFixture(t *testing.T) (*Manifest, ExportResult) {
	t.Helper()
	manifest, sourceOpts, shutdown := bundleSourceFixture(t)
	result, err := Export(t.Context(), manifest, ExportOptions{Output: filepath.Join(t.TempDir(), "offline"), SourceOptions: sourceOpts})
	require.NoError(t, err)
	shutdown() // Local import must work after the actual source has gone away.
	return manifest, result
}

func bundleSourceFixture(t *testing.T) (*Manifest, []crane.Option, func()) {
	t.Helper()
	server := httptest.NewTLSServer(registryserver.New())
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	require.NoError(t, err)
	transport := rewriteTransport{target: u, base: server.Client().Transport}
	sourceOpts := []crane.Option{crane.WithAuth(authn.Anonymous), crane.WithTransport(transport)}
	images := make(map[string]v1.Image)
	for _, arch := range []string{ArchAMD64, ArchARM64} {
		images[arch] = bundleTestImage(t, arch)
	}
	// Preserve Docker media types too; the OCI envelope must not convert them.
	index := mutate.IndexMediaType(mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: images[ArchAMD64], Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: ArchAMD64}}},
		mutate.IndexAddendum{Add: images[ArchARM64], Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: ArchARM64}}},
	), types.DockerManifestList)
	digest, err := index.Digest()
	require.NoError(t, err)
	manifest := &Manifest{
		APIVersion: APIVersion, Kind: Kind,
		Metadata:   Metadata{Name: "kubeclipper-resources", Version: "v2.0.11", SourceRevision: "bundle-revision"},
		Registries: Registries{Package: OfficialSourcePrefix, Image: OfficialSourcePrefix},
	}
	for _, component := range []string{"kubeclipper", "registry"} {
		version := "v2.0.11"
		if component == "registry" {
			version = "3.1.1"
		}
		target := "package-bootstrap-" + component + ":" + version
		manifest.Artifacts = append(manifest.Artifacts, Artifact{
			Type: ArtifactTypePackage, Component: Component{Kind: "bootstrap", Name: component, Version: version},
			Source: OfficialSourcePrefix + "/" + target, Target: target,
			Platforms: []string{PlatformAMD64, PlatformARM64}, Digest: digest.String(), SourceRevision: "bundle-revision",
		})
	}
	for _, arch := range []string{ArchAMD64, ArchARM64} {
		child, digestErr := images[arch].Digest()
		require.NoError(t, digestErr)
		manifest.Artifacts = append(manifest.Artifacts, Artifact{
			Type: ArtifactTypeRuntime, Component: Component{Kind: "k8s", Name: "k8s", Version: "v1.37.0"},
			Source: OfficialSourcePrefix + "/pause:3.10", Target: "pause:3.10", Platforms: []string{"linux/" + arch}, Digest: child.String(),
		})
	}
	for _, group := range groupArtifacts(manifest.Artifacts) {
		ref, parseErr := name.ParseReference(group.Source)
		require.NoError(t, parseErr)
		require.NoError(t, remote.WriteIndex(ref, index, remote.WithAuth(authn.Anonymous), remote.WithTransport(transport), remote.WithNondistributable))
	}
	return manifest, sourceOpts, server.Close
}

type cancelAfterRegistryBundle struct {
	context.Context
	cancel context.CancelFunc
	path   string
}

func (c cancelAfterRegistryBundle) Err() error {
	if _, err := os.Stat(c.path); err == nil {
		c.cancel()
	}
	return c.Context.Err()
}

func TestExportRetriesAfterRegistryBundleIsWritten(t *testing.T) {
	manifest, sourceOpts, _ := bundleSourceFixture(t)
	opts := ExportOptions{Output: filepath.Join(t.TempDir(), "offline"), SourceOptions: sourceOpts}
	paths, _, err := exportPaths(manifest, opts.Output)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err = Export(cancelAfterRegistryBundle{Context: ctx, cancel: cancel, path: paths.RegistryBundle}, manifest, opts)
	require.ErrorIs(t, err, context.Canceled)
	require.FileExists(t, paths.RegistryBundle)
	require.NoFileExists(t, paths.Bundle)
	temporary, globErr := filepath.Glob(filepath.Join(opts.Output, ".kc-bundle-*"))
	require.NoError(t, globErr)
	require.Empty(t, temporary)
	before, err := os.ReadFile(paths.RegistryBundle)
	require.NoError(t, err)
	beforeInfo, err := os.Stat(paths.RegistryBundle)
	require.NoError(t, err)

	result, err := Export(t.Context(), manifest, opts)
	require.NoError(t, err)
	after, err := os.ReadFile(result.RegistryBundle)
	require.NoError(t, err)
	afterInfo, err := os.Stat(result.RegistryBundle)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.True(t, os.SameFile(beforeInfo, afterInfo))
	require.Equal(t, beforeInfo.ModTime(), afterInfo.ModTime())
	bundle, err := OpenBundle(t.Context(), result.Bundle)
	require.NoError(t, err)
	defer bundle.Close()
	require.Equal(t, manifest, bundle.Manifest)
}

func TestExportRejectsInvalidExistingRegistryBundle(t *testing.T) {
	manifest, sourceOpts, _ := bundleSourceFixture(t)
	for _, mode := range []string{"corrupt", "different-digest", "different-reference"} {
		t.Run(mode, func(t *testing.T) {
			opts := ExportOptions{Output: filepath.Join(t.TempDir(), "offline"), SourceOptions: sourceOpts}
			paths, target, pathErr := exportPaths(manifest, opts.Output)
			require.NoError(t, pathErr)
			require.NoError(t, os.MkdirAll(opts.Output, 0700))
			if mode == "corrupt" {
				require.NoError(t, os.WriteFile(paths.RegistryBundle, []byte("corrupt"), 0600))
			} else {
				local, layoutErr := layout.Write(t.TempDir(), empty.Index)
				require.NoError(t, layoutErr)
				if mode == "different-reference" {
					target = "package-bootstrap-etcd:3.5.21"
				}
				require.NoError(t, appendBundleContent(local, target, bundleTestImage(t, ArchAMD64)))
				require.NoError(t, writeBundleArchive(t.Context(), string(local), paths.RegistryBundle))
			}
			before, readErr := os.ReadFile(paths.RegistryBundle)
			require.NoError(t, readErr)
			_, exportErr := Export(t.Context(), manifest, opts)
			require.Error(t, exportErr)
			if mode == "different-digest" {
				require.Contains(t, exportErr.Error(), "registry bundle digest mismatch")
			}
			after, readErr := os.ReadFile(paths.RegistryBundle)
			require.NoError(t, readErr)
			require.Equal(t, before, after)
			require.NoFileExists(t, paths.Bundle)
		})
	}
}

func TestWriteBundleArchiveDoesNotOverwriteExistingOutput(t *testing.T) {
	_, result := exportFixture(t)
	bundle, err := OpenBundle(t.Context(), result.RegistryBundle)
	require.NoError(t, err)
	defer bundle.Close()
	output := filepath.Join(t.TempDir(), "existing.tar.gz")
	require.NoError(t, os.WriteFile(output, []byte("existing output"), 0600))
	err = writeBundleArchive(t.Context(), string(bundle.path), output)
	require.ErrorIs(t, err, os.ErrExist)
	data, readErr := os.ReadFile(output)
	require.NoError(t, readErr)
	require.Equal(t, "existing output", string(data))
}

func TestExportOutputDirectory(t *testing.T) {
	manifest, result := exportFixture(t)
	require.Equal(t, "kubeclipper-offline-v2.0.11.tar.gz", filepath.Base(result.Bundle))
	require.Equal(t, "registry-3.1.1.tar.gz", filepath.Base(result.RegistryBundle))
	require.Equal(t, filepath.Dir(result.Bundle), filepath.Dir(result.RegistryBundle))
	t.Chdir(t.TempDir())
	defaults, _, err := exportPaths(manifest, "")
	require.NoError(t, err)
	require.Equal(t, filepath.Join("offline", "kubeclipper-offline-v2.0.11.tar.gz"), defaults.Bundle)
	require.Equal(t, filepath.Join("offline", "registry-3.1.1.tar.gz"), defaults.RegistryBundle)
	_, _, err = exportPaths(manifest, filepath.Dir(result.Bundle))
	require.Error(t, err)
	require.Contains(t, err.Error(), "output already exists")
	_, _, err = exportPaths(manifest, result.Bundle)
	require.Error(t, err)
	require.Contains(t, err.Error(), "output must be a directory")
}

func TestBundleMultiArchRoundTrip(t *testing.T) {
	manifest, result := exportFixture(t)
	bundle, err := OpenBundle(t.Context(), result.Bundle)
	require.NoError(t, err)
	defer bundle.Close()
	small, err := OpenBundle(t.Context(), result.RegistryBundle)
	require.NoError(t, err)
	defer small.Close()
	if small.Manifest != nil {
		t.Fatal("Registry bootstrap bundle must not carry the complete release manifest")
	}
	if _, err = small.source(manifest.Artifacts[0].Target); err == nil {
		t.Fatal("Registry bundle unexpectedly contains KubeClipper")
	}
	for _, arch := range []string{ArchAMD64, ArchARM64} {
		image, imageErr := small.RegistryImage(arch)
		require.NoError(t, imageErr)
		config, configErr := image.ConfigFile()
		require.NoError(t, configErr)
		if config.Architecture != arch {
			t.Fatalf("Registry image architecture = %q, want %q", config.Architecture, arch)
		}
	}
	_, err = small.RegistryImage("riscv64")
	require.Error(t, err)
	target := httptest.NewServer(registryserver.New())
	defer target.Close()
	for _, arch := range []string{ArchAll, ArchAMD64, ArchARM64} {
		prefix := strings.TrimPrefix(target.URL, "http://") + "/" + arch
		opts := &SyncOptions{Registry: prefix, Arch: arch, Bundle: bundle,
			Config:        &registryconfig.Config{Registry: prefix, Scheme: "http"},
			SourceOptions: []crane.Option{crane.WithTransport(forbiddenSourceTransport{t})},
		}
		copied, err := Sync(t.Context(), bundle.Manifest, opts)
		require.NoError(t, err)
		require.Equal(t, 3, copied.Copied)
		for _, group := range groupArtifacts(manifest.Artifacts) {
			_, content, validateErr := validateSource(mustBundleSource(t, bundle, group.Target), selectedArtifacts(group.Artifacts, arch), arch)
			require.NoError(t, validateErr)
			var expected v1.Hash
			var digestErr error
			if image, ok := content.(v1.Image); ok {
				expected, digestErr = image.Digest()
			} else {
				expectedIndex, ok := content.(v1.ImageIndex)
				require.True(t, ok)
				expected, digestErr = expectedIndex.Digest()
			}
			require.NoError(t, digestErr)
			actual, getErr := crane.Get(prefix+"/"+group.Target, crane.Insecure)
			require.NoError(t, getErr)
			require.Equal(t, expected, actual.Digest)
			if arch == ArchAll {
				checkBundleLayers(t, actual)
			}
		}
		skipped, err := Sync(t.Context(), bundle.Manifest, opts)
		require.NoError(t, err)
		require.Equal(t, 3, skipped.Skipped)
		if err = crane.Push(empty.Image, prefix+"/"+manifest.Artifacts[0].Target, crane.Insecure); err != nil {
			t.Fatal(err)
		}
		if _, err = Sync(t.Context(), bundle.Manifest, opts); err == nil || !strings.Contains(err.Error(), "conflict") {
			t.Fatalf("tag conflict was not rejected: %v", err)
		}
	}
}

func checkBundleLayers(t *testing.T, actual *remote.Descriptor) {
	t.Helper()
	require.Equal(t, types.DockerManifestList, actual.MediaType)
	index, err := actual.ImageIndex()
	require.NoError(t, err)
	for _, platform := range []string{PlatformAMD64, PlatformARM64} {
		descriptor, platformErr := platformDescriptor(index, platform)
		require.NoError(t, platformErr)
		image, imageErr := index.Image(descriptor.Digest)
		require.NoError(t, imageErr)
		layers, layersErr := image.Layers()
		require.NoError(t, layersErr)
		require.Len(t, layers, 1)
		reader, readErr := layers[0].Compressed()
		require.NoError(t, readErr)
		_, copyErr := io.Copy(io.Discard, reader)
		_ = reader.Close()
		require.NoError(t, copyErr)
	}
}

func mustBundleSource(t *testing.T, bundle *Bundle, ref string) *artifactSource {
	t.Helper()
	source, err := bundle.source(ref)
	require.NoError(t, err)
	return source
}

func TestBundleRejectsDamagedContent(t *testing.T) {
	_, result := exportFixture(t)
	for _, mode := range []string{"missing", "corrupt", "metadata"} {
		t.Run(mode, func(t *testing.T) {
			bundle, err := OpenBundle(t.Context(), result.Bundle)
			require.NoError(t, err)
			defer bundle.Close()
			files, globErr := filepath.Glob(filepath.Join(string(bundle.path), "blobs", "sha256", "*"))
			require.NoError(t, globErr)
			require.NotEmpty(t, files)
			switch mode {
			case "missing":
				err = os.Remove(files[0])
			case "corrupt":
				err = os.WriteFile(files[0], []byte("corrupt"), 0600)
			case "metadata":
				err = os.WriteFile(filepath.Join(string(bundle.path), "index.json"), []byte("corrupt"), 0600)
			}
			require.NoError(t, err)
			if mode == "corrupt" {
				exportErr := writeBundleArchive(t.Context(), string(bundle.path), filepath.Join(t.TempDir(), "invalid-export.tar.gz"))
				require.Error(t, exportErr)
				require.Contains(t, exportErr.Error(), "blob digest mismatch during export")
			}
			archive := damagedBundleArchive(t, string(bundle.path))
			damaged, openErr := OpenBundle(t.Context(), archive)
			if damaged != nil {
				defer damaged.Close()
			}
			reasons := map[string]string{"missing": "incomplete bundle", "corrupt": "blob digest mismatch", "metadata": "bundle checksum mismatch"}
			require.Error(t, openErr)
			require.Contains(t, openErr.Error(), reasons[mode])
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if bundle, err := OpenBundle(ctx, result.Bundle); err == nil {
		_ = bundle.Close()
		t.Fatal("canceled bundle read succeeded")
	}
	data, readErr := os.ReadFile(result.Bundle)
	require.NoError(t, readErr)
	truncated, createErr := os.CreateTemp(t.TempDir(), "truncated-*.tar.gz")
	require.NoError(t, createErr)
	_, writeErr := truncated.Write(data[:len(data)-8])
	require.NoError(t, writeErr)
	require.NoError(t, truncated.Close())
	if bundle, err := OpenBundle(t.Context(), truncated.Name()); err == nil {
		_ = bundle.Close()
		t.Fatal("truncated gzip trailer was accepted")
	}
}

func damagedBundleArchive(t *testing.T, dir string) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "damaged-*.tar.gz")
	require.NoError(t, err)
	gz := gzip.NewWriter(file)
	tw := tar.NewWriter(gz)
	// Deliberately bypass the production writer's digest checks to construct a corrupt input.
	require.NoError(t, tw.AddFS(os.DirFS(dir)))
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())
	require.NoError(t, file.Close())
	return file.Name()
}

func TestBundleRejectsUnsafeEntries(t *testing.T) {
	for _, entry := range []tar.Header{{Name: "../outside", Typeflag: tar.TypeReg}, {Name: "index.json", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"}} {
		path := filepath.Join(t.TempDir(), "unsafe.tar.gz")
		file, createErr := os.Create(path)
		require.NoError(t, createErr)
		gz := gzip.NewWriter(file)
		tw := tar.NewWriter(gz)
		require.NoError(t, tw.WriteHeader(&entry))
		_ = tw.Close()
		_ = gz.Close()
		_ = file.Close()
		if bundle, err := OpenBundle(t.Context(), path); err == nil {
			_ = bundle.Close()
			t.Fatalf("unsafe archive entry accepted: %+v", entry)
		}
	}
}
