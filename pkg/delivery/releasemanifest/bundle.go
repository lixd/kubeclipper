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
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	imagename "github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/layout"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"sigs.k8s.io/yaml"
)

const (
	bundlePrivateDirMode os.FileMode = 0700
	bundleOutputDirMode  os.FileMode = 0755
	bundleBufferSize                 = 256 * 1024
	bundleRefName                    = "org.opencontainers.image.ref.name"
	bundleLayoutFile                 = "oci-layout"
	bundleIndexFile                  = "index.json"
	bundleManifestFile               = "release-manifest.yaml"
	bundleChecksumsFile              = "SHA256SUMS"
	bootstrapKind                    = "bootstrap"
	bundlePrivateMode    os.FileMode = 0600
)

type ExportOptions struct {
	Output        string
	SourceOptions []crane.Option
	Out           io.Writer
}

type ExportResult struct {
	Bundle         string
	RegistryBundle string
}

// Export preserves every source index, manifest and compressed blob. Registry
// bootstrap is a separate small archive using the same OCI layout reader.
func Export(ctx context.Context, manifest *Manifest, opts ExportOptions) (ExportResult, error) {
	result, registryTarget, err := exportPaths(manifest, opts.Output)
	if err != nil {
		return result, err
	}
	workspace, err := os.MkdirTemp("", "kc-export-")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(workspace)
	full, err := layout.Write(filepath.Join(workspace, "release"), empty.Index)
	if err != nil {
		return result, err
	}
	small, err := layout.Write(filepath.Join(workspace, "registry"), empty.Index)
	if err != nil {
		return result, err
	}
	sourceOptions := append([]crane.Option{crane.WithContext(ctx), crane.WithAuth(authn.Anonymous)}, opts.SourceOptions...)
	if checkErr := exportContents(ctx, manifest, full, sourceOptions, opts.Out); checkErr != nil {
		return result, checkErr
	}
	fullBundle := &Bundle{path: full}
	registry, err := fullBundle.source(registryTarget)
	if err != nil {
		return result, err
	}
	if checkErr := appendBundleContent(small, registryTarget, registry.content()); checkErr != nil {
		return result, checkErr
	}
	data, err := yaml.Marshal(manifest)
	if err != nil {
		return result, err
	}
	if checkErr := full.WriteFile(bundleManifestFile, data, bundlePrivateMode); checkErr != nil {
		return result, checkErr
	}
	// Write the small bootstrap archive first so it is available independently.
	if checkErr := writeRegistryBundle(ctx, string(small), result.RegistryBundle, registryTarget, registry.Digest); checkErr != nil {
		return result, checkErr
	}
	if checkErr := writeBundleArchive(ctx, string(full), result.Bundle); checkErr != nil {
		return result, checkErr
	}
	return result, nil
}

func exportContents(ctx context.Context, manifest *Manifest, full layout.Path, sourceOptions []crane.Option, out io.Writer) error {
	for _, group := range groupArtifacts(manifest.Artifacts) {
		if checkErr := ctx.Err(); checkErr != nil {
			return checkErr
		}
		_, content, err := loadAndValidateSource(group.Source, group.Artifacts, ArchAll, sourceOptions)
		if err != nil {
			return fmt.Errorf("validate source %s: %w", group.Source, err)
		}
		if checkErr := appendBundleContent(full, group.Target, content); checkErr != nil {
			return fmt.Errorf("export %s: %w", group.Target, checkErr)
		}
		if out != nil {
			_, _ = fmt.Fprintf(out, "export: %s\n", group.Target)
		}
	}
	return nil
}

func exportPaths(manifest *Manifest, output string) (ExportResult, string, error) {
	var result ExportResult
	if manifest == nil {
		return result, "", fmt.Errorf("release manifest is required")
	}
	if checkErr := manifest.Validate(); checkErr != nil {
		return result, "", checkErr
	}
	registryTarget := ""
	for i := range manifest.Artifacts {
		artifact := &manifest.Artifacts[i]
		if artifact.Type == ArtifactTypePackage && artifact.Component.Kind == bootstrapKind && artifact.Component.Name == "registry" {
			if registryTarget != "" {
				return result, "", fmt.Errorf("export requires exactly one bootstrap/registry artifact")
			}
			registryTarget = artifact.Target
		}
	}
	if registryTarget == "" {
		return result, "", fmt.Errorf("bootstrap/registry artifact is required")
	}
	if output == "" {
		output = "./offline"
	}
	if info, statErr := os.Stat(output); statErr == nil && !info.IsDir() {
		return result, "", fmt.Errorf("output must be a directory: %s", output)
	} else if statErr != nil && !os.IsNotExist(statErr) {
		return result, "", statErr
	}
	registryTag, err := imagename.NewTag("bundle.invalid/" + registryTarget)
	if err != nil {
		return result, "", err
	}
	result.Bundle = filepath.Join(output, "kubeclipper-offline-"+manifest.Metadata.Version+".tar.gz")
	result.RegistryBundle = filepath.Join(output, "registry-"+registryTag.TagStr()+".tar.gz")
	if checkErr := checkExportPaths(result); checkErr != nil {
		return result, "", checkErr
	}
	return result, registryTarget, nil
}

func checkExportPaths(result ExportResult) error {
	if _, err := os.Lstat(result.Bundle); err == nil {
		return fmt.Errorf("output already exists: %s", result.Bundle)
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

func writeRegistryBundle(ctx context.Context, dir, output, ref string, expected v1.Hash) error {
	if _, err := os.Lstat(output); os.IsNotExist(err) {
		if writeErr := writeBundleArchive(ctx, dir, output); !errors.Is(writeErr, os.ErrExist) {
			return writeErr
		}
	} else if err != nil {
		return err
	}
	// A prior export may have completed the Registry archive before failing to
	// write the full bundle. Verify every blob and the exact root before reuse.
	bundle, err := OpenBundle(ctx, output)
	if err != nil {
		return fmt.Errorf("reuse Registry bundle %s: %w", output, err)
	}
	defer bundle.Close()
	index, err := bundle.path.ImageIndex()
	if err != nil {
		return err
	}
	roots, err := index.IndexManifest()
	if err != nil {
		return err
	}
	if len(roots.Manifests) != 1 {
		return fmt.Errorf("registry bundle %s must contain exactly one package", output)
	}
	source, err := bundle.source(ref)
	if err != nil {
		return fmt.Errorf("reuse Registry bundle %s: %w", output, err)
	}
	if source.Digest != expected {
		return fmt.Errorf("registry bundle digest mismatch: %s expected=%s actual=%s", output, expected, source.Digest)
	}
	return nil
}

func appendBundleContent(path layout.Path, ref string, content any) error {
	annotation := layout.WithAnnotations(map[string]string{bundleRefName: ref})
	switch content := content.(type) {
	case v1.ImageIndex:
		return path.AppendIndex(content, annotation)
	case v1.Image:
		return path.AppendImage(content, annotation)
	default:
		return fmt.Errorf("unsupported bundle content %T", content)
	}
}

// Bundle owns an extracted, verified archive. No operation on it contacts a
// source Registry; all referenced content must exist locally.
type Bundle struct {
	Manifest *Manifest
	path     layout.Path
}

func OpenBundle(ctx context.Context, archive string) (_ *Bundle, err error) {
	dir, err := os.MkdirTemp("", "kc-bundle-")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dir)
		}
	}()
	if checkErr := extractBundle(ctx, archive, dir); checkErr != nil {
		return nil, checkErr
	}
	if checkErr := verifyBundleMetadata(dir); checkErr != nil {
		return nil, checkErr
	}
	path, err := layout.FromPath(dir)
	if err != nil {
		return nil, err
	}
	bundle := &Bundle{path: path}
	if data, readErr := os.ReadFile(filepath.Join(dir, bundleManifestFile)); readErr == nil {
		bundle.Manifest, err = Parse(data)
		if err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(readErr) {
		return nil, readErr
	}
	if checkErr := verifyBundleRoots(path); checkErr != nil {
		return nil, checkErr
	}

	return bundle, nil
}

func verifyBundleRoots(path layout.Path) error {
	index, err := path.ImageIndex()
	if err != nil {
		return err
	}
	roots, err := index.IndexManifest()
	if err != nil {
		return err
	}
	seenRefs := make(map[string]bool)
	visited := make(map[v1.Hash]bool)
	for i := range roots.Manifests {
		root := &roots.Manifests[i]
		ref := root.Annotations[bundleRefName]
		if ref == "" || seenRefs[ref] {
			return fmt.Errorf("missing or duplicate bundle reference %q", ref)
		}
		seenRefs[ref] = true
		if checkErr := verifyBundleGraph(path, root, visited); checkErr != nil {
			return fmt.Errorf("incomplete bundle %s: %w", ref, checkErr)
		}
	}
	if len(roots.Manifests) == 0 {
		return fmt.Errorf("bundle contains no images")
	}
	return nil
}

func (b *Bundle) Close() error { return os.RemoveAll(string(b.path)) }

func (b *Bundle) source(ref string) (*artifactSource, error) {
	index, err := b.path.ImageIndex()
	if err != nil {
		return nil, err
	}
	roots, err := index.IndexManifest()
	if err != nil {
		return nil, err
	}
	for i := range roots.Manifests {
		descriptor := &roots.Manifests[i]
		if descriptor.Annotations[bundleRefName] != ref {
			continue
		}
		source := &artifactSource{Descriptor: *descriptor}
		if descriptor.MediaType.IsIndex() {
			source.index, err = index.ImageIndex(descriptor.Digest)
		} else {
			source.image, err = index.Image(descriptor.Digest)
		}
		return source, err
	}
	return nil, fmt.Errorf("bundle is missing %s", ref)
}

func (b *Bundle) RegistryImage(arch string) (v1.Image, error) {
	index, err := b.path.ImageIndex()
	if err != nil {
		return nil, err
	}
	roots, err := index.IndexManifest()
	if err != nil {
		return nil, err
	}
	ref := ""
	for i := range roots.Manifests {
		root := &roots.Manifests[i]
		if strings.HasPrefix(root.Annotations[bundleRefName], "package-bootstrap-registry:") {
			if ref != "" {
				return nil, fmt.Errorf("registry bundle contains multiple Registry packages")
			}
			ref = root.Annotations[bundleRefName]
		}
	}
	if ref == "" {
		return nil, fmt.Errorf("bundle is missing package-bootstrap-registry")
	}
	source, err := b.source(ref)
	if err != nil {
		return nil, err
	}
	if source.index == nil {
		return source.Image()
	}
	platform, err := platformDescriptor(source.index, "linux/"+arch)
	if err != nil {
		return nil, err
	}
	return source.index.Image(platform.Digest)
}

func verifyBundleGraph(path layout.Path, descriptor *v1.Descriptor, visited map[v1.Hash]bool) error {
	if checkErr := verifyBundleBlob(path, descriptor); checkErr != nil {
		return checkErr
	}
	if visited[descriptor.Digest] {
		return nil
	}
	visited[descriptor.Digest] = true
	data, err := path.Bytes(descriptor.Digest)
	if err != nil {
		return err
	}
	switch descriptor.MediaType {
	case types.OCIImageIndex, types.DockerManifestList:
		var index v1.IndexManifest
		if checkErr := json.Unmarshal(data, &index); checkErr != nil {
			return checkErr
		}
		for i := range index.Manifests {
			if checkErr := verifyBundleGraph(path, &index.Manifests[i], visited); checkErr != nil {
				return checkErr
			}
		}
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
		var manifest v1.Manifest
		if checkErr := json.Unmarshal(data, &manifest); checkErr != nil {
			return checkErr
		}
		for i := range manifest.Layers {
			if checkErr := verifyBundleBlob(path, &manifest.Layers[i]); checkErr != nil {
				return checkErr
			}
		}
		if checkErr := verifyBundleBlob(path, &manifest.Config); checkErr != nil {
			return checkErr
		}

	default:
		return fmt.Errorf("unsupported manifest media type %s", descriptor.MediaType)
	}
	return nil
}

func verifyBundleBlob(path layout.Path, descriptor *v1.Descriptor) error {
	if !bundleEntry("blobs/" + descriptor.Digest.Algorithm + "/" + descriptor.Digest.Hex) {
		return fmt.Errorf("unsupported blob digest %s", descriptor.Digest)
	}
	file := filepath.Join(string(path), "blobs", descriptor.Digest.Algorithm, descriptor.Digest.Hex)
	info, err := os.Stat(file)
	if err != nil {
		return err
	}
	if info.Size() != descriptor.Size {
		return fmt.Errorf("blob %s size mismatch", descriptor.Digest)
	}

	return nil
}

func bundleEntry(name string) bool {
	if name == bundleLayoutFile || name == bundleIndexFile || name == bundleManifestFile || name == bundleChecksumsFile {
		return true
	}
	parts := strings.Split(name, "/")
	if len(parts) != 3 || parts[0] != "blobs" || parts[1] != "sha256" || len(parts[2]) != 64 {
		return false
	}
	_, err := hex.DecodeString(parts[2])
	return err == nil
}

func extractBundle(ctx context.Context, archive, dir string) error {
	file, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gz.Close()
	reader := tar.NewReader(gz)
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			return verifyBundleTrailer(gz)
		}
		if err != nil {
			return err
		}
		if bundleDirectory(header) {
			continue
		}
		if !bundleEntry(header.Name) || header.Typeflag != tar.TypeReg {
			return fmt.Errorf("unsupported bundle entry %q", header.Name)
		}
		path := filepath.Join(dir, filepath.FromSlash(header.Name))
		if checkErr := os.MkdirAll(filepath.Dir(path), bundlePrivateDirMode); checkErr != nil {
			return checkErr
		}
		out, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, bundlePrivateMode)
		if err != nil {
			return err
		}
		hash := sha256.New()
		_, copyErr := io.Copy(io.MultiWriter(out, hash), contextReader{ctx, reader})
		closeErr := out.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		if strings.HasPrefix(header.Name, "blobs/") && hex.EncodeToString(hash.Sum(nil)) != filepath.Base(path) {
			return fmt.Errorf("blob digest mismatch: %s", header.Name)
		}
	}
}

func bundleDirectory(header *tar.Header) bool {
	if header.Typeflag != tar.TypeDir {
		return false
	}
	dir := strings.TrimSuffix(header.Name, "/")
	return dir == "." || dir == "blobs" || dir == "blobs/sha256"
}

func verifyBundleTrailer(gz *gzip.Reader) error {
	_, trailerErr := io.CopyN(io.Discard, gz, 1)
	if errors.Is(trailerErr, io.EOF) {
		return nil
	}
	if trailerErr != nil {
		return trailerErr
	}
	return fmt.Errorf("unexpected payload after bundle tar stream")
}

func verifyBundleMetadata(dir string) error {
	data, err := os.ReadFile(filepath.Join(dir, bundleChecksumsFile))
	if err != nil {
		return err
	}
	checksums := make(map[string]string)
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || !bundleEntry(fields[1]) || strings.Contains(fields[1], "/") || fields[1] == bundleChecksumsFile {
			return fmt.Errorf("invalid bundle checksum entry %q", line)
		}
		checksums[fields[1]] = fields[0]
	}
	for _, name := range []string{bundleLayoutFile, bundleIndexFile, bundleManifestFile} {
		data, err = os.ReadFile(filepath.Join(dir, name))
		if os.IsNotExist(err) && name == bundleManifestFile {
			continue
		}
		if err != nil {
			return err
		}
		if fmt.Sprintf("%x", sha256.Sum256(data)) != checksums[name] {
			return fmt.Errorf("bundle checksum mismatch: %s", name)
		}
	}
	return nil
}

func writeBundleChecksums(dir string) error {
	var sums strings.Builder
	for _, name := range []string{bundleLayoutFile, bundleIndexFile, bundleManifestFile} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if os.IsNotExist(err) && name == bundleManifestFile {
			continue
		}
		if err != nil {
			return err
		}
		_, _ = fmt.Fprintf(&sums, "%x  %s\n", sha256.Sum256(data), name)
	}
	if checkErr := os.WriteFile(filepath.Join(dir, bundleChecksumsFile), []byte(sums.String()), bundlePrivateMode); checkErr != nil {
		return checkErr
	}

	return nil
}

func writeBundleArchive(ctx context.Context, dir, output string) error {
	if checkErr := writeBundleChecksums(dir); checkErr != nil {
		return checkErr
	}
	if checkErr := os.MkdirAll(filepath.Dir(output), bundleOutputDirMode); checkErr != nil {
		return checkErr
	}
	file, err := os.CreateTemp(filepath.Dir(output), ".kc-bundle-*.tar.gz")
	if err != nil {
		return err
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	buffered := bufio.NewWriterSize(file, bundleBufferSize)
	gz, err := gzip.NewWriterLevel(buffered, gzip.BestSpeed)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(gz)
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	err = filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		relative, relErr := filepath.Rel(dir, path)
		if relErr != nil {
			return relErr
		}
		return writeBundleEntry(ctx, tw, root, relative)
	})
	if err != nil {
		return err
	}
	if checkErr := tw.Close(); checkErr != nil {
		return checkErr
	}
	if checkErr := gz.Close(); checkErr != nil {
		return checkErr
	}
	if checkErr := buffered.Flush(); checkErr != nil {
		return checkErr
	}
	if checkErr := file.Close(); checkErr != nil {
		return checkErr
	}
	// The temporary file shares the output filesystem. Linking publishes it
	// atomically and fails if another exporter has already created the output.
	return os.Link(file.Name(), output)
}

func writeBundleEntry(ctx context.Context, tw *tar.Writer, root *os.Root, relative string) error {
	input, err := root.Open(relative)
	if err != nil {
		return err
	}
	defer input.Close()
	info, err := input.Stat()
	if err != nil {
		return err
	}
	header := &tar.Header{Name: filepath.ToSlash(relative), Mode: 0600, Size: info.Size(), Typeflag: tar.TypeReg}
	if checkErr := tw.WriteHeader(header); checkErr != nil {
		return checkErr
	}
	hash := sha256.New()
	_, err = io.Copy(io.MultiWriter(tw, hash), contextReader{ctx, input})
	if err != nil {
		return err
	}
	if strings.HasPrefix(header.Name, "blobs/") && hex.EncodeToString(hash.Sum(nil)) != filepath.Base(relative) {
		return fmt.Errorf("blob digest mismatch during export: %s", header.Name)
	}
	return nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if checkErr := r.ctx.Err(); checkErr != nil {
		return 0, checkErr
	}
	return r.reader.Read(p)
}
