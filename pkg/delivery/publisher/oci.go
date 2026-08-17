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
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
)

type PublishRequest struct {
	PackagePath      string
	Kind             string
	Name             string
	Version          string
	Arch             string
	Registry         string
	ContentProfile   string
	SourceRevision   string
	ExternalContents []deliveryapis.ArtifactContent
	RegistryConfig   *deliveryregistry.Config
}

type PublishResult struct {
	Transport deliveryapis.TransportRef
	Contents  []deliveryapis.ArtifactContent
}

type OCIArtifactPublisher struct{}

const (
	packageRootDir       = "opt/kubeclipper/resource"
	packageManifestFile  = "kc-package-manifest.json"
	packageWriteAttempts = 3
)

var (
	stableArtifactVersionPattern = regexp.MustCompile(
		`^v?[0-9]+(?:\.[0-9]+){0,2}` +
			`(?:-[0-9A-Za-z][0-9A-Za-z.-]*)?(?:\+[0-9A-Za-z][0-9A-Za-z.-]*)?$`,
	)
)

func NewOCIArtifactPublisher() *OCIArtifactPublisher {
	return &OCIArtifactPublisher{}
}

func (p *OCIArtifactPublisher) Publish(req PublishRequest) (*PublishResult, error) {
	if req.PackagePath == "" && len(req.ExternalContents) == 0 {
		return nil, fmt.Errorf("package path or external content is required")
	}
	if req.Kind == "" || req.Name == "" || req.Version == "" || req.Arch == "" {
		return nil, fmt.Errorf("kind, name, version and arch are required")
	}
	if req.Registry == "" {
		return nil, fmt.Errorf("registry is required")
	}
	profile := req.ContentProfile
	if profile == "" {
		profile = deliveryapis.ContentProfileForKind(req.Kind)
	}

	workdir, err := os.MkdirTemp("", "kc-oci-publisher-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(workdir)

	var payloads []payloadFile
	if req.PackagePath != "" {
		extractedDir := filepath.Join(workdir, "pkg")
		if err = extractTarGz(req.PackagePath, extractedDir); err != nil {
			return nil, err
		}
		payloads, err = inspectPackageContents(extractedDir, req.Name, req.Version, req.Arch, profile)
		if err != nil {
			return nil, err
		}
	}
	contents := append(buildContents(payloads), req.ExternalContents...)
	if err = validatePublishContents(profile, contents); err != nil {
		return nil, err
	}

	manifestPath, err := writePackageManifest(workdir, req, profile, payloads, req.ExternalContents)
	if err != nil {
		return nil, err
	}
	img, err := buildArtifactImage(manifestPath, payloads, req)
	if err != nil {
		return nil, err
	}
	target := repositoryRef(req.Registry, req.Kind, req.Name, req.Version)
	digest, err := img.Digest()
	if err != nil {
		return nil, err
	}
	registryConfig := req.RegistryConfig
	if registryConfig == nil {
		registryConfig, err = deliveryregistry.Resolve(req.Registry)
		if err != nil {
			return nil, err
		}
	}
	registryConfigCopy := *registryConfig
	registryConfig = &registryConfigCopy
	if err := registryConfig.ValidateRegistry(req.Registry); err != nil {
		return nil, err
	}
	if err := pushPackageIndex(context.Background(), target, img, req.Arch, registryConfig); err != nil {
		return nil, err
	}

	return &PublishResult{
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    target,
			Digest: digest.String(),
		},
		Contents: contents,
	}, nil
}

type payloadFile struct {
	name      string
	path      string
	file      string
	mediaType string
	digest    string
}

func buildContents(payloads []payloadFile) []deliveryapis.ArtifactContent {
	contents := make([]deliveryapis.ArtifactContent, 0, len(payloads))
	for _, payload := range payloads {
		contents = append(contents, deliveryapis.ArtifactContent{
			Name:      payload.name,
			File:      payload.file,
			Digest:    payload.digest,
			MediaType: payload.mediaType,
		})
	}
	return contents
}

func validatePublishContents(profile string, contents []deliveryapis.ArtifactContent) error {
	inventory := deliveryapis.NewPackageInventory("validation")
	inventory.Spec.Packages = append(inventory.Spec.Packages, deliveryapis.PackageEntry{
		Kind:           "validation",
		Name:           "validation",
		Version:        "v0.0.0",
		OS:             deliveryapis.DefaultPackageOS,
		Arch:           "amd64",
		ContentProfile: profile,
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    "registry.local/kubeclipper/packages/validation/validation:v0.0.0",
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
		Contents: contents,
	})
	return inventory.Validate()
}

func writePackageManifest(workdir string, req PublishRequest, profile string, payloads []payloadFile, externalContents []deliveryapis.ArtifactContent) (string, error) {
	manifest := deliveryapis.PackageManifest{
		SchemaVersion:  1,
		Kind:           req.Kind,
		Name:           req.Name,
		Version:        req.Version,
		SourceRevision: req.SourceRevision,
		ContentProfile: profile,
		Platform: deliveryapis.PackageManifestPlatform{
			OS:   "linux",
			Arch: req.Arch,
		},
		Contents: make([]deliveryapis.PackageManifestFile, 0, len(payloads)),
	}
	for _, payload := range payloads {
		mediaType := payload.mediaType
		if mediaType == "" {
			mediaType = deliveryapis.MediaTypeForContent(payload.name)
		}
		manifest.Contents = append(manifest.Contents, deliveryapis.PackageManifestFile{
			Name:      payload.name,
			File:      payload.file,
			MediaType: mediaType,
			Digest:    payload.digest,
		})
	}
	for _, content := range externalContents {
		mediaType := content.MediaType
		if mediaType == "" {
			mediaType = deliveryapis.MediaTypeForContent(content.Name)
		}
		manifest.Contents = append(manifest.Contents, deliveryapis.PackageManifestFile{
			Name:      content.Name,
			File:      content.File,
			MediaType: mediaType,
			Digest:    content.Digest,
			Transport: content.Transport,
		})
	}
	data, err := json.Marshal(manifest)
	if err != nil {
		return "", err
	}
	path := filepath.Join(workdir, packageManifestFile)
	return path, os.WriteFile(path, data, 0644)
}

func buildArtifactImage(manifestPath string, payloads []payloadFile, req PublishRequest) (v1.Image, error) {
	rootfsPath := filepath.Join(filepath.Dir(manifestPath), "kc-package-rootfs.tar")
	if err := writePackageRootFS(rootfsPath, manifestPath, payloads); err != nil {
		return nil, err
	}
	layer, err := tarball.LayerFromFile(rootfsPath, tarball.WithMediaType(types.OCILayer))
	if err != nil {
		return nil, err
	}
	img := mutate.MediaType(empty.Image, types.OCIManifestSchema1)
	img = mutate.ConfigMediaType(img, types.OCIConfigJSON)
	img, err = mutate.Append(img, mutate.Addendum{
		Layer:     layer,
		MediaType: types.OCILayer,
	})
	if err != nil {
		return nil, err
	}
	config, err := img.ConfigFile()
	if err != nil {
		return nil, err
	}
	config.Config.Labels = map[string]string{
		"org.opencontainers.image.source":  "https://github.com/kubeclipper/kubeclipper",
		"org.opencontainers.image.title":   req.Kind + "/" + req.Name,
		"org.opencontainers.image.version": req.Version,
	}
	if req.SourceRevision != "" {
		config.Config.Labels["org.opencontainers.image.revision"] = req.SourceRevision
	}
	img, err = mutate.ConfigFile(img, config)
	if err != nil {
		return nil, err
	}
	return img, nil
}

func writePackageRootFS(target, manifestPath string, payloads []payloadFile) error {
	file, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	tw := tar.NewWriter(file)
	defer tw.Close()
	if err = tw.WriteHeader(&tar.Header{
		Name:     packageRootDir + "/",
		Typeflag: tar.TypeDir,
		Mode:     0755,
	}); err != nil {
		return err
	}
	if err = addFileToRootFSTar(tw, manifestPath, path.Join(packageRootDir, packageManifestFile), 0644); err != nil {
		return err
	}
	for _, payload := range payloads {
		if payload.file == "" {
			return fmt.Errorf("payload %q file is required", payload.name)
		}
		if strings.Contains(payload.file, "/") || strings.Contains(payload.file, "\\") || payload.file == "." || payload.file == ".." {
			return fmt.Errorf("payload %q file %q must be a base name", payload.name, payload.file)
		}
		mode := int64(0644)
		if isBinaryPayloadContent(payload.name) {
			mode = 0755
		}
		if err = addFileToRootFSTar(tw, payload.path, path.Join(packageRootDir, payload.file), mode); err != nil {
			return err
		}
	}
	return nil
}

func addFileToRootFSTar(tw *tar.Writer, src, name string, mode int64) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", src)
	}
	file, err := os.Open(src)
	if err != nil {
		return err
	}
	defer file.Close()
	if err = tw.WriteHeader(&tar.Header{
		Name: name,
		Mode: mode,
		Size: info.Size(),
	}); err != nil {
		return err
	}
	_, err = io.Copy(tw, file)
	return err
}

func pushPackageIndex(ctx context.Context, target string, img v1.Image, arch string, config *deliveryregistry.Config) error {
	configCopy := *config
	craneOpts, err := configCopy.CraneOptions(ctx)
	if err != nil {
		return err
	}
	opts := crane.GetOptions(craneOpts...)
	ref, err := name.ParseReference(target, opts.Name...)
	if err != nil {
		return err
	}
	digest, err := img.Digest()
	if err != nil {
		return fmt.Errorf("calculate package image digest: %w", err)
	}

	// The Distribution API has no portable compare-and-swap operation for tags.
	// Serialize writers in this process, verify every write, and let the release
	// workflow's component concurrency group serialize separate publisher processes.
	unlock, err := lockPublishReference(ctx, ref.Name())
	if err != nil {
		return fmt.Errorf("lock package reference %s: %w", ref.Name(), err)
	}
	defer unlock()

	platform := v1.Platform{OS: "linux", Architecture: arch}
	immutable := isStableArtifactVersion(ref.Identifier())
	var verificationErr error
	for attempt := range packageWriteAttempts {
		index, readErr := existingPackageIndex(ref, opts.Remote...)
		if readErr != nil {
			return fmt.Errorf("inspect existing package index %s: %w", ref.Name(), readErr)
		}
		existingDigest, found, inspectErr := packagePlatformDigest(index, platform.OS, platform.Architecture)
		if inspectErr != nil {
			return fmt.Errorf("inspect package platform %s in %s: %w", platform, ref.Name(), inspectErr)
		}
		if found && existingDigest == digest {
			return nil
		}
		if found && immutable {
			return fmt.Errorf("package tag conflict: %s platform %s already points to %s, refusing %s", ref.Name(), platform, existingDigest, digest)
		}

		next := buildPackageIndex(index, img, arch)
		if writeErr := remote.WriteIndex(ref, next, opts.Remote...); writeErr != nil {
			return fmt.Errorf("write package index %s: %w", ref.Name(), writeErr)
		}
		written, readErr := existingPackageIndex(ref, opts.Remote...)
		if readErr != nil {
			return fmt.Errorf("verify package index %s after write: %w", ref.Name(), readErr)
		}
		verificationErr = verifyPackageIndex(written, next)
		if verificationErr == nil {
			writtenDigest, writtenFound, inspectErr := packagePlatformDigest(written, platform.OS, platform.Architecture)
			switch {
			case inspectErr != nil:
				verificationErr = inspectErr
			case !writtenFound:
				verificationErr = fmt.Errorf("written index has no %s descriptor", platform)
			case writtenDigest != digest:
				verificationErr = fmt.Errorf("written index %s digest is %s, want %s", platform, writtenDigest, digest)
			}
		}
		if verificationErr == nil {
			return nil
		}
		if attempt+1 < packageWriteAttempts {
			if waitErr := waitPackageWriteRetry(ctx, attempt); waitErr != nil {
				return waitErr
			}
		}
	}
	return fmt.Errorf("verify package index %s after %d attempts: %w", ref.Name(), packageWriteAttempts, verificationErr)
}

func buildPackageIndex(base v1.ImageIndex, img v1.Image, arch string) v1.ImageIndex {
	platform := v1.Platform{OS: "linux", Architecture: arch}
	index := mutate.RemoveManifests(base, func(desc v1.Descriptor) bool {
		return desc.Platform != nil && desc.Platform.OS == platform.OS && desc.Platform.Architecture == platform.Architecture
	})
	index = mutate.AppendManifests(index, mutate.IndexAddendum{
		Add: img,
		Descriptor: v1.Descriptor{
			MediaType: types.OCIManifestSchema1,
			Platform:  &platform,
		},
	})
	return mutate.IndexMediaType(index, types.OCIImageIndex)
}

func existingPackageIndex(ref name.Reference, options ...remote.Option) (v1.ImageIndex, error) {
	desc, exists, err := remoteDescriptor(ref, options...)
	if err != nil {
		return nil, err
	}
	if !exists {
		return empty.Index, nil
	}
	switch desc.MediaType {
	case types.OCIImageIndex, types.DockerManifestList:
		index, err := desc.ImageIndex()
		if err != nil {
			return nil, fmt.Errorf("decode existing image index: %w", err)
		}
		if _, err = index.IndexManifest(); err != nil {
			return nil, fmt.Errorf("parse existing image index: %w", err)
		}
		return index, nil
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
		image, err := desc.Image()
		if err != nil {
			return nil, fmt.Errorf("decode existing package image: %w", err)
		}
		manifest, err := deliveryManifestFromImage(image)
		if err != nil {
			return nil, fmt.Errorf("read existing package manifest: %w", err)
		}
		platform := v1.Platform{OS: manifest.Platform.OS, Architecture: manifest.Platform.Arch}
		if platform.OS == "" {
			platform.OS = "linux"
		}
		if platform.Architecture == "" {
			return nil, fmt.Errorf("existing package image has no architecture")
		}
		return mutate.AppendManifests(empty.Index, mutate.IndexAddendum{
			Add: image,
			Descriptor: v1.Descriptor{
				MediaType: desc.MediaType,
				Platform:  &platform,
			},
		}), nil
	default:
		return nil, fmt.Errorf("existing reference has unsupported media type %q", desc.MediaType)
	}
}

func remoteDescriptor(ref name.Reference, options ...remote.Option) (*remote.Descriptor, bool, error) {
	desc, err := remote.Get(ref, options...)
	if err == nil {
		return desc, true, nil
	}
	var transportErr *transport.Error
	if errors.As(err, &transportErr) && transportErr.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	return nil, false, err
}

func packagePlatformDigest(index v1.ImageIndex, osName, architecture string) (v1.Hash, bool, error) {
	manifest, err := index.IndexManifest()
	if err != nil {
		return v1.Hash{}, false, err
	}
	var digest v1.Hash
	found := false
	for i := range manifest.Manifests {
		descriptor := &manifest.Manifests[i]
		if descriptor.Platform == nil || descriptor.Platform.OS != osName || descriptor.Platform.Architecture != architecture {
			continue
		}
		if found {
			return v1.Hash{}, false, fmt.Errorf("multiple descriptors found for %s/%s", osName, architecture)
		}
		digest = descriptor.Digest
		found = true
	}
	return digest, found, nil
}

func verifyPackageIndex(actual, expected v1.ImageIndex) error {
	expectedManifest, err := expected.IndexManifest()
	if err != nil {
		return fmt.Errorf("read expected index: %w", err)
	}
	actualManifest, err := actual.IndexManifest()
	if err != nil {
		return fmt.Errorf("read written index: %w", err)
	}
	actualDescriptors := make(map[string]int, len(actualManifest.Manifests))
	for i := range actualManifest.Manifests {
		actualDescriptors[packageDescriptorKey(&actualManifest.Manifests[i])]++
	}
	for i := range expectedManifest.Manifests {
		key := packageDescriptorKey(&expectedManifest.Manifests[i])
		if actualDescriptors[key] == 0 {
			return fmt.Errorf("written index is missing descriptor %s", key)
		}
		actualDescriptors[key]--
	}
	return nil
}

func packageDescriptorKey(descriptor *v1.Descriptor) string {
	platform := "<none>"
	if descriptor.Platform != nil {
		platform = strings.Join([]string{
			descriptor.Platform.OS,
			descriptor.Platform.Architecture,
			descriptor.Platform.Variant,
		}, "/")
	}
	return fmt.Sprintf("%s|%s|%s", platform, descriptor.MediaType, descriptor.Digest)
}

func isStableArtifactVersion(version string) bool {
	return stableArtifactVersionPattern.MatchString(strings.TrimSpace(version))
}

func waitPackageWriteRetry(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt+1) * 100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func deliveryManifestFromImage(img v1.Image) (deliveryapis.PackageManifest, error) {
	layers, err := img.Layers()
	if err != nil {
		return deliveryapis.PackageManifest{}, err
	}
	for _, layer := range layers {
		reader, err := layer.Uncompressed()
		if err != nil {
			return deliveryapis.PackageManifest{}, err
		}
		manifest, err := readPackageManifestFromRootFS(reader)
		reader.Close()
		if err == nil {
			return manifest, nil
		}
		if !os.IsNotExist(err) {
			return deliveryapis.PackageManifest{}, err
		}
	}
	return deliveryapis.PackageManifest{}, fmt.Errorf("%s not found in package image", path.Join(packageRootDir, packageManifestFile))
}

func readPackageManifestFromRootFS(reader io.Reader) (deliveryapis.PackageManifest, error) {
	tr := tar.NewReader(reader)
	manifestPath := path.Join(packageRootDir, packageManifestFile)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return deliveryapis.PackageManifest{}, os.ErrNotExist
		}
		if err != nil {
			return deliveryapis.PackageManifest{}, err
		}
		if header.Typeflag != tar.TypeReg || path.Clean(header.Name) != manifestPath {
			continue
		}
		var manifest deliveryapis.PackageManifest
		if err = json.NewDecoder(tr).Decode(&manifest); err != nil {
			return deliveryapis.PackageManifest{}, err
		}
		return manifest, nil
	}
}

func inspectPackageContents(root, name, version, arch, profile string) ([]payloadFile, error) {
	expected := expectedPayloadContents(profile)
	found := make(map[string]payloadFile, len(expected))
	err := filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		if isMetadataFile(info.Name()) {
			return nil
		}
		if info.Name() == "images.tar.gz" {
			return fmt.Errorf("embedded runtime image archive %q is not supported; publish runtime images as standard images", path)
		}
		contentName, ok := contentNameFromFile(info.Name(), profile)
		if !ok {
			return nil
		}
		if !pathMatchesPackage(path, name, version, arch) {
			return nil
		}
		if _, exists := found[contentName]; exists {
			return fmt.Errorf("duplicate payload %q in package", contentName)
		}
		if err := validatePayloadFile(path, contentName); err != nil {
			return err
		}
		digest, err := payloadDigest(path, contentName)
		if err != nil {
			return err
		}
		found[contentName] = payloadFile{
			name:      contentName,
			path:      path,
			file:      info.Name(),
			mediaType: mediaTypeForPayloadContent(contentName, profile),
			digest:    digest,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var payloads []payloadFile
	if profile == deliveryapis.ContentProfileBinary {
		for _, payload := range found {
			payloads = append(payloads, payload)
		}
		sort.Slice(payloads, func(i, j int) bool {
			return payloads[i].name < payloads[j].name
		})
	} else {
		for _, name := range expected {
			if payload, ok := found[name]; ok {
				payloads = append(payloads, payload)
			}
		}
	}
	if len(payloads) == 0 {
		return nil, fmt.Errorf("no supported payloads found in package")
	}
	return payloads, nil
}

func repositoryRef(registry, kind, name, version string) string {
	return fmt.Sprintf("%s/%s/%s/%s:%s", strings.TrimRight(registry, "/"), deliveryapis.PackageRepositoryPrefix, kind, name, version)
}

func expectedPayloadContents(profile string) []string {
	switch profile {
	case deliveryapis.ContentProfileK8s:
		return []string{
			deliveryapis.ContentConfigs,
		}
	case deliveryapis.ContentProfileRuntime:
		return []string{
			deliveryapis.ContentConfigs,
		}
	case deliveryapis.ContentProfileAddon:
		return []string{
			deliveryapis.ContentConfigs,
			deliveryapis.ContentCharts,
		}
	case deliveryapis.ContentProfileExtension:
		return []string{deliveryapis.ContentConfigs}
	case deliveryapis.ContentProfileBinary:
		return []string{deliveryapis.ContentBinary}
	default:
		return []string{
			deliveryapis.ContentConfigs,
			deliveryapis.ContentCharts,
		}
	}
}

func pathMatchesPackage(path, name, version, arch string) bool {
	slashed := filepath.ToSlash(path)
	versionArch := "/" + version + "/" + arch + "/"
	nameVersionArch := "/" + name + "/" + version + "/" + arch + "/"
	return strings.Contains(slashed, versionArch) || strings.Contains(slashed, nameVersionArch)
}

func validatePayloadFile(path, contentName string) error {
	if isBinaryPayloadContent(contentName) {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.Size() == 0 {
			return fmt.Errorf("payload %q is empty", filepath.Base(path))
		}
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	gzr, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("payload %q is not gzip/tgz: %w", filepath.Base(path), err)
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return fmt.Errorf("payload %q archive is empty", filepath.Base(path))
		}
		if err != nil {
			return fmt.Errorf("payload %q is not a readable tar archive: %w", filepath.Base(path), err)
		}
		if header.Typeflag == tar.TypeReg {
			return nil
		}
	}
}

func contentNameFromFile(file, profile string) (string, bool) {
	if profile == deliveryapis.ContentProfileBinary {
		return file, true
	}
	switch file {
	case deliveryapis.ContentFile(deliveryapis.ContentConfigs):
		return deliveryapis.ContentConfigs, true
	case deliveryapis.ContentFile(deliveryapis.ContentCharts):
		return deliveryapis.ContentCharts, true
	default:
		return "", false
	}
}

func isMetadataFile(file string) bool {
	return file == ".DS_Store" || strings.HasPrefix(file, "._")
}

func payloadDigest(path, contentName string) (string, error) {
	if isBinaryPayloadContent(contentName) {
		return fileDigest(path)
	}
	return gzipUncompressedDigest(path)
}

func mediaTypeForPayloadContent(contentName, profile string) string {
	if profile == deliveryapis.ContentProfileBinary || isBinaryPayloadContent(contentName) {
		return deliveryapis.MediaTypeBinaryLayer
	}
	return deliveryapis.MediaTypeForContent(contentName)
}

func isBinaryPayloadContent(contentName string) bool {
	switch contentName {
	case deliveryapis.ContentConfigs, deliveryapis.ContentCharts:
		return false
	default:
		return true
	}
}

func gzipUncompressedDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	gzr, err := gzip.NewReader(f)
	if err != nil {
		return "", err
	}
	defer gzr.Close()
	h := sha256.New()
	if _, err = io.Copy(h, gzr); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func fileDigest(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func extractTarGz(src, dst string) error {
	file, err := os.Open(src)
	if err != nil {
		return err
	}
	defer file.Close()
	gzr, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		target := filepath.Join(dst, filepath.Clean(hdr.Name))
		if !strings.HasPrefix(target, filepath.Clean(dst)+string(filepath.Separator)) && filepath.Clean(target) != filepath.Clean(dst) {
			return fmt.Errorf("tar entry %q escapes destination", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err = os.MkdirAll(target, 0755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err = os.MkdirAll(filepath.Dir(target), 0755); err != nil {
				return err
			}
			w, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			if _, err = io.Copy(w, tr); err != nil {
				w.Close()
				return err
			}
			if err = w.Close(); err != nil {
				return err
			}
		}
	}
}
