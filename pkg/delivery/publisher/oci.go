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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/match"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

type PublishRequest struct {
	PackagePath      string
	Kind             string
	Name             string
	Version          string
	Arch             string
	Registry         string
	ContentProfile   string
	ExternalContents []deliveryapis.ArtifactContent
}

type PublishResult struct {
	Transport deliveryapis.TransportRef
	Contents  []deliveryapis.ArtifactContent
}

type OCIArtifactPublisher struct{}

const (
	packageRootDir      = "package"
	packageManifestFile = "kc-package-manifest.json"
)

func NewOCIArtifactPublisher() *OCIArtifactPublisher {
	return &OCIArtifactPublisher{}
}

func (p *OCIArtifactPublisher) Publish(req PublishRequest) (*PublishResult, error) {
	if req.PackagePath == "" && len(req.ExternalContents) == 0 {
		return nil, fmt.Errorf("package path is required")
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
	img, err := buildArtifactImage(manifestPath, payloads)
	if err != nil {
		return nil, err
	}
	target := repositoryRef(req.Registry, req.Kind, req.Name, req.Version)
	digest, err := img.Digest()
	if err != nil {
		return nil, err
	}
	if err = pushPackageIndex(target, img, req.Arch); err != nil {
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

func buildArtifactImage(manifestPath string, payloads []payloadFile) (v1.Image, error) {
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
		if payload.name == deliveryapis.ContentBinary {
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

func pushPackageIndex(target string, img v1.Image, arch string) error {
	opts := crane.GetOptions(crane.Insecure)
	ref, err := name.ParseReference(target, opts.Name...)
	if err != nil {
		return err
	}
	index := existingPackageIndex(ref, opts.Remote...)
	index = buildPackageIndex(index, img, arch)
	return remote.WriteIndex(ref, index, opts.Remote...)
}

func buildPackageIndex(base v1.ImageIndex, img v1.Image, arch string) v1.ImageIndex {
	platform := v1.Platform{OS: "linux", Architecture: arch}
	index := mutate.RemoveManifests(base, match.Platforms(platform))
	index = mutate.AppendManifests(index, mutate.IndexAddendum{
		Add: img,
		Descriptor: v1.Descriptor{
			MediaType: types.OCIManifestSchema1,
			Platform:  &platform,
		},
	})
	return mutate.IndexMediaType(index, types.OCIImageIndex)
}

func existingPackageIndex(ref name.Reference, options ...remote.Option) v1.ImageIndex {
	desc, err := remote.Get(ref, options...)
	if err != nil {
		return empty.Index
	}
	switch desc.MediaType {
	case types.OCIImageIndex, types.DockerManifestList:
		index, err := desc.ImageIndex()
		if err == nil {
			return index
		}
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
		image, err := desc.Image()
		if err != nil {
			return empty.Index
		}
		manifest, err := deliveryManifestFromImage(image)
		if err != nil {
			return empty.Index
		}
		platform := v1.Platform{OS: manifest.Platform.OS, Architecture: manifest.Platform.Arch}
		if platform.OS == "" {
			platform.OS = "linux"
		}
		return mutate.AppendManifests(empty.Index, mutate.IndexAddendum{
			Add: image,
			Descriptor: v1.Descriptor{
				MediaType: types.OCIManifestSchema1,
				Platform:  &platform,
			},
		})
	}
	return empty.Index
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
			mediaType: deliveryapis.MediaTypeForContent(contentName),
			digest:    digest,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	var payloads []payloadFile
	for _, name := range expected {
		if payload, ok := found[name]; ok {
			payloads = append(payloads, payload)
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
			deliveryapis.ContentImages,
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
	if contentName == deliveryapis.ContentBinary {
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
		return deliveryapis.ContentBinary, true
	}
	switch file {
	case deliveryapis.ContentFile(deliveryapis.ContentConfigs):
		return deliveryapis.ContentConfigs, true
	case deliveryapis.ContentFile(deliveryapis.ContentImages):
		return deliveryapis.ContentImages, true
	case deliveryapis.ContentFile(deliveryapis.ContentCharts):
		return deliveryapis.ContentCharts, true
	default:
		return "", false
	}
}

func payloadDigest(path, contentName string) (string, error) {
	if contentName != deliveryapis.ContentBinary {
		return gzipUncompressedDigest(path)
	}
	return fileDigest(path)
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
