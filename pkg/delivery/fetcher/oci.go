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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/google/go-containerregistry/pkg/crane"
	containerv1 "github.com/google/go-containerregistry/pkg/v1"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
)

const fetchedFileMode = 0o644

type OCIArtifactFetcher struct {
	DryRun         bool
	PullImage      func(ref string) (containerv1.Image, error)
	RegistryConfig *deliveryregistry.Config
}

func NewOCIArtifactFetcher(dryRun bool) *OCIArtifactFetcher {
	return &OCIArtifactFetcher{DryRun: dryRun}
}

func NewOCIArtifactFetcherWithConfig(dryRun bool, config *deliveryregistry.Config) *OCIArtifactFetcher {
	return &OCIArtifactFetcher{DryRun: dryRun, RegistryConfig: config}
}

func (f *OCIArtifactFetcher) Fetch(ctx context.Context, plan *deliveryapis.ResolvedArtifactPlan) (*FetchResult, error) {
	if err := validatePlan(plan); err != nil {
		return nil, err
	}
	result := &FetchResult{}
	for _, component := range plan.Components {
		componentResult, err := f.fetchComponent(ctx, plan.OS, plan.Arch, component)
		if err != nil {
			return nil, err
		}
		result.Components = append(result.Components, componentResult)
	}
	return result, nil
}

func (f *OCIArtifactFetcher) fetchComponent(ctx context.Context, osName, arch string, component deliveryapis.ResolvedComponent) (result ComponentFetchResult, err error) {
	if component.Transport.Type != deliveryapis.TransportOCI {
		return ComponentFetchResult{}, fmt.Errorf("unsupported transport %q for oci fetcher", component.Transport.Type)
	}
	ref, err := ociReference(component.Transport)
	if err != nil {
		return ComponentFetchResult{}, fmt.Errorf("invalid oci transport for %s/%s:%s: %w", component.Kind, component.Name, component.Version, err)
	}
	fetchContents := packageLayerContents(component.Contents)
	componentResult := ComponentFetchResult{
		Slot:      component.Slot,
		Kind:      component.Kind,
		Name:      component.Name,
		Version:   component.Version,
		OS:        osName,
		Arch:      arch,
		BaseDir:   downloader.PackageDir(component.Kind, component.Name, component.Version, platformDir(osName, arch)),
		Transport: component.Transport,
		Files:     make(map[string]string, len(fetchContents)),
	}
	componentResult.ManifestPath = downloader.PackageManifestPath(component.Kind, component.Name, component.Version, platformDir(osName, arch))
	if f.DryRun {
		for _, content := range fetchContents {
			if err := ctx.Err(); err != nil {
				return ComponentFetchResult{}, err
			}
			componentResult.Files[content.Name] = filepath.Join(downloader.PackageContentsDir(component.Kind, component.Name, component.Version, platformDir(osName, arch)), contentFile(content))
		}
		if err := writeFetchedManifest(componentResult, fetchContents, true); err != nil {
			return ComponentFetchResult{}, err
		}
		return componentResult, nil
	}
	lock, err := downloader.AcquirePackageLock(component.Kind, component.Name, component.Version, platformDir(osName, arch))
	if err != nil {
		return ComponentFetchResult{}, err
	}
	defer func() {
		err = errors.Join(err, lock.Unlock())
	}()
	if cached, ok := loadCachedComponent(componentResult, fetchContents); ok {
		return cached, nil
	}
	image, err := f.pullImage(ctx, ref)
	if err != nil {
		return ComponentFetchResult{}, err
	}
	if err = validatePulledImageDigest(image, component.Transport.Digest); err != nil {
		return ComponentFetchResult{}, fmt.Errorf("validate pulled artifact %s: %w", ref, err)
	}
	manifest, err := readPackageManifest(image)
	if err != nil {
		return ComponentFetchResult{}, fmt.Errorf("read package manifest from %s: %w", ref, err)
	}
	layerComponent := component
	layerComponent.Contents = fetchContents
	if err = validateFetchedManifest(manifest, layerComponent, osName, arch); err != nil {
		return ComponentFetchResult{}, fmt.Errorf("validate package manifest from %s: %w", ref, err)
	}
	layers, err := image.Layers()
	if err != nil {
		return ComponentFetchResult{}, err
	}
	for _, content := range fetchContents {
		if err = ctx.Err(); err != nil {
			return ComponentFetchResult{}, err
		}
		path := filepath.Join(downloader.PackageContentsDir(component.Kind, component.Name, component.Version, platformDir(osName, arch)), contentFile(content))
		if err = writePackageFile(layers, path, content); err != nil {
			return ComponentFetchResult{}, err
		}
		componentResult.Files[content.Name] = path
	}
	if err = writeFetchedManifest(componentResult, fetchContents, false); err != nil {
		return ComponentFetchResult{}, err
	}
	return componentResult, nil
}

func loadCachedComponent(result ComponentFetchResult, contents []deliveryapis.ArtifactContent) (ComponentFetchResult, bool) {
	data, err := os.ReadFile(result.ManifestPath)
	if err != nil {
		return ComponentFetchResult{}, false
	}
	var cached fetchedComponentManifest
	if err = json.Unmarshal(data, &cached); err != nil {
		return ComponentFetchResult{}, false
	}
	// Slot identifies the resolver choice, not the artifact. Runtime component
	// commands intentionally reconstruct the resolved component without it, so
	// a slot difference must not invalidate otherwise identical digest-pinned
	// content.
	if cached.Kind != result.Kind || cached.Name != result.Name ||
		cached.Version != result.Version || cached.OS != result.OS || cached.Arch != result.Arch ||
		!reflect.DeepEqual(cached.Transport, result.Transport) || !reflect.DeepEqual(cached.Contents, contents) {
		return ComponentFetchResult{}, false
	}
	expectedDir := downloader.PackageContentsDir(result.Kind, result.Name, result.Version, platformDir(result.OS, result.Arch))
	files := make(map[string]string, len(contents))
	for _, content := range contents {
		expectedPath := filepath.Join(expectedDir, contentFile(content))
		if filepath.Clean(cached.Files[content.Name]) != filepath.Clean(expectedPath) {
			return ComponentFetchResult{}, false
		}
		payload, readErr := os.ReadFile(expectedPath)
		if readErr != nil {
			return ComponentFetchResult{}, false
		}
		digest, digestErr := packageFilePayloadDigest(payload, content.Name)
		if digestErr != nil || digest != content.Digest {
			return ComponentFetchResult{}, false
		}
		files[content.Name] = expectedPath
	}
	result.Files = files
	return result, true
}

func packageLayerContents(contents []deliveryapis.ArtifactContent) []deliveryapis.ArtifactContent {
	filtered := make([]deliveryapis.ArtifactContent, 0, len(contents))
	for _, content := range contents {
		if content.Transport.Type != "" {
			continue
		}
		filtered = append(filtered, content)
	}
	return filtered
}

func (f *OCIArtifactFetcher) pullImage(ctx context.Context, ref string) (containerv1.Image, error) {
	if f.PullImage != nil {
		return f.PullImage(ref)
	}
	config := f.RegistryConfig
	if config == nil {
		var err error
		config, err = deliveryregistry.ResolveReference(ref)
		if err != nil {
			return nil, err
		}
	}
	if err := config.ValidateReference(ref); err != nil {
		return nil, err
	}
	opts, err := config.CraneOptions(ctx)
	if err != nil {
		return nil, err
	}
	return crane.Pull(ref, opts...)
}

func validatePulledImageDigest(image containerv1.Image, expectedDigest string) error {
	digest, err := image.Digest()
	if err != nil {
		return err
	}
	if digest.String() != expectedDigest {
		return fmt.Errorf("platform manifest digest mismatch: expected %s, got %s", expectedDigest, digest.String())
	}
	return nil
}

func ociReference(transport deliveryapis.TransportRef) (string, error) {
	if transport.Ref == "" {
		return "", fmt.Errorf("reference is required")
	}
	if transport.Digest == "" {
		return "", fmt.Errorf("digest is required")
	}
	if strings.Contains(transport.Ref, "@") {
		_, digest, _ := strings.Cut(transport.Ref, "@")
		if digest != transport.Digest {
			return "", fmt.Errorf("reference digest %q does not match transport digest %q", digest, transport.Digest)
		}
		return transport.Ref, nil
	}
	return transport.Ref + "@" + transport.Digest, nil
}

func platformDir(osName, arch string) string {
	if osName == "" {
		osName = deliveryapis.DefaultPackageOS
	}
	if arch == "" {
		return osName
	}
	return osName + "-" + arch
}

func readPackageManifest(img containerv1.Image) (deliveryapis.PackageManifest, error) {
	layers, err := img.Layers()
	if err != nil {
		return deliveryapis.PackageManifest{}, err
	}
	for _, layer := range layers {
		manifest, err := readPackageManifestLayer(layer)
		if err == nil {
			return manifest, nil
		}
		if !os.IsNotExist(err) {
			return deliveryapis.PackageManifest{}, err
		}
	}
	return deliveryapis.PackageManifest{}, fmt.Errorf("%s not found in package image", packageManifestPath())
}

func readPackageManifestLayer(layer containerv1.Layer) (deliveryapis.PackageManifest, error) {
	reader, err := layer.Uncompressed()
	if err != nil {
		return deliveryapis.PackageManifest{}, err
	}
	defer reader.Close()
	tr := tar.NewReader(reader)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return deliveryapis.PackageManifest{}, os.ErrNotExist
		}
		if err != nil {
			return deliveryapis.PackageManifest{}, err
		}
		if header.Typeflag != tar.TypeReg || path.Clean(header.Name) != packageManifestPath() {
			continue
		}
		var manifest deliveryapis.PackageManifest
		if err = json.NewDecoder(tr).Decode(&manifest); err != nil {
			return deliveryapis.PackageManifest{}, err
		}
		return manifest, nil
	}
}

func packageManifestPath() string {
	return path.Join("opt/kubeclipper/resource", "kc-package-manifest.json")
}

func validateFetchedManifest(manifest deliveryapis.PackageManifest, component deliveryapis.ResolvedComponent, osName, arch string) error {
	if manifest.SchemaVersion != 1 {
		return fmt.Errorf("unsupported package manifest schemaVersion %d", manifest.SchemaVersion)
	}
	if manifest.Kind != component.Kind || manifest.Name != component.Name || manifest.Version != component.Version {
		return fmt.Errorf("manifest identity %s/%s:%s does not match resolved component %s/%s:%s", manifest.Kind, manifest.Name, manifest.Version, component.Kind, component.Name, component.Version)
	}
	if manifest.Platform.OS != "" && manifest.Platform.OS != osName {
		return fmt.Errorf("manifest os %q does not match resolved os %q", manifest.Platform.OS, osName)
	}
	if manifest.Platform.Arch != arch {
		return fmt.Errorf("manifest arch %q does not match resolved arch %q", manifest.Platform.Arch, arch)
	}
	manifestContents := make(map[string]deliveryapis.PackageManifestFile, len(manifest.Contents))
	for _, content := range manifest.Contents {
		if content.Name == "" {
			continue
		}
		manifestContents[content.Name] = content
	}
	for _, content := range component.Contents {
		manifestContent, ok := manifestContents[content.Name]
		if !ok {
			return fmt.Errorf("manifest content %q is missing", content.Name)
		}
		if content.File != "" && manifestContent.File != content.File {
			return fmt.Errorf("manifest content %q file %q does not match resolved file %q", content.Name, manifestContent.File, content.File)
		}
		if content.MediaType != "" && manifestContent.MediaType != "" && manifestContent.MediaType != content.MediaType {
			return fmt.Errorf("manifest content %q mediaType %q does not match resolved mediaType %q", content.Name, manifestContent.MediaType, content.MediaType)
		}
		if content.Digest != "" && manifestContent.Digest != content.Digest {
			return fmt.Errorf("manifest content %q digest %q does not match resolved digest %q", content.Name, manifestContent.Digest, content.Digest)
		}
	}
	return nil
}

func contentFile(content deliveryapis.ArtifactContent) string {
	if content.File != "" {
		return content.File
	}
	switch content.Name {
	case deliveryapis.ContentConfigs:
		return downloader.ConfigFilename
	case deliveryapis.ContentCharts:
		return downloader.ChartFilename
	default:
		return content.Name
	}
}

func writePackageFile(layers []containerv1.Layer, target string, content deliveryapis.ArtifactContent) error {
	data, err := readPackageFile(layers, contentFile(content))
	if err != nil {
		return fmt.Errorf("read package content %q: %w", content.Name, err)
	}
	if content.Digest != "" {
		actual, err := packageFilePayloadDigest(data, content.Name)
		if err != nil {
			return err
		}
		if actual != content.Digest {
			return fmt.Errorf("payload digest mismatch: expected %s, got %s", content.Digest, actual)
		}
	}
	return downloader.AtomicWriteFile(target, data, fetchedFileMode)
}

func readPackageFile(layers []containerv1.Layer, file string) ([]byte, error) {
	if file == "" {
		return nil, fmt.Errorf("package content file is required")
	}
	target := path.Join("opt/kubeclipper/resource", file)
	for _, layer := range layers {
		reader, err := layer.Uncompressed()
		if err != nil {
			return nil, err
		}
		data, err := readFileFromRootFS(reader, target)
		reader.Close()
		if err == nil {
			return data, nil
		}
		if !os.IsNotExist(err) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("%s not found in package image", target)
}

func readFileFromRootFS(reader io.Reader, target string) ([]byte, error) {
	tr := tar.NewReader(reader)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return nil, os.ErrNotExist
		}
		if err != nil {
			return nil, err
		}
		if header.Typeflag != tar.TypeReg || path.Clean(header.Name) != target {
			continue
		}
		return io.ReadAll(tr)
	}
}

func packageFilePayloadDigest(data []byte, contentName string) (string, error) {
	if isPlainFileContent(contentName) {
		sum := sha256.Sum256(data)
		return "sha256:" + hex.EncodeToString(sum[:]), nil
	}
	gzr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return "", err
	}
	defer gzr.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, gzr); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func isPlainFileContent(contentName string) bool {
	switch contentName {
	case deliveryapis.ContentConfigs, deliveryapis.ContentCharts:
		return false
	default:
		return true
	}
}

type fetchedComponentManifest struct {
	Slot      string                         `json:"slot,omitempty"`
	Kind      string                         `json:"kind"`
	Name      string                         `json:"name"`
	Version   string                         `json:"version"`
	OS        string                         `json:"os,omitempty"`
	Arch      string                         `json:"arch,omitempty"`
	Transport deliveryapis.TransportRef      `json:"transport"`
	Files     map[string]string              `json:"files"`
	Contents  []deliveryapis.ArtifactContent `json:"contents,omitempty"`
}

func writeFetchedManifest(result ComponentFetchResult, contents []deliveryapis.ArtifactContent, dryRun bool) error {
	if result.ManifestPath == "" {
		return nil
	}
	if dryRun {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(result.ManifestPath), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(fetchedComponentManifest{
		Slot:      result.Slot,
		Kind:      result.Kind,
		Name:      result.Name,
		Version:   result.Version,
		OS:        result.OS,
		Arch:      result.Arch,
		Transport: result.Transport,
		Files:     result.Files,
		Contents:  contents,
	}, "", "  ")
	if err != nil {
		return err
	}
	return downloader.AtomicWriteFile(result.ManifestPath, data, fetchedFileMode)
}
