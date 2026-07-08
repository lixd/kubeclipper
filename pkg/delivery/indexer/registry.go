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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/go-containerregistry/pkg/crane"
	containerv1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/types"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/logger"
)

type RegistryClient interface {
	Catalog(ctx context.Context, registry string) ([]string, error)
	ListTags(ctx context.Context, registry, repository string) ([]string, error)
	Resolve(ctx context.Context, ref string) ([]RegistryPackageArtifact, error)
}

type RegistryPackageArtifact struct {
	Digest   string
	Platform *containerv1.Platform
	Image    containerv1.Image
}

type RegistryPackageInventoryIndexer struct {
	Client   RegistryClient
	CacheTTL time.Duration

	mu    sync.Mutex
	cache map[string]cachedInventory
}

func NewRegistryPackageInventoryIndexer(client RegistryClient) *RegistryPackageInventoryIndexer {
	if client == nil {
		client = craneRegistryClient{}
	}
	return &RegistryPackageInventoryIndexer{
		Client:   client,
		CacheTTL: 5 * time.Minute,
		cache:    make(map[string]cachedInventory),
	}
}

func (i *RegistryPackageInventoryIndexer) Index(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	return i.Get(ctx, registry)
}

func (i *RegistryPackageInventoryIndexer) Get(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	if registry == "" {
		return nil, fmt.Errorf("registry is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cached, ok := i.loadCache(registry); ok {
		return cached, nil
	}
	return i.Refresh(ctx, registry)
}

func (i *RegistryPackageInventoryIndexer) Refresh(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	if registry == "" {
		return nil, fmt.Errorf("registry is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	inventory, err := i.index(ctx, registry)
	if err != nil {
		return nil, err
	}
	i.storeCache(registry, inventory)
	return inventory, nil
}

func (i *RegistryPackageInventoryIndexer) index(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
	repositories, err := i.Client.Catalog(ctx, registry)
	if err != nil {
		return nil, err
	}
	inventory := deliveryapis.NewPackageInventory("registry")
	inventory.Spec.Registry = registry
	for _, repository := range repositories {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		if _, _, ok := deliveryapis.ParsePackageRepository(repository); !ok {
			continue
		}
		tags, err := i.Client.ListTags(ctx, registry, repository)
		if err != nil {
			return nil, err
		}
		for _, tag := range tags {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			ref := packageRef(registry, repository, tag)
			artifacts, err := i.Client.Resolve(ctx, ref)
			if err != nil {
				logger.Warnf("skip invalid OCI package tag %s: resolve failed: %v", ref, err)
				continue
			}
			for _, artifact := range artifacts {
				manifest, err := ReadPackageManifest(artifact.Image)
				if err != nil {
					logger.Warnf("skip invalid OCI package image %s@%s: read package manifest failed: %v", ref, artifact.Digest, err)
					continue
				}
				if err = validateArtifactPlatform(artifact.Platform, manifest.Platform); err != nil {
					logger.Warnf("skip invalid OCI package image %s@%s: validate package platform failed: %v", ref, artifact.Digest, err)
					continue
				}
				if err = validateManifestContentFiles(artifact.Image, manifest); err != nil {
					logger.Warnf("skip invalid OCI package image %s@%s: validate package contents failed: %v", ref, artifact.Digest, err)
					continue
				}
				entry, err := deliveryapis.DerivePackageEntryFromManifest(deliveryapis.PackageRef{
					Registry:   registry,
					Repository: repository,
					Tag:        tag,
					Digest:     artifact.Digest,
				}, manifest)
				if err != nil {
					logger.Warnf("skip invalid OCI package image %s@%s: derive package entry failed: %v", ref, artifact.Digest, err)
					continue
				}
				inventory.Spec.Packages = append(inventory.Spec.Packages, entry)
			}
		}
	}
	if err = inventory.Validate(); err != nil {
		return nil, err
	}
	return inventory, nil
}

type cachedInventory struct {
	inventory *deliveryapis.PackageInventory
	cachedAt  time.Time
}

func (i *RegistryPackageInventoryIndexer) loadCache(registry string) (*deliveryapis.PackageInventory, bool) {
	if i == nil || i.CacheTTL <= 0 {
		return nil, false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.cache == nil {
		return nil, false
	}
	cached, ok := i.cache[registry]
	if !ok {
		return nil, false
	}
	if time.Since(cached.cachedAt) > i.CacheTTL {
		delete(i.cache, registry)
		return nil, false
	}
	return cached.inventory, true
}

func (i *RegistryPackageInventoryIndexer) storeCache(registry string, inventory *deliveryapis.PackageInventory) {
	if i == nil || i.CacheTTL <= 0 || inventory == nil {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.cache == nil {
		i.cache = make(map[string]cachedInventory)
	}
	i.cache[registry] = cachedInventory{
		inventory: inventory,
		cachedAt:  time.Now(),
	}
}

func ReadPackageManifest(img containerv1.Image) (deliveryapis.PackageManifest, error) {
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

func packageRef(registry, repository, tag string) string {
	return fmt.Sprintf("%s/%s:%s", strings.TrimRight(registry, "/"), strings.Trim(repository, "/"), tag)
}

type craneRegistryClient struct{}

func (craneRegistryClient) Catalog(ctx context.Context, registry string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return crane.Catalog(registry, crane.Insecure)
}

func (craneRegistryClient) ListTags(ctx context.Context, registry, repository string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return crane.ListTags(fmt.Sprintf("%s/%s", strings.TrimRight(registry, "/"), strings.Trim(repository, "/")), crane.Insecure)
}

func (craneRegistryClient) Resolve(ctx context.Context, ref string) ([]RegistryPackageArtifact, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	desc, err := crane.Get(ref, crane.Insecure)
	if err != nil {
		return nil, err
	}
	switch desc.MediaType {
	case types.OCIImageIndex, types.DockerManifestList:
		index, err := desc.ImageIndex()
		if err != nil {
			return nil, err
		}
		indexManifest, err := index.IndexManifest()
		if err != nil {
			return nil, err
		}
		artifacts := make([]RegistryPackageArtifact, 0, len(indexManifest.Manifests))
		for _, manifest := range indexManifest.Manifests {
			if err = ctx.Err(); err != nil {
				return nil, err
			}
			image, err := crane.Pull(ref+"@"+manifest.Digest.String(), crane.Insecure)
			if err != nil {
				return nil, err
			}
			artifacts = append(artifacts, RegistryPackageArtifact{
				Digest:   manifest.Digest.String(),
				Platform: manifest.Platform,
				Image:    image,
			})
		}
		return artifacts, nil
	default:
		image, err := desc.Image()
		if err != nil {
			return nil, err
		}
		return []RegistryPackageArtifact{{
			Digest: desc.Digest.String(),
			Image:  image,
		}}, nil
	}
}

func validateArtifactPlatform(platform *containerv1.Platform, manifestPlatform deliveryapis.PackageManifestPlatform) error {
	if platform == nil {
		return nil
	}
	if manifestPlatform.OS != "" && platform.OS != manifestPlatform.OS {
		return fmt.Errorf("oci platform os %q does not match package manifest os %q", platform.OS, manifestPlatform.OS)
	}
	if platform.Architecture != manifestPlatform.Arch {
		return fmt.Errorf("oci platform arch %q does not match package manifest arch %q", platform.Architecture, manifestPlatform.Arch)
	}
	return nil
}

func validateManifestContentFiles(img containerv1.Image, manifest deliveryapis.PackageManifest) error {
	layers, err := img.Layers()
	if err != nil {
		return err
	}
	for _, content := range manifest.Contents {
		if content.Name == "" {
			continue
		}
		if content.Transport.Type != "" {
			continue
		}
		if content.Digest == "" {
			return fmt.Errorf("content %q digest is required", content.Name)
		}
		data, err := readPackageFile(layers, content.File)
		if err != nil {
			return fmt.Errorf("content %q file %q not found in package image: %w", content.Name, content.File, err)
		}
		digest, err := packageFilePayloadDigest(data, content.Name)
		if err != nil {
			return err
		}
		if digest != content.Digest {
			return fmt.Errorf("content %q digest mismatch: expected %s, got %s", content.Name, content.Digest, digest)
		}
	}
	return nil
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
	return nil, fmt.Errorf("%s not found", target)
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
	case deliveryapis.ContentConfigs, deliveryapis.ContentImages, deliveryapis.ContentCharts:
		return false
	default:
		return true
	}
}

func layerUncompressedDigest(layer containerv1.Layer) (string, error) {
	reader, err := layer.Uncompressed()
	if err != nil {
		return "", err
	}
	defer reader.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, reader); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}
