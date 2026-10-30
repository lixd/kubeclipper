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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/google/go-containerregistry/pkg/crane"
	containerv1 "github.com/google/go-containerregistry/pkg/v1"

	digest "github.com/opencontainers/go-digest"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/logger"
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
	if f.RegistryConfig != nil {
		for _, c := range plan.Components {
			ref, err := ociReference(c.Transport)
			if err != nil {
				return nil, err
			}
			cfg := *f.RegistryConfig
			if err = cfg.ValidateReference(ref); err != nil {
				return nil, err
			}
		}
	}
	protected := planCachePaths(plan)
	if !f.DryRun {
		if err := downloader.PruneCache(ctx, protected); err != nil {
			logger.Warnf("OCI cache maintenance failed: %v", err)
		}
	}
	for repair := 0; repair < 2; repair++ {
		var lock *downloader.CacheLock
		var err error
		if !f.DryRun {
			lock, err = downloader.LockCache(ctx, "lifecycle", false, false)
			if err != nil {
				return nil, err
			}
		}
		result := &FetchResult{}
		for _, c := range plan.Components {
			var r ComponentFetchResult
			r, err = f.fetchComponent(ctx, plan.OS, plan.Arch, c)
			if err != nil {
				break
			}
			result.Components = append(result.Components, r)
		}
		if err != nil {
			if lock != nil {
				_ = lock.Release()
			}
			var corrupt *corruptCache
			if repair == 0 && errors.As(err, &corrupt) {
				logger.Warnf("invalid OCI cache detected; rebuilding %s", corrupt.path)
				exclusive, e := downloader.LockCache(ctx, "lifecycle", true, false)
				if e != nil {
					return nil, e
				}
				// Other consumers may have rebuilt it while we waited: invalidate only if still corrupt.
				stillBad := true
				if filepath.Base(corrupt.path) != "receipt.json" {
					for _, c := range plan.Components {
						r, contents, e := snapshotResult(plan.OS, plan.Arch, c)
						if e == nil && r.BaseDir == corrupt.path && validateSnapshot(ctx, r, contents) == nil {
							stillBad = false
						}
					}
				}

				if strings.Contains(corrupt.path, string(filepath.Separator)+"blobs"+string(filepath.Separator)) {
					d := digest.Digest(filepath.Base(filepath.Dir(corrupt.path)) + ":" + filepath.Base(corrupt.path))
					if info, statErr := os.Stat(corrupt.path); statErr == nil {
						if store, openErr := openContent(); openErr == nil {
							if ok, verifyErr := store.verified(ctx, ocispec.Descriptor{Digest: d, Size: info.Size()}); verifyErr == nil && ok {
								stillBad = false
							}
						}
					}
				}
				if stillBad {
					e = os.RemoveAll(corrupt.path)
				}

				releaseErr := exclusive.Release()
				if e != nil {
					return nil, e
				}
				if releaseErr != nil {
					return nil, releaseErr
				}
				continue
			}
			return nil, err
		}
		objects := planCachePaths(plan)
		result.lease = &resultLease{release: func() error {
			if lock == nil {
				return nil
			}
			for _, p := range objects {
				if err := downloader.TouchCache(p); err != nil {
					logger.Warnf("OCI cache access record failed: %v", err)
				}
			}
			err := lock.Release()
			if e := downloader.PruneCache(context.Background(), nil); e != nil {
				logger.Warnf("OCI cache maintenance failed: %v", e)
			}
			return err
		}}
		for i := range result.Components {
			result.Components[i].lease = result.lease
		}
		return result, nil
	}
	return nil, fmt.Errorf("cache repair exhausted")
}

func planCachePaths(plan *deliveryapis.ResolvedArtifactPlan) []string {
	var paths []string
	for _, c := range plan.Components {
		r, _, err := snapshotResult(plan.OS, plan.Arch, c)
		if err != nil {
			continue
		}
		paths = append(paths, r.BaseDir)
		dgst := digest.Digest(c.Transport.Digest)
		p := filepath.Join(downloader.CacheDir(), "content", "blobs", dgst.Algorithm().String(), dgst.Encoded())
		paths = append(paths, p)
		f, err := os.Open(p)
		if err != nil {
			continue
		}
		data, err := io.ReadAll(io.LimitReader(f, maxManifestSize+1))
		_ = f.Close()
		if err != nil || int64(len(data)) > maxManifestSize {
			continue
		}
		var m containerv1.Manifest
		if json.Unmarshal(data, &m) == nil {
			for _, l := range m.Layers {
				paths = append(paths, filepath.Join(downloader.CacheDir(), "content", "blobs", l.Digest.Algorithm, l.Digest.Hex))
			}
		}
	}
	return paths
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
	if config != nil {
		value := *config
		config = &value
	}
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
	manifestTargets := map[string]bool{}
	for _, content := range manifest.Contents {
		if _, exists := manifestContents[content.Name]; exists {
			return fmt.Errorf("duplicate package manifest content")
		}
		if content.Transport.Type != "" {
			manifestContents[content.Name] = content
			continue
		}
		if manifestTargets[content.File] {
			return fmt.Errorf("duplicate package manifest file")
		}
		if err := deliveryapis.ValidateContentFileName(content.File); err != nil {
			return err
		}
		if err := digest.Digest(content.Digest).Validate(); err != nil {
			return fmt.Errorf("invalid manifest payload digest")
		}
		manifestTargets[content.File] = true
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
	Package   deliveryapis.PackageManifest   `json:"package"`
}

func writeFetchedManifest(result ComponentFetchResult, contents []deliveryapis.ArtifactContent, manifest deliveryapis.PackageManifest, dryRun bool) error {
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
		Package:   manifest,
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
