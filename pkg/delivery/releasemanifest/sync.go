/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package releasemanifest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"

	registryconfig "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
)

type SyncOptions struct {
	Registry      string
	Arch          string
	Config        *registryconfig.Config
	Out           io.Writer
	SourceOptions []crane.Option
}

type SyncResult struct {
	Copied  int
	Skipped int
}

type artifactGroup struct {
	Source    string
	Target    string
	Artifacts []Artifact
}

//nolint:gocyclo // Sync is the transaction boundary for source and target validation.
func Sync(ctx context.Context, manifest *Manifest, options *SyncOptions) (SyncResult, error) {
	var result SyncResult
	if manifest == nil {
		return result, fmt.Errorf("release manifest is required")
	}
	if options == nil {
		return result, fmt.Errorf("sync options are required")
	}
	if err := manifest.Validate(); err != nil {
		return result, err
	}
	options.Registry = strings.TrimRight(strings.TrimSpace(options.Registry), "/")
	if options.Registry == "" {
		return result, fmt.Errorf("target Registry is required")
	}
	if options.Arch == "" {
		options.Arch = ArchAll
	}
	if options.Arch != ArchAll && options.Arch != ArchAMD64 && options.Arch != ArchARM64 {
		return result, fmt.Errorf("architecture must be amd64, arm64, or all")
	}
	if options.Config == nil {
		return result, fmt.Errorf("target Registry config is required")
	}
	if err := options.Config.ValidateRegistry(options.Registry); err != nil {
		return result, err
	}
	destinationOptions, err := options.Config.CraneOptions(ctx)
	if err != nil {
		return result, err
	}
	sourceOptions := options.SourceOptions
	if len(sourceOptions) == 0 {
		sourceOptions = []crane.Option{crane.WithContext(ctx), crane.WithAuth(authn.Anonymous)}
	}

	for _, group := range groupArtifacts(manifest.Artifacts) {
		selected := selectedArtifacts(group.Artifacts, options.Arch)
		if len(selected) == 0 {
			continue
		}
		destination := options.Registry + "/" + group.Target
		expected, content, err := loadAndValidateSource(group.Source, selected, options.Arch, sourceOptions)
		if err != nil {
			return result, fmt.Errorf("validate source %s: %w", group.Source, err)
		}
		actual, exists, err := destinationDigest(destination, destinationOptions)
		if err != nil {
			return result, fmt.Errorf("inspect target %s: %w", destination, err)
		}
		if exists {
			if actual == expected {
				result.Skipped++
				writeStatus(options.Out, "skip", destination, expected)
				continue
			}
			return result, fmt.Errorf("target tag conflict: %s expected=%s actual=%s", destination, expected, actual)
		}
		if err = writeContent(destination, content, destinationOptions); err != nil {
			return result, fmt.Errorf("copy %s to %s: %w", group.Source, destination, err)
		}
		actual, exists, err = destinationDigest(destination, destinationOptions)
		if err != nil || !exists {
			return result, fmt.Errorf("verify target %s after copy: %w", destination, err)
		}
		if actual != expected {
			return result, fmt.Errorf("target digest mismatch after copy: %s expected=%s actual=%s", destination, expected, actual)
		}
		result.Copied++
		writeStatus(options.Out, "copy", destination, expected)
	}
	return result, nil
}

func groupArtifacts(artifacts []Artifact) []artifactGroup {
	groups := make(map[string]*artifactGroup)
	for i := range artifacts {
		artifact := &artifacts[i]
		key := artifact.Source + "\x00" + artifact.Target
		group := groups[key]
		if group == nil {
			group = &artifactGroup{Source: artifact.Source, Target: artifact.Target}
			groups[key] = group
		}
		group.Artifacts = append(group.Artifacts, *artifact)
	}
	result := make([]artifactGroup, 0, len(groups))
	for _, group := range groups {
		result = append(result, *group)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Target < result[j].Target })
	return result
}

func selectedArtifacts(artifacts []Artifact, arch string) []Artifact {
	if arch == ArchAll {
		return artifacts
	}
	platform := "linux/" + arch
	result := make([]Artifact, 0, len(artifacts))
	for i := range artifacts {
		artifact := &artifacts[i]
		if len(artifact.Platforms) == 0 || containsString(artifact.Platforms, platform) {
			result = append(result, *artifact)
		}
	}
	return result
}

//nolint:gocyclo // OCI indexes and manifests require distinct digest and provenance paths.
func loadAndValidateSource(
	source string,
	artifacts []Artifact,
	arch string,
	sourceOptions []crane.Option,
) (digest string, content any, err error) {
	descriptor, err := crane.Get(source, sourceOptions...)
	if err != nil {
		return "", nil, err
	}
	isIndex := descriptor.MediaType == types.OCIImageIndex || descriptor.MediaType == types.DockerManifestList
	if arch == ArchAll || len(artifacts[0].Platforms) == 0 {
		if artifacts[0].Type != ArtifactTypeRuntime && descriptor.Digest.String() != artifacts[0].Digest {
			return "", nil, fmt.Errorf("digest mismatch: manifest=%s source=%s", artifacts[0].Digest, descriptor.Digest)
		}
		if artifacts[0].Type == ArtifactTypeRuntime {
			if validationErr := validateRuntimeDigests(descriptor, artifacts); validationErr != nil {
				return "", nil, validationErr
			}
		}
		if artifacts[0].Type == ArtifactTypePackage {
			if validationErr := validatePackageProvenance(descriptor, &artifacts[0]); validationErr != nil {
				return "", nil, validationErr
			}
		}
		if isIndex {
			index, indexErr := descriptor.ImageIndex()
			return descriptor.Digest.String(), index, indexErr
		}
		image, imageErr := descriptor.Image()
		return descriptor.Digest.String(), image, imageErr
	}

	platform := "linux/" + arch
	if !isIndex {
		if descriptor.Digest.String() != artifacts[0].Digest {
			return "", nil, fmt.Errorf("digest mismatch: manifest=%s source=%s", artifacts[0].Digest, descriptor.Digest)
		}
		image, imageErr := descriptor.Image()
		return descriptor.Digest.String(), image, imageErr
	}
	index, err := descriptor.ImageIndex()
	if err != nil {
		return "", nil, err
	}
	child, err := platformDescriptor(index, platform)
	if err != nil {
		return "", nil, err
	}
	if artifacts[0].Type == ArtifactTypeRuntime && child.Digest.String() != artifacts[0].Digest {
		return "", nil, fmt.Errorf("platform %s digest mismatch: manifest=%s source=%s", platform, artifacts[0].Digest, child.Digest)
	}
	image, err := index.Image(child.Digest)
	if err != nil {
		return "", nil, err
	}
	if artifacts[0].Type == ArtifactTypePackage {
		if descriptor.Digest.String() != artifacts[0].Digest {
			return "", nil, fmt.Errorf("index digest mismatch: manifest=%s source=%s", artifacts[0].Digest, descriptor.Digest)
		}
		if err := validateImageRevision(image, artifacts[0].SourceRevision, platform); err != nil {
			return "", nil, err
		}
	}
	return child.Digest.String(), image, nil
}

func validateRuntimeDigests(descriptor *remote.Descriptor, artifacts []Artifact) error {
	if descriptor.MediaType != types.OCIImageIndex && descriptor.MediaType != types.DockerManifestList {
		if len(artifacts) != 1 || descriptor.Digest.String() != artifacts[0].Digest {
			return fmt.Errorf("runtime image digest mismatch")
		}
		return nil
	}
	index, err := descriptor.ImageIndex()
	if err != nil {
		return err
	}
	for i := range artifacts {
		artifact := &artifacts[i]
		child, err := platformDescriptor(index, artifact.Platforms[0])
		if err != nil {
			return err
		}
		if child.Digest.String() != artifact.Digest {
			return fmt.Errorf("platform %s digest mismatch: manifest=%s source=%s", artifact.Platforms[0], artifact.Digest, child.Digest)
		}
	}
	return nil
}

func validatePackageProvenance(descriptor *remote.Descriptor, artifact *Artifact) error {
	if descriptor.MediaType != types.OCIImageIndex && descriptor.MediaType != types.DockerManifestList {
		image, err := descriptor.Image()
		if err != nil {
			return err
		}
		return validateImageRevision(image, artifact.SourceRevision, artifact.Platforms[0])
	}
	index, err := descriptor.ImageIndex()
	if err != nil {
		return err
	}
	for _, platform := range artifact.Platforms {
		child, err := platformDescriptor(index, platform)
		if err != nil {
			return err
		}
		image, err := index.Image(child.Digest)
		if err != nil {
			return err
		}
		if err := validateImageRevision(image, artifact.SourceRevision, platform); err != nil {
			return err
		}
	}
	return nil
}

func validateImageRevision(image v1.Image, expected, platform string) error {
	config, err := image.ConfigFile()
	if err != nil {
		return fmt.Errorf("read package provenance for %s: %w", platform, err)
	}
	actual := config.Config.Labels["org.opencontainers.image.revision"]
	if actual != expected {
		return fmt.Errorf("package sourceRevision mismatch for %s: expected=%s actual=%s", platform, expected, actual)
	}
	return nil
}

func platformDescriptor(index v1.ImageIndex, platform string) (v1.Descriptor, error) {
	manifest, err := index.IndexManifest()
	if err != nil {
		return v1.Descriptor{}, err
	}
	parts := strings.SplitN(platform, "/", 2)
	for i := range manifest.Manifests {
		descriptor := &manifest.Manifests[i]
		if descriptor.Platform != nil && descriptor.Platform.OS == parts[0] && descriptor.Platform.Architecture == parts[1] {
			return *descriptor, nil
		}
	}
	return v1.Descriptor{}, fmt.Errorf("source index does not contain platform %s", platform)
}

func destinationDigest(reference string, options []crane.Option) (digest string, exists bool, err error) {
	descriptor, err := crane.Head(reference, options...)
	if err == nil {
		return descriptor.Digest.String(), true, nil
	}
	var transportError *transport.Error
	if errors.As(err, &transportError) && transportError.StatusCode == http.StatusNotFound {
		return "", false, nil
	}
	return "", false, err
}

func writeContent(reference string, content any, options []crane.Option) error {
	parsed := crane.GetOptions(options...)
	target, err := name.ParseReference(reference, parsed.Name...)
	if err != nil {
		return err
	}
	switch typed := content.(type) {
	case v1.ImageIndex:
		return remote.WriteIndex(target, typed, parsed.Remote...)
	case v1.Image:
		return remote.Write(target, typed, parsed.Remote...)
	default:
		return fmt.Errorf("unsupported source content %T", content)
	}
}

func containsString(values []string, value string) bool {
	return slices.Contains(values, value)
}

func writeStatus(out io.Writer, status, reference, digest string) {
	if out != nil {
		_, _ = fmt.Fprintf(out, "%s: %s@%s\n", status, reference, digest)
	}
}
