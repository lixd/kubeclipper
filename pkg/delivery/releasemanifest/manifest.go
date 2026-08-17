/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package releasemanifest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"sigs.k8s.io/yaml"
)

const (
	APIVersion           = "delivery.kubeclipper.io/v1alpha1"
	Kind                 = "ReleaseManifest"
	OfficialSourcePrefix = "ghcr.io/kubeclipper/kubeclipper"

	ArtifactTypePackage = "package-image"
	ArtifactTypeChart   = "helm-chart"
	ArtifactTypeRuntime = "runtime-image"
	ArchAMD64           = "amd64"
	ArchARM64           = "arm64"
	ArchAll             = "all"
	PlatformAMD64       = "linux/amd64"
	PlatformARM64       = "linux/arm64"
)

var stableVersionPattern = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

type Manifest struct {
	APIVersion string     `json:"apiVersion"`
	Kind       string     `json:"kind"`
	Metadata   Metadata   `json:"metadata"`
	Registries Registries `json:"registries"`
	Artifacts  []Artifact `json:"artifacts"`
}

type Metadata struct {
	Name           string `json:"name"`
	Version        string `json:"version"`
	SourceRevision string `json:"sourceRevision"`
}

type Registries struct {
	Package string `json:"package"`
	Image   string `json:"image"`
}

type Artifact struct {
	Type           string    `json:"type"`
	Component      Component `json:"component"`
	Source         string    `json:"source"`
	Target         string    `json:"target"`
	Platforms      []string  `json:"platforms,omitempty"`
	Upstream       string    `json:"upstream,omitempty"`
	Digest         string    `json:"digest"`
	SourceRevision string    `json:"sourceRevision,omitempty"`
}

type Component struct {
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Version string `json:"version"`
}

func IsStableVersion(version string) bool {
	return stableVersionPattern.MatchString(version)
}

func Parse(data []byte) (*Manifest, error) {
	jsonData, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, fmt.Errorf("parse release manifest YAML: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(jsonData))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err = decoder.Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decode release manifest: %w", err)
	}
	var extra any
	if err = decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, fmt.Errorf("decode release manifest: multiple documents are not supported")
		}
		return nil, fmt.Errorf("decode release manifest: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func (m *Manifest) Validate() error { //nolint:gocyclo // Validation intentionally reports each malformed release field precisely.
	if m.APIVersion != APIVersion {
		return fmt.Errorf("release manifest apiVersion must be %s", APIVersion)
	}
	if m.Kind != Kind {
		return fmt.Errorf("release manifest kind must be %s", Kind)
	}
	if strings.TrimSpace(m.Metadata.Name) == "" {
		return fmt.Errorf("release manifest metadata.name is required")
	}
	if !IsStableVersion(m.Metadata.Version) {
		return fmt.Errorf("release manifest metadata.version must be a stable vX.Y.Z version")
	}
	if strings.TrimSpace(m.Metadata.SourceRevision) == "" {
		return fmt.Errorf("release manifest metadata.sourceRevision is required")
	}
	if len(m.Artifacts) == 0 {
		return fmt.Errorf("release manifest artifacts must not be empty")
	}
	packageRegistry := strings.TrimRight(m.Registries.Package, "/")
	imageRegistry := strings.TrimRight(m.Registries.Image, "/")
	if packageRegistry != OfficialSourcePrefix || imageRegistry != OfficialSourcePrefix {
		return fmt.Errorf("release manifest package and image registries must be %s", OfficialSourcePrefix)
	}

	seen := make(map[string]struct{}, len(m.Artifacts))
	targetSources := make(map[string]string, len(m.Artifacts))
	targetTypes := make(map[string]string, len(m.Artifacts))
	bootstrapRevision := ""
	for i := range m.Artifacts {
		artifact := &m.Artifacts[i]
		if err := artifact.validate(i); err != nil {
			return err
		}
		key := artifact.Target + "\x00" + strings.Join(artifact.Platforms, ",")
		if _, ok := seen[key]; ok {
			return fmt.Errorf("artifact[%d] duplicates target and platforms %q", i, artifact.Target)
		}
		seen[key] = struct{}{}
		if source, ok := targetSources[artifact.Target]; ok &&
			(source != artifact.Source || targetTypes[artifact.Target] != ArtifactTypeRuntime || artifact.Type != ArtifactTypeRuntime) {
			return fmt.Errorf("artifact[%d] conflicts with another artifact for target %q", i, artifact.Target)
		}
		targetSources[artifact.Target] = artifact.Source
		targetTypes[artifact.Target] = artifact.Type
		if artifact.Type == ArtifactTypePackage && artifact.Component.Kind == "bootstrap" && artifact.Component.Name == "kubeclipper" {
			if bootstrapRevision != "" {
				return fmt.Errorf("release manifest contains multiple bootstrap/kubeclipper artifacts")
			}
			bootstrapRevision = artifact.SourceRevision
		}
	}
	if bootstrapRevision == "" {
		return fmt.Errorf("release manifest bootstrap/kubeclipper artifact is required")
	}
	if bootstrapRevision != m.Metadata.SourceRevision {
		return fmt.Errorf("release manifest sourceRevision does not match bootstrap/kubeclipper sourceRevision")
	}
	return nil
}

func (a *Artifact) validate(index int) error { //nolint:gocyclo // Artifact validation keeps field-specific errors for release diagnostics.
	switch a.Type {
	case ArtifactTypePackage, ArtifactTypeChart, ArtifactTypeRuntime:
	default:
		return fmt.Errorf("artifact[%d].type is unsupported: %q", index, a.Type)
	}
	if strings.TrimSpace(a.Component.Kind) == "" || strings.TrimSpace(a.Component.Name) == "" || strings.TrimSpace(a.Component.Version) == "" {
		return fmt.Errorf("artifact[%d].component kind, name, and version are required", index)
	}
	a.Source = strings.TrimPrefix(strings.TrimSpace(a.Source), "oci://")
	if a.Source != OfficialSourcePrefix && !strings.HasPrefix(a.Source, OfficialSourcePrefix+"/") {
		return fmt.Errorf("artifact[%d].source must be under %s", index, OfficialSourcePrefix)
	}
	a.Target = strings.TrimSpace(a.Target)
	cleanTarget := path.Clean(a.Target)
	unsafeTarget := a.Target == "" || strings.HasPrefix(a.Target, "/") || cleanTarget == "." ||
		cleanTarget != a.Target || strings.HasPrefix(cleanTarget, "../")
	if unsafeTarget {
		return fmt.Errorf("artifact[%d].target must be a clean relative Registry path", index)
	}
	if !strings.Contains(path.Base(a.Target), ":") && !strings.Contains(a.Target, "@") {
		return fmt.Errorf("artifact[%d].target must include a tag or digest", index)
	}
	digest, err := v1.NewHash(a.Digest)
	if err != nil {
		return fmt.Errorf("artifact[%d].digest is invalid: %w", index, err)
	}
	if digest.Algorithm != "sha256" {
		return fmt.Errorf("artifact[%d].digest must use sha256", index)
	}
	platforms := make(map[string]struct{}, len(a.Platforms))
	for _, platform := range a.Platforms {
		if platform != PlatformAMD64 && platform != PlatformARM64 {
			return fmt.Errorf("artifact[%d].platforms contains unsupported platform %q", index, platform)
		}
		if _, ok := platforms[platform]; ok {
			return fmt.Errorf("artifact[%d].platforms contains duplicate platform %q", index, platform)
		}
		platforms[platform] = struct{}{}
	}
	if a.Type == ArtifactTypeRuntime && len(a.Platforms) != 1 {
		return fmt.Errorf("artifact[%d] runtime-image must declare exactly one platform", index)
	}
	if a.Type == ArtifactTypePackage {
		if len(a.Platforms) == 0 {
			return fmt.Errorf("artifact[%d] package-image platforms are required", index)
		}
		if strings.TrimSpace(a.SourceRevision) == "" {
			return fmt.Errorf("artifact[%d] package-image sourceRevision is required", index)
		}
	}
	return nil
}
