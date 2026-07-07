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

package apis

import (
	"fmt"
	"strings"
)

const PackageRepositoryPrefix = "kubeclipper/packages"

type PackageManifestFile struct {
	Name      string       `json:"name"`
	File      string       `json:"file"`
	MediaType string       `json:"mediaType,omitempty"`
	Digest    string       `json:"digest,omitempty"`
	Transport TransportRef `json:"transport,omitempty"`
}

type PackageManifestPlatform struct {
	OS   string `json:"os"`
	Arch string `json:"arch"`
}

type PackageManifest struct {
	SchemaVersion  int                     `json:"schemaVersion"`
	Kind           string                  `json:"kind"`
	Name           string                  `json:"name"`
	Version        string                  `json:"version"`
	ContentProfile string                  `json:"contentProfile,omitempty"`
	Platform       PackageManifestPlatform `json:"platform"`
	Contents       []PackageManifestFile   `json:"contents"`
}

type PackageRef struct {
	Registry   string
	Repository string
	Tag        string
	Digest     string
}

func ParsePackageRepository(repository string) (kind, name string, ok bool) {
	repository = strings.Trim(repository, "/")
	prefix := PackageRepositoryPrefix + "/"
	if !strings.HasPrefix(repository, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(repository, prefix)
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func DerivePackageEntryFromManifest(ref PackageRef, manifest PackageManifest) (PackageEntry, error) {
	if ref.Registry == "" {
		return PackageEntry{}, fmt.Errorf("registry is required")
	}
	if ref.Repository == "" {
		return PackageEntry{}, fmt.Errorf("repository is required")
	}
	if ref.Tag == "" {
		return PackageEntry{}, fmt.Errorf("tag is required")
	}
	if !digestRegexp.MatchString(ref.Digest) {
		return PackageEntry{}, fmt.Errorf("digest must be sha256:<64 hex>")
	}
	kind, name, ok := ParsePackageRepository(ref.Repository)
	if !ok {
		return PackageEntry{}, fmt.Errorf("repository %q is not under %s/{kind}/{name}", ref.Repository, PackageRepositoryPrefix)
	}
	if manifest.SchemaVersion != 1 {
		return PackageEntry{}, fmt.Errorf("unsupported package manifest schemaVersion %d", manifest.SchemaVersion)
	}
	if manifest.Kind != kind || manifest.Name != name || manifest.Version != ref.Tag {
		return PackageEntry{}, fmt.Errorf("manifest identity %s/%s:%s does not match repository %s/%s:%s", manifest.Kind, manifest.Name, manifest.Version, kind, name, ref.Tag)
	}
	profile := manifest.ContentProfile
	if profile == "" {
		profile = ContentProfileForKind(kind)
	}
	if manifest.Platform.Arch == "" {
		return PackageEntry{}, fmt.Errorf("platform arch is required")
	}
	if manifest.Platform.OS == "" {
		return PackageEntry{}, fmt.Errorf("platform os is required")
	}
	contents, err := contentsFromManifestFiles(manifest.Contents)
	if err != nil {
		return PackageEntry{}, fmt.Errorf("platform[%s/%s]: %w", manifest.Platform.OS, manifest.Platform.Arch, err)
	}
	entry := PackageEntry{
		Kind:           kind,
		Name:           name,
		Version:        ref.Tag,
		OS:             manifest.Platform.OS,
		Arch:           manifest.Platform.Arch,
		ContentProfile: profile,
		Transport: TransportRef{
			Type:   TransportOCI,
			Ref:    packageRef(ref.Registry, ref.Repository, ref.Tag),
			Digest: ref.Digest,
		},
		Contents: contents,
	}
	if err = validatePackageEntry(entry); err != nil {
		return PackageEntry{}, fmt.Errorf("derived package %s/%s:%s/%s/%s: %w", kind, name, ref.Tag, manifest.Platform.OS, manifest.Platform.Arch, err)
	}
	return entry, nil
}

func contentsFromManifestFiles(files []PackageManifestFile) ([]ArtifactContent, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("files are required")
	}
	contents := make([]ArtifactContent, 0, len(files))
	seen := make(map[string]struct{}, len(files))
	for i, file := range files {
		if file.Name == "" {
			return nil, fmt.Errorf("files[%d] name is required", i)
		}
		if file.File == "" {
			return nil, fmt.Errorf("files[%d] file is required", i)
		}
		if file.Transport.Type == "" && !digestRegexp.MatchString(file.Digest) {
			return nil, fmt.Errorf("files[%d] digest must be sha256:<64 hex>", i)
		}
		if _, exists := seen[file.Name]; exists {
			return nil, fmt.Errorf("duplicate file content %q", file.Name)
		}
		seen[file.Name] = struct{}{}
		mediaType := file.MediaType
		if mediaType == "" {
			mediaType = MediaTypeForContent(file.Name)
		}
		contents = append(contents, ArtifactContent{
			Name:      file.Name,
			File:      file.File,
			Digest:    file.Digest,
			MediaType: mediaType,
			Transport: file.Transport,
		})
	}
	return contents, nil
}

func packageRef(registry, repository, tag string) string {
	return fmt.Sprintf("%s/%s:%s", strings.TrimRight(registry, "/"), strings.Trim(repository, "/"), tag)
}
