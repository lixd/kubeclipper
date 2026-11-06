/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package releasemanifest

import (
	"io"
	"log"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/crane"
	registryserver "github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"
)

func TestReleaseVerifierPlatforms(t *testing.T) {
	amd64 := verifierImage(t, ArchAMD64, false)
	arm64 := verifierImage(t, ArchARM64, false)
	packageAMD64 := verifierImage(t, ArchAMD64, true)
	packageARM64 := verifierImage(t, ArchARM64, true)
	amdDescriptor := v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: ArchAMD64}}
	armDescriptor := v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: ArchARM64}}
	packages := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: packageAMD64, Descriptor: amdDescriptor},
		mutate.IndexAddendum{Add: packageARM64, Descriptor: armDescriptor})
	amdPackage := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{Add: packageAMD64, Descriptor: amdDescriptor})
	wrongRuntime := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{Add: amd64, Descriptor: armDescriptor})
	runtimes := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: amd64, Descriptor: amdDescriptor},
		mutate.IndexAddendum{Add: arm64, Descriptor: armDescriptor})
	tests := []struct {
		name      string
		source    any
		kind      string
		platforms []string
		arch      string
		wantError string
	}{
		{name: "packages", source: packages, kind: ArtifactTypePackage, platforms: []string{PlatformAMD64, PlatformARM64}},
		{name: "package-missing-arm64", source: amdPackage, kind: ArtifactTypePackage,
			platforms: []string{PlatformAMD64, PlatformARM64}, wantError: "source index does not contain platform linux/arm64"},
		{name: "package-single-image", source: amd64, kind: ArtifactTypePackage,
			platforms: []string{PlatformAMD64, PlatformARM64}, wantError: "single image does not contain all requested platforms"},
		{name: "package-single-platform", source: amd64, kind: ArtifactTypePackage, platforms: []string{PlatformAMD64}},
		{name: "package-selected-platform", source: amdPackage, kind: ArtifactTypePackage,
			platforms: []string{PlatformAMD64, PlatformARM64}, arch: ArchAMD64},
		{name: "runtime-single-image", source: amd64, kind: ArtifactTypeRuntime,
			platforms: []string{PlatformARM64}, wantError: "source platform mismatch"},
		{name: "runtime-wrong-index-platform", source: wrongRuntime, kind: ArtifactTypeRuntime,
			platforms: []string{PlatformARM64}, wantError: "source platform mismatch"},
		{name: "runtime-index", source: runtimes, kind: ArtifactTypeRuntime, platforms: []string{PlatformARM64}},
		{name: "runtime-missing-platform", source: amdPackage, kind: ArtifactTypeRuntime,
			platforms: []string{PlatformARM64}, wantError: "source index does not contain platform linux/arm64"},
		{name: "runtime-single-platform", source: amd64, kind: ArtifactTypeRuntime, platforms: []string{PlatformAMD64}},
	}
	for _, tool := range []string{"crane", "skopeo"} {
		t.Run(tool, func(t *testing.T) {
			if _, err := exec.LookPath(tool); err != nil {
				t.Skipf("%s is required for the external verifier regression", tool)
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					server := httptest.NewServer(registryserver.New(registryserver.Logger(log.New(io.Discard, "", 0))))
					defer server.Close()
					prefix := strings.TrimPrefix(server.URL, "http://") + "/release"
					bootstrap := Artifact{Type: ArtifactTypePackage,
						Component: Component{Kind: "bootstrap", Name: "kubeclipper", Version: "v2.0.11"},
						Target:    "package-bootstrap-kubeclipper:v2.0.11", Platforms: []string{PlatformAMD64, PlatformARM64}}
					bootstrap.Source = prefix + "/" + bootstrap.Target
					manifest := &Manifest{APIVersion: APIVersion, Kind: Kind,
						Metadata:   Metadata{Name: "verifier", Version: "v2.0.11"},
						Registries: Registries{Package: prefix, Image: prefix}, Artifacts: []Artifact{bootstrap}}
					target := bootstrap.Target
					if test.kind == ArtifactTypeRuntime {
						require.NoError(t, writeContent(prefix+"/"+bootstrap.Target, packages, []crane.Option{crane.Insecure}))
						target = "pause:3.10"
						manifest.Artifacts = append(manifest.Artifacts, Artifact{Type: ArtifactTypeRuntime,
							Component: Component{Kind: "k8s", Name: "k8s", Version: "v1.37.0"},
							Source:    prefix + "/" + target, Target: target, Platforms: test.platforms})
					} else {
						manifest.Artifacts[0].Platforms = test.platforms
					}
					require.NoError(t, manifest.Validate())
					require.NoError(t, writeContent(prefix+"/"+target, test.source, []crane.Option{crane.Insecure}))
					data, err := yaml.Marshal(manifest)
					require.NoError(t, err)
					file := filepath.Join(t.TempDir(), "release.yaml")
					require.NoError(t, os.WriteFile(file, data, 0600))
					args := []string{"../../../scripts/open-packaging/verify-release-manifest.sh",
						"--manifest", file, "--registry", prefix, "--tool", tool, "--insecure"}
					if test.arch != "" {
						args = append(args, "--arch", test.arch)
					}
					output, err := exec.CommandContext(t.Context(), "bash", args...).CombinedOutput()
					if test.wantError != "" {
						require.Error(t, err, "%s", output)
						require.Contains(t, string(output), test.wantError)
					} else {
						require.NoError(t, err, "%s", output)
						require.Contains(t, string(output), "failures: 0")
					}
				})
			}
		})
	}
}

func verifierImage(t *testing.T, arch string, dataOnly bool) v1.Image {
	t.Helper()
	config, err := empty.Image.ConfigFile()
	require.NoError(t, err)
	config.Config.Labels = map[string]string{"test.platform": arch}
	if !dataOnly {
		config.OS, config.Architecture = "linux", arch
	}
	image, err := mutate.ConfigFile(empty.Image, config)
	require.NoError(t, err)
	return image
}
