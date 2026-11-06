/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package releasemanifest

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadManifestOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.yaml")
	require.NoError(t, os.WriteFile(path, []byte(validManifestYAML()), 0600))
	manifest, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "v2.0.0", manifest.Metadata.Version)
	_, err = Load(path + ".missing")
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, os.WriteFile(path, []byte("invalid: manifest"), 0600))
	_, err = Load(path)
	require.Error(t, err)
}

func TestManifestRegistrySelectionPreservesContentIdentity(t *testing.T) {
	t.Setenv("KCZONE", "")
	manifest, err := Parse([]byte(validManifestYAML()))
	require.NoError(t, err)
	original := manifest.Artifacts[0]
	resolved, err := manifest.WithRegistry("")
	require.NoError(t, err)
	require.Same(t, manifest, resolved)
	t.Setenv("KCZONE", "cn")
	resolved, err = manifest.WithRegistry("")
	require.NoError(t, err)
	require.Equal(t, "registry.cn-hangzhou.aliyuncs.com/kubeclipper-release/"+original.Target, resolved.Artifacts[0].Source)
	require.Equal(t, original, manifest.Artifacts[0])
	require.Equal(t, original.Digest, resolved.Artifacts[0].Digest)
	require.Equal(t, original.Platforms, resolved.Artifacts[0].Platforms)
	resolved, err = manifest.WithRegistry("docker.io/kubeclipper")
	require.NoError(t, err)
	require.Equal(t, "docker.io/kubeclipper/"+original.Target, resolved.Artifacts[0].Source)
	require.Equal(t, "docker.io/kubeclipper", resolved.Registries.Image)
}

func TestEmbeddedManifestUsesTagsWithoutReleaseHashes(t *testing.T) {
	t.Setenv("KCZONE", "cn")
	manifest, err := Load("")
	require.NoError(t, err)
	require.True(t, IsStableVersion(manifest.Metadata.Version))
	require.NotEmpty(t, manifest.Artifacts)
	require.Equal(t, manifest.Metadata.Version, manifest.BootstrapKubeClipperArtifact().Component.Version)
	for _, artifact := range manifest.Artifacts {
		require.Empty(t, artifact.Digest)
		require.Empty(t, artifact.SourceRevision)
		require.Contains(t, artifact.Target, ":")
		require.NotContains(t, artifact.Target, "@")
		require.Equal(t, "registry.cn-hangzhou.aliyuncs.com/kubeclipper-release/"+artifact.Target, artifact.Source)
	}
	data, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NotContains(t, string(data), `"digest"`)
	require.NotContains(t, string(data), `"sourceRevision"`)
}
