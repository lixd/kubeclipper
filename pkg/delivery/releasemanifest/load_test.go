/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package releasemanifest

import (
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
