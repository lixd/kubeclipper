/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package releasemanifest

import (
	"embed"
	"fmt"
	"os"
	"slices"

	"github.com/kubeclipper/kubeclipper/pkg/delivery/source"
)

//go:embed embedded/release-manifest.yaml
var embeddedRelease embed.FS

// Load reads an explicit manifest or the inventory embedded in release builds.
func Load(path string) (*Manifest, error) {
	var data []byte
	var err error
	if path == "" {
		data, err = embeddedRelease.ReadFile("embedded/release-manifest.yaml")
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, fmt.Errorf("read release manifest: %w", err)
	}
	manifest, err := Parse(data)
	if err != nil || path != "" {
		return manifest, err
	}
	registry, err := source.Registry("")
	if err != nil {
		return nil, err
	}
	return manifest.WithRegistry(registry)
}

// WithRegistry resolves mirror locations without changing the caller's manifest.
func (m *Manifest) WithRegistry(configured string) (*Manifest, error) {
	registry, err := source.RegistryOverride(configured)
	if err != nil {
		return nil, err
	}
	if registry == "" {
		return m, nil
	}
	resolved := *m
	resolved.Registries = Registries{Package: registry, Image: registry}
	resolved.Artifacts = slices.Clone(m.Artifacts)
	for i := range resolved.Artifacts {
		resolved.Artifacts[i].Source = registry + "/" + resolved.Artifacts[i].Target
	}
	if err := resolved.Validate(); err != nil {
		return nil, err
	}
	return &resolved, nil
}
