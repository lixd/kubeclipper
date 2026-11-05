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
)

//go:embed embedded
var embeddedRelease embed.FS

// Load reads an explicit manifest or the inventory embedded in release builds.
func Load(path string) (*Manifest, error) {
	var data []byte
	var err error
	if path == "" {
		data, err = embeddedRelease.ReadFile("embedded/release-manifest.yaml")
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("embedded release manifest unavailable. Specify --manifest <file>")
		}
	} else {
		data, err = os.ReadFile(path)
	}
	if err != nil {
		return nil, fmt.Errorf("read release manifest: %w", err)
	}
	return Parse(data)
}
