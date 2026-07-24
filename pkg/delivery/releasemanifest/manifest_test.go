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
	"fmt"
	"strings"
	"testing"
)

const testDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

func validManifestYAML() string {
	return fmt.Sprintf(`apiVersion: delivery.kubeclipper.io/v1alpha1
kind: ReleaseManifest
metadata:
  name: kubeclipper-resources
  version: v2.0.0
  sourceRevision: abc123
registries:
  package: ghcr.io/kubeclipper/kubeclipper
  image: ghcr.io/kubeclipper/kubeclipper
artifacts:
- type: package-image
  component: {kind: bootstrap, name: kubeclipper, version: v2.0.0}
  source: ghcr.io/kubeclipper/kubeclipper/kubeclipper/packages/bootstrap/kubeclipper:v2.0.0
  target: kubeclipper/packages/bootstrap/kubeclipper:v2.0.0
  platforms: [linux/amd64, linux/arm64]
  digest: %s
  sourceRevision: abc123
`, testDigest)
}

func TestParseStrictManifest(t *testing.T) {
	manifest, err := Parse([]byte(validManifestYAML()))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if manifest.Metadata.Version != "v2.0.0" || len(manifest.Artifacts) != 1 {
		t.Fatalf("Parse() = %+v", manifest)
	}
}

func TestParseRejectsUnsafeAndUnknownValues(t *testing.T) {
	tests := map[string]struct {
		old  string
		new  string
		want string
	}{
		"unknown field":       {"  sourceRevision: abc123\n", "  sourceRevision: abc123\n  unexpected: true\n", "unknown field"},
		"development version": {"  version: v2.0.0", "  version: v2.0.0-dirty", "stable"},
		"foreign source":      {"ghcr.io/kubeclipper/kubeclipper/kubeclipper/packages", "docker.io/example/packages", "must be under"},
		"parent target":       {"  target: kubeclipper/packages/bootstrap/kubeclipper:v2.0.0", "  target: ../kubeclipper:v2.0.0", "clean relative"},
		"bad digest":          {testDigest, "sha256:nope", "digest is invalid"},
		"revision mismatch":   {"  sourceRevision: abc123\nregistries:", "  sourceRevision: other\nregistries:", "does not match"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(strings.Replace(validManifestYAML(), test.old, test.new, 1)))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestParseRejectsDuplicateArtifact(t *testing.T) {
	document := validManifestYAML()
	artifactStart := strings.Index(document, "- type: package-image")
	if artifactStart < 0 {
		t.Fatal("test manifest is missing package artifact")
	}
	artifact := document[artifactStart:]
	_, err := Parse([]byte(document + artifact))
	if err == nil || !strings.Contains(err.Error(), "duplicates target") {
		t.Fatalf("Parse() error = %v", err)
	}
}
