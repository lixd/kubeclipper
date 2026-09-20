/*
 *
 *  * Copyright 2026 KubeClipper Authors.
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

package upgrade

import (
	"strings"
	"testing"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/delivery/releasemanifest"
)

const testReleaseManifest = `
apiVersion: delivery.kubeclipper.io/v1alpha1
kind: ReleaseManifest
metadata:
  name: kubeclipper
  version: v2.0.3
  sourceRevision: b23a9ab2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
registries:
  package: registry.internal:5000
  image: registry.internal:5000
artifacts:
  - type: package-image
    component:
      kind: bootstrap
      name: kubeclipper
      version: v2.0.3-rc.4
    source: registry.internal:5000/kubeclipper/packages/bootstrap/kubeclipper:v2.0.3-rc.4
    target: kubeclipper/packages/bootstrap/kubeclipper:v2.0.3-rc.4
    platforms:
      - linux/amd64
      - linux/arm64
    digest: sha256:0000000000000000000000000000000000000000000000000000000000000000
    sourceRevision: b23a9ab2aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
`

func parseTestManifest(t *testing.T) *releasemanifest.Manifest {
	t.Helper()
	manifest, err := releasemanifest.Parse([]byte(testReleaseManifest))
	if err != nil {
		t.Fatalf("parse release manifest: %v", err)
	}
	return manifest
}

func TestEvaluateVersionPolicyDowngradeRefused(t *testing.T) {
	err := evaluateVersionPolicy("v2.0.4", "rev-new", "v2.0.3", "rev-old")
	if err == nil || !strings.Contains(err.Error(), "refusing implicit downgrade") {
		t.Fatalf("expected downgrade refusal, got %v", err)
	}
}

func TestEvaluateVersionPolicySameRevisionIdempotent(t *testing.T) {
	if err := evaluateVersionPolicy("v2.0.3-rc.4", "rev-1", "v2.0.3-rc.4", "rev-1"); err != nil {
		t.Fatalf("same revision must be an idempotent no-op, got %v", err)
	}
}

func TestEvaluateVersionPolicySameVersionDifferentRevision(t *testing.T) {
	if err := evaluateVersionPolicy("v2.0.3-rc.3", "rev-3", "v2.0.3-rc.4", "rev-4"); err != nil {
		t.Fatalf("same-version re-install must be allowed, got %v", err)
	}
}

func TestEvaluateVersionPolicyUpgradeAllowed(t *testing.T) {
	if err := evaluateVersionPolicy("v2.0.3", "rev-3", "v2.0.4", "rev-4"); err != nil {
		t.Fatalf("forward upgrade must be allowed, got %v", err)
	}
}

func TestNormalizeUnameArch(t *testing.T) {
	for uname, want := range map[string]string{"x86_64": "amd64", "aarch64": "arm64"} {
		got, err := normalizeUnameArch(uname)
		if err != nil || got != want {
			t.Fatalf("normalizeUnameArch(%q) = %q, %v; want %q", uname, got, err, want)
		}
	}
	if _, err := normalizeUnameArch("ppc64le"); err == nil {
		t.Fatal("unsupported architecture must be rejected")
	}
}

func TestSelectPackageEntry(t *testing.T) {
	inventory := &deliveryapis.PackageInventory{}
	inventory.Spec.Packages = append(inventory.Spec.Packages,
		deliveryapis.PackageEntry{Kind: "bootstrap", Name: "kubeclipper", Version: "v2.0.3-rc.4", Arch: "amd64"},
		deliveryapis.PackageEntry{Kind: "bootstrap", Name: "kubeclipper", Version: "v2.0.3-rc.4", Arch: "arm64"},
		deliveryapis.PackageEntry{Kind: "bootstrap", Name: "etcd", Version: "v2.0.3-rc.4", Arch: "amd64"},
		deliveryapis.PackageEntry{Kind: "bootstrap", Name: "kubeclipper", Version: "v2.0.2", Arch: "amd64"},
	)
	if entry := selectPackageEntry(inventory, "v2.0.3-rc.4", "arm64"); entry == nil || entry.Arch != "arm64" {
		t.Fatalf("expected arm64 entry, got %+v", entry)
	}
	if entry := selectPackageEntry(inventory, "v9.9.9", "amd64"); entry != nil {
		t.Fatalf("missing version must not resolve, got %+v", entry)
	}
	if entry := selectPackageEntry(nil, "v2.0.3-rc.4", "amd64"); entry != nil {
		t.Fatalf("nil inventory must not resolve, got %+v", entry)
	}
}

func TestBootstrapKubeClipperArtifact(t *testing.T) {
	manifest := parseTestManifest(t)
	artifact := manifest.BootstrapKubeClipperArtifact()
	if artifact == nil {
		t.Fatal("bootstrap/kubeclipper artifact not found")
	}
	if artifact.Component.Version != "v2.0.3-rc.4" {
		t.Fatalf("unexpected component version %q", artifact.Component.Version)
	}
	if artifact.Target != "kubeclipper/packages/bootstrap/kubeclipper:v2.0.3-rc.4" {
		t.Fatalf("unexpected artifact target %q", artifact.Target)
	}
}

func TestManifestArtifactKeepsTargetPathAndTag(t *testing.T) {
	// The upgrade consumes registries.package + artifact.target verbatim so a
	// synced bundle keeps its repository path and pinned tag: the registry
	// prefix is only mapped, never re-resolved to a different tag.
	manifest := parseTestManifest(t)
	artifact := manifest.BootstrapKubeClipperArtifact()
	wantRef := manifest.Registries.Package + "/" + artifact.Target
	if wantRef != "registry.internal:5000/kubeclipper/packages/bootstrap/kubeclipper:v2.0.3-rc.4" {
		t.Fatalf("target reference %q does not keep the manifest repository path and tag", wantRef)
	}
	if !strings.Contains(artifact.Target, ":") {
		t.Fatal("artifact target must include the pinned tag")
	}
}
