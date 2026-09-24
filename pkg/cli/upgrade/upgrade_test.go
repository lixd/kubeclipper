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
  - type: package-image
    component:
      kind: bootstrap
      name: console
      version: v2.0.3-rc.4
    source: registry.internal:5000/kubeclipper/packages/bootstrap/console:v2.0.3-rc.4
    target: kubeclipper/packages/bootstrap/console:v2.0.3-rc.4
    platforms:
      - linux/amd64
      - linux/arm64
    digest: sha256:1111111111111111111111111111111111111111111111111111111111111111
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

const testReleaseManifestNoConsole = `
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
	if entry := selectPackageEntry(inventory, "bootstrap", "kubeclipper", "v2.0.3-rc.4", "arm64"); entry == nil || entry.Arch != "arm64" {
		t.Fatalf("expected arm64 entry, got %+v", entry)
	}
	if entry := selectPackageEntry(inventory, "bootstrap", "kubeclipper", "v9.9.9", "amd64"); entry != nil {
		t.Fatalf("missing version must not resolve, got %+v", entry)
	}
	if entry := selectPackageEntry(inventory, "bootstrap", "console", "v2.0.3-rc.4", "amd64"); entry != nil {
		t.Fatalf("different package name must not resolve, got %+v", entry)
	}
	if entry := selectPackageEntry(nil, "bootstrap", "kubeclipper", "v2.0.3-rc.4", "amd64"); entry != nil {
		t.Fatalf("nil inventory must not resolve, got %+v", entry)
	}
}

func TestBootstrapConsoleArtifact(t *testing.T) {
	manifest := parseTestManifest(t)
	artifact := manifest.BootstrapConsoleArtifact()
	if artifact == nil {
		t.Fatal("bootstrap/console artifact not found")
	}
	if artifact.Component.Name != "console" || artifact.Component.Version != "v2.0.3-rc.4" {
		t.Fatalf("unexpected console artifact component %+v", artifact.Component)
	}
	if artifact.Target != "kubeclipper/packages/bootstrap/console:v2.0.3-rc.4" {
		t.Fatalf("unexpected artifact target %q", artifact.Target)
	}
}

func TestRequiredArtifactsFor(t *testing.T) {
	manifest := parseTestManifest(t)
	for _, component := range []string{"server", "agent", "kcctl"} {
		artifacts, err := requiredArtifactsFor(component, manifest)
		if err != nil || len(artifacts) != 1 || artifacts[0].Component.Name != "kubeclipper" {
			t.Fatalf("component %s: expected exactly the kubeclipper artifact, got %+v, %v", component, artifacts, err)
		}
	}
	consoleArtifacts, err := requiredArtifactsFor("console", manifest)
	if err != nil || len(consoleArtifacts) != 1 || consoleArtifacts[0].Component.Name != "console" {
		t.Fatalf("console: expected exactly the console artifact, got %+v, %v", consoleArtifacts, err)
	}
	allArtifacts, err := requiredArtifactsFor("all", manifest)
	if err != nil || len(allArtifacts) != 2 {
		t.Fatalf("all: expected both artifacts, got %+v, %v", allArtifacts, err)
	}
	if allArtifacts[0].Component.Name != "kubeclipper" || allArtifacts[1].Component.Name != "console" {
		t.Fatalf("all: artifact order must keep the platform package primary, got %s then %s",
			allArtifacts[0].Component.Name, allArtifacts[1].Component.Name)
	}

	// A manifest without the console package must be rejected for console/all.
	bare := parseBareManifest(t)
	if _, err := requiredArtifactsFor("console", bare); err == nil || !strings.Contains(err.Error(), "bootstrap/console") {
		t.Fatalf("console upgrade without console artifact must be refused, got %v", err)
	}
	if _, err := requiredArtifactsFor("all", bare); err == nil || !strings.Contains(err.Error(), "bootstrap/console") {
		t.Fatalf("upgrade all without console artifact must be refused, got %v", err)
	}
	if artifacts, err := requiredArtifactsFor("server", bare); err != nil || len(artifacts) != 1 {
		t.Fatalf("server upgrade must not require the console artifact, got %+v, %v", artifacts, err)
	}
}

func TestParseVersionJSON(t *testing.T) {
	// platform binaries print plain JSON
	info, err := parseVersionJSON(`{"gitVersion":"v2.0.3-rc.4","gitCommit":"b23a9ab2"}` + "\n")
	if err != nil || info.GitCommit != "b23a9ab2" {
		t.Fatalf("plain JSON parse failed: %+v, %v", info, err)
	}
	// kcctl prefixes the JSON with a banner line
	info, err = parseVersionJSON("kcctl version:\n" + `{"gitVersion":"v2.0.3-rc.4","gitCommit":"b23a9ab2"}` + "\n")
	if err != nil || info.GitCommit != "b23a9ab2" || info.GitVersion != "v2.0.3-rc.4" {
		t.Fatalf("banner-prefixed JSON parse failed: %+v, %v", info, err)
	}
	// kcctl with a reachable platform config appends the server version object;
	// the CLIENT object (first) is the one that identifies the binary
	info, err = parseVersionJSON("kcctl version:\n" +
		"{\n\t\"gitVersion\": \"v2.0.3-rc.4\",\n\t\"gitCommit\": \"b23a9ab2\"\n}\n" +
		"kubeclipper-server version:\n" +
		"{\n\t\"gitVersion\": \"v2.0.3-rc.9\",\n\t\"gitCommit\": \"88d4820b\"\n}\n")
	if err != nil || info.GitCommit != "b23a9ab2" {
		t.Fatalf("two-object output must parse to the first object: %+v, %v", info, err)
	}
	if _, err := parseVersionJSON("no json here"); err == nil {
		t.Fatal("output without JSON must fail to parse")
	}
}

// parseBareManifest: same release manifest minus the console artifact —
// the shape manifests had before the console component was added.
func parseBareManifest(t *testing.T) *releasemanifest.Manifest {
	t.Helper()
	manifest, err := releasemanifest.Parse([]byte(testReleaseManifestNoConsole))
	if err != nil {
		t.Fatalf("parse bare release manifest: %v", err)
	}
	return manifest
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

func TestDedupNodes(t *testing.T) {
	nodes := []nodePlan{
		{role: roleServer, host: "10.0.0.1", arch: "amd64", currentRevision: "rev-1"},
		{role: roleServer, host: "10.0.0.2", arch: "amd64", currentRevision: "rev-1"},
		{role: roleServer, host: "10.0.0.1", arch: "amd64", currentRevision: ""},     // duplicate, probe glitch
		{role: roleAgent, host: "10.0.0.1", arch: "amd64", currentRevision: "rev-1"}, // same host, other role: keep
		{role: roleAgent, host: "10.0.0.1", arch: "amd64", currentRevision: "rev-1"}, // duplicate
	}
	kept := dedupNodes(nodes)
	if len(kept) != 3 {
		t.Fatalf("expected 3 nodes after dedup, got %d: %+v", len(kept), kept)
	}
	if kept[0].role != roleServer || kept[0].host != "10.0.0.1" || kept[0].currentRevision != "rev-1" {
		t.Fatalf("first occurrence must win, got %+v", kept[0])
	}
	if kept[2].role != roleAgent {
		t.Fatalf("agent entry for the same host must be kept, got %+v", kept[2])
	}
}

func TestEvaluateVersionPolicySameRevisionLowerVersionRefused(t *testing.T) {
	// A manifest pinning the current revision but claiming an older version
	// must be refused as a downgrade, not treated as an idempotent re-run.
	err := evaluateVersionPolicy("v2.0.3-rc.4", "rev-1", "v2.0.2", "rev-1")
	if err == nil || !strings.Contains(err.Error(), "refusing implicit downgrade") {
		t.Fatalf("expected downgrade refusal despite matching revision, got %v", err)
	}
}
