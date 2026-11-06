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

package publisher

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	containerregistry "github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/stretchr/testify/require"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
)

// Run crane's Registry APIs in subprocesses against the in-process Distribution
// registry. No external registry or packaging downloads are used.
func TestEtcdReleaseCraneProcess(t *testing.T) {
	if os.Getenv("KC_ETCD_RELEASE_CRANE_PROCESS") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg != "--" {
			continue
		}
		args := os.Args[i+1:]
		var output string
		var err error
		switch args[0] {
		case "digest":
			output, err = crane.Digest(args[1])
		case "manifest":
			var data []byte
			data, err = crane.Manifest(args[1])
			output = string(data)
		case "config":
			var platform *v1.Platform
			platform, err = v1.ParsePlatform(args[2])
			if err == nil {
				var data []byte
				data, err = crane.Config(args[3], crane.WithPlatform(platform))
				output = string(data)
			}
		case "copy":
			err = crane.Copy(args[1], args[2])
		case "publish-fixture":
			var image v1.Image
			image, err = crane.Pull(args[1])
			if err == nil {
				config := &deliveryregistry.Config{Registry: strings.Split(args[2], "/")[0], Scheme: deliveryregistry.SchemeHTTP}
				err = pushPackageIndex(t.Context(), args[2], image, args[3], config)
			}
		default:
			t.Fatalf("unexpected crane operation: %v", args)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(output)
		os.Exit(0)
	}
	t.Fatal("crane arguments missing")
}

func TestEtcdReleaseReusesOriginalMultiArchPackage(t *testing.T) { //nolint:gocyclo,funlen // One Registry fixture verifies reuse and rejection paths.
	registryHandler := containerregistry.New()
	server := httptest.NewServer(registryHandler)
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	canonicalPrefix := host + "/canonical"
	canonical := canonicalPrefix + "/package-bootstrap-etcd:3.5.21"
	var index v1.ImageIndex = empty.Index
	for _, arch := range []string{"amd64", "arm64"} {
		img := testEtcdReleaseImage(t, arch, strings.Repeat("a", 40))
		index = buildPackageIndex(index, img, arch)
	}
	ref, parseErr := name.ParseReference(canonical)
	if parseErr != nil {
		t.Fatal(parseErr)
	}
	if writeErr := remote.WriteIndex(ref, index); writeErr != nil {
		t.Fatal(writeErr)
	}
	want, digestErr := index.Digest()
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	run := testEtcdReleaseRunner(t)
	for _, revision := range []string{strings.Repeat("b", 40), strings.Repeat("c", 40)} {
		targetPrefix := host + "/qualification-" + revision
		for range 2 {
			output, runErr := run(canonicalPrefix, targetPrefix, revision)
			if runErr != nil {
				t.Fatalf("reuse etcd package: %v\n%s", runErr, output)
			}
		}
		target := targetPrefix + "/package-bootstrap-etcd:3.5.21"
		actual, readErr := crane.Digest(target)
		if readErr != nil || actual != want.String() {
			t.Fatalf("candidate package digest = %q, %v; want %s", actual, readErr, want)
		}
		for _, arch := range []string{"amd64", "arm64"} {
			img, pullErr := crane.Pull(target, crane.WithPlatform(&v1.Platform{OS: "linux", Architecture: arch}))
			if pullErr != nil {
				t.Fatal(pullErr)
			}
			manifest, manifestErr := deliveryManifestFromImage(img)
			if manifestErr != nil || manifest.SourceRevision != strings.Repeat("a", 40) {
				t.Fatalf("reused %s manifest = %+v, %v", arch, manifest, manifestErr)
			}
		}
	}

	// A conflicting target remains untouched.
	conflict := host + "/conflict/package-bootstrap-etcd:3.5.21"
	if copyErr := crane.Copy(canonical, conflict); copyErr != nil {
		t.Fatal(copyErr)
	}
	other := buildPackageIndex(index, testEtcdReleaseImage(t, "amd64", strings.Repeat("d", 40)), "amd64")
	conflictRef, conflictParseErr := name.ParseReference(conflict)
	if conflictParseErr != nil {
		t.Fatal(conflictParseErr)
	}
	if writeErr := remote.WriteIndex(conflictRef, other); writeErr != nil {
		t.Fatal(writeErr)
	}
	output, conflictErr := run(canonicalPrefix, host+"/conflict", strings.Repeat("b", 40))
	if conflictErr == nil || !strings.Contains(string(output), "versioned package tag conflict") {
		t.Fatalf("conflicting target error = %v\n%s", conflictErr, output)
	}
	got, readErr := crane.Digest(conflict)
	otherDigest, otherDigestErr := other.Digest()
	if readErr != nil || otherDigestErr != nil || got != otherDigest.String() {
		t.Fatalf("conflicting target was modified: %s, %v, %v", got, readErr, otherDigestErr)
	}

	// Authorization errors must never trigger rebuilding or publication.
	denied := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer denied.Close()
	deniedOutput, deniedErr := run(strings.TrimPrefix(denied.URL, "http://"), host+"/denied", strings.Repeat("b", 40))
	if deniedErr == nil || !strings.Contains(string(deniedOutput), "cannot inspect versioned package") {
		t.Fatalf("authorization failure = %v\n%s", deniedErr, deniedOutput)
	}

	// An incomplete or mixed-provenance shared index is rejected before copying.
	for _, invalid := range []v1.ImageIndex{
		buildPackageIndex(empty.Index, testEtcdReleaseImage(t, "amd64", strings.Repeat("a", 40)), "amd64"),
		other,
	} {
		if writeErr := remote.WriteIndex(ref, invalid); writeErr != nil {
			t.Fatal(writeErr)
		}
		invalidOutput, invalidErr := run(canonicalPrefix, host+"/invalid", strings.Repeat("b", 40))
		if invalidErr == nil {
			t.Fatalf("invalid shared package was accepted\n%s", invalidOutput)
		}
		if _, lookupErr := crane.Digest(host + "/invalid/package-bootstrap-etcd:3.5.21"); lookupErr == nil {
			t.Fatal("invalid shared package was copied")
		}
	}
}

func testEtcdReleaseImage(t *testing.T, arch, revision string) v1.Image {
	t.Helper()
	return testVersionedReleaseImage(t, "bootstrap", "etcd", "3.5.21", arch, revision)
}

func testVersionedReleaseImage(t *testing.T, kind, packageName, version, arch, revision string) v1.Image {
	t.Helper()
	dir := t.TempDir()
	payload := filepath.Join(dir, "etcd")
	if err := os.WriteFile(payload, []byte("etcd fixture "+arch), 0600); err != nil {
		t.Fatal(err)
	}
	req := PublishRequest{
		Kind: kind, Name: packageName, Version: version, Arch: arch,
		SourceURL: "https://github.com/fixture/kubeclipper", SourceRevision: revision,
	}
	payloads := []payloadFile{{name: "etcd", file: "etcd", path: payload}}
	manifest, err := writePackageManifest(dir, req, deliveryapis.ContentProfileBinary, payloads, nil)
	if err != nil {
		t.Fatal(err)
	}
	img, err := buildArtifactImage(manifest, payloads, req)
	if err != nil {
		t.Fatal(err)
	}
	return img
}

func testEtcdReleaseRunner(t *testing.T) func(string, string, string) ([]byte, error) {
	t.Helper()
	script, err := filepath.Abs("../../../scripts/open-packaging/publish-component-release.sh")
	if err != nil {
		t.Fatal(err)
	}
	return testVersionedReleaseRunner(t, script, "bootstrap-etcd", "3.5.21")
}

func testVersionedReleaseRunner(t *testing.T, script, component, version string) func(string, string, string) ([]byte, error) {
	t.Helper()
	bin := t.TempDir()
	launcher := "#!/usr/bin/env bash\nKC_ETCD_RELEASE_CRANE_PROCESS=1 exec \"$KC_ETCD_RELEASE_TEST_BIN\" -test.run=^TestEtcdReleaseCraneProcess$ -- \"$@\"\n"
	if err := os.WriteFile(filepath.Join(bin, "crane"), []byte(launcher), 0755); err != nil { // #nosec G306 -- Executable test launcher.
		t.Fatal(err)
	}
	t.Setenv("KC_ETCD_RELEASE_TEST_BIN", os.Args[0])
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("DOCKER_CONFIG", bin)
	t.Setenv("KC_SOURCE_URL", "https://github.com/fixture/kubeclipper")
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	return func(canonical, target, revision string) ([]byte, error) {
		cmd := exec.CommandContext(t.Context(), "bash", script, component, canonical, target, version)
		cmd.Env = append(os.Environ(), "KC_SOURCE_REVISION="+revision)
		return cmd.CombinedOutput()
	}
}

func TestOtherVersionedPackagesReuseOriginalIndex(t *testing.T) {
	cases := []struct{ component, kind, name, version string }{
		{"bootstrap-console", "bootstrap", "console", "v1.6.1"},
		{"bootstrap-registry", "bootstrap", "registry", "3.1.1"},
		{"resource-k8s", "k8s", "k8s", "v1.37.0"},
		{"resource-containerd", "cri", "containerd", "2.2.4"},
		{"resource-k8s-extension", "k8s-extension", "k8s-extension", "v1"},
		{"resource-calico", "cni", "calico", "v3.31.5"},
	}
	for _, tc := range cases {
		t.Run(tc.component, func(t *testing.T) {
			server := httptest.NewServer(containerregistry.New())
			defer server.Close()
			host := strings.TrimPrefix(server.URL, "http://")
			canonical := host + "/canonical"
			repository := "package-" + tc.name
			if tc.kind == "bootstrap" {
				repository = "package-bootstrap-" + tc.name
			}
			var index v1.ImageIndex = empty.Index
			for _, arch := range []string{"amd64", "arm64"} {
				img := testVersionedReleaseImage(t, tc.kind, tc.name, tc.version, arch, strings.Repeat("a", 40))
				index = buildPackageIndex(index, img, arch)
			}
			ref, err := name.ParseReference(canonical + "/" + repository + ":" + tc.version)
			if err != nil {
				t.Fatal(err)
			}
			if err = remote.WriteIndex(ref, index); err != nil {
				t.Fatal(err)
			}
			want, err := index.Digest()
			if err != nil {
				t.Fatal(err)
			}
			script, resourceDir := testResourceReleaseInputs(t, tc.component)
			run := testVersionedReleaseRunner(t, script, tc.component, tc.version)
			for _, revision := range []string{strings.Repeat("b", 40), strings.Repeat("c", 40)} {
				target := host + "/qualification-" + revision
				output, runErr := run(canonical, target, revision)
				if runErr != nil {
					t.Fatalf("reuse %s: %v\n%s", tc.component, runErr, output)
				}
				actual, readErr := crane.Digest(target + "/" + repository + ":" + tc.version)
				if readErr != nil || actual != want.String() {
					t.Fatalf("reused index = %s, %v; want %s", actual, readErr, want)
				}
			}
			if tc.kind != "bootstrap" {
				data, readErr := os.ReadFile(filepath.Join(resourceDir, "images.lock"))
				if readErr != nil || !strings.Contains(string(data), "\tamd64\t") || !strings.Contains(string(data), "\tarm64\t") {
					t.Fatalf("resource assembly inputs missing: %s, %v", data, readErr)
				}
			}
		})
	}
}

func testResourceReleaseInputs(t *testing.T, component string) (scriptPath, resourceDir string) {
	t.Helper()
	root, err := filepath.Abs("../../../scripts/open-packaging")
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(component, "bootstrap-") {
		return filepath.Join(root, "publish-component-release.sh"), ""
	}
	// Fixture builders exercise the real resource publisher and metadata writer
	// while the Registry reads and multi-architecture copies use crane's APIs.
	dir := filepath.Join(t.TempDir(), "scripts", "open-packaging")
	for _, path := range []string{"publish-component-release.sh", "resource-packages/common.sh", "generate-resource-metadata.sh", "image_repository.py"} {
		data, readErr := os.ReadFile(filepath.Join(root, path))
		if readErr != nil {
			t.Fatal(readErr)
		}
		target := filepath.Join(dir, path)
		if mkdirErr := os.MkdirAll(filepath.Dir(target), 0700); mkdirErr != nil {
			t.Fatal(mkdirErr)
		}
		if writeErr := os.WriteFile(target, data, 0755); writeErr != nil { // #nosec G306,G703 -- Executable fixtures use fixed names under t.TempDir.
			t.Fatal(writeErr)
		}
	}
	builder := `#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/resource-packages/common.sh"
name="${0##*/publish-resource-}"
name="${name%.sh}"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry-prefix) registry="$2" ;;
    --version) version="$2" ;;
    --arch) arch="$2" ;;
    --image-registry-prefix) image_registry="$2" ;;
    *) exit 2 ;;
  esac
  shift 2
done
if [[ -n "${KC_RELEASE_BUILDER_LOG:-}" ]]; then
  printf '%s\n' "$arch" >> "$KC_RELEASE_BUILDER_LOG"
fi
if [[ "$arch" == arm64 && -n "${KC_RELEASE_FAIL_ARM_ONCE:-}" && ! -e "$KC_RELEASE_FAIL_ARM_ONCE" ]]; then
  touch "$KC_RELEASE_FAIL_ARM_ONCE"
  exit 23
fi
resource_dir="$KC_RESOURCE_DIR"
leaf="$resource_dir/$name/$version/$arch"
mkdir -p "$leaf"
printf 'fixture payload from %s\n' "$KC_SOURCE_REVISION" > "$leaf/configs.tar.gz"
printf 'registry.k8s.io/pause:3.10\n' > "$leaf/images.txt"
kind="$name"
[[ "$name" != containerd ]] || kind=cri
[[ "$name" != calico ]] || kind=cni
init_resource_publish_workspace
publish_resource_package "$name" "$kind" "$name" core "$leaf"
"$SCRIPT_DIR/generate-resource-metadata.sh" --resource-dir "$resource_dir" --image-registry "${image_registry:-$registry}"
`
	if err := os.WriteFile(filepath.Join(dir, "publish-"+component+".sh"), []byte(builder), 0755); err != nil { // #nosec G306 -- Executable fixture builder.
		t.Fatal(err)
	}
	resourceDir = t.TempDir()
	t.Setenv("KC_RESOURCE_DIR", resourceDir)
	t.Setenv("PYTHONDONTWRITEBYTECODE", "1")
	return filepath.Join(dir, "publish-component-release.sh"), resourceDir
}

func TestResourceReleaseResumesInitialPublication(t *testing.T) { //nolint:funlen // One fixture covers interrupted publication through promotion.
	server := httptest.NewServer(containerregistry.New())
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	canonical := host + "/canonical"
	target := host + "/qualification-current"
	revision := strings.Repeat("b", 40)
	source := host + "/fixtures/k8s"
	var expected v1.ImageIndex = empty.Index
	for _, arch := range []string{"amd64", "arm64"} {
		image := testVersionedReleaseImage(t, "k8s", "k8s", "v1.37.0", arch, revision)
		if pushErr := crane.Push(image, source+":"+arch); pushErr != nil {
			t.Fatal(pushErr)
		}
		expected = buildPackageIndex(expected, image, arch)
	}
	script, resourceDir := testResourceReleaseInputs(t, "resource-k8s")
	run := testVersionedReleaseRunner(t, script, "resource-k8s", "v1.37.0")
	workspace := t.TempDir()
	t.Setenv("KC_RELEASE_FAIL_ARM_ONCE", filepath.Join(workspace, "arm-failed"))
	t.Setenv("KC_RELEASE_BUILDER_LOG", filepath.Join(workspace, "builders"))
	t.Setenv("KC_RELEASE_PRODUCER_LOG", filepath.Join(workspace, "publications"))
	t.Setenv("KC_RELEASE_FIXTURE_SOURCE", source)
	// The fixture producer uses the production index writer. Payload acquisition
	// is covered by the existing builder tests and requires no external download.
	producer := `#!/usr/bin/env bash
set -euo pipefail
while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry) registry="$2" ;;
    --version) version="$2" ;;
    --arch) arch="$2" ;;
    --name) name="$2" ;;
    --kind|--profile|--package) ;;
    *) exit 2 ;;
  esac
  shift 2
done
printf '%s\n' "$arch" >> "$KC_RELEASE_PRODUCER_LOG"
crane publish-fixture "$KC_RELEASE_FIXTURE_SOURCE:$arch" "$registry/package-$name:$version" "$arch"
`
	producerPath := filepath.Join(filepath.Dir(script), "..", "publish-oci-package.sh")
	if writeErr := os.WriteFile(producerPath, []byte(producer), 0755); writeErr != nil { // #nosec G306 -- Executable fixture producer.
		t.Fatal(writeErr)
	}
	output, firstErr := run(canonical, target, revision)
	if firstErr == nil {
		t.Fatalf("interrupted publication unexpectedly succeeded\n%s", output)
	}
	if _, lookupErr := crane.Digest(canonical + "/package-k8s:v1.37.0"); lookupErr == nil {
		t.Fatal("incomplete candidate was promoted")
	}
	partial, partialErr := crane.Get(target + "/package-k8s:v1.37.0")
	require.NoError(t, partialErr)
	partialIndex, indexErr := partial.ImageIndex()
	require.NoError(t, indexErr)
	partialManifest, manifestErr := partialIndex.IndexManifest()
	require.NoError(t, manifestErr)
	require.Len(t, partialManifest.Manifests, 1)
	output, retryErr := run(canonical, target, revision)
	if retryErr != nil {
		t.Fatalf("publication retry failed: %v\n%s", retryErr, output)
	}
	want, digestErr := expected.Digest()
	if digestErr != nil {
		t.Fatal(digestErr)
	}
	for _, prefix := range []string{canonical, target} {
		actual, readErr := crane.Digest(prefix + "/package-k8s:v1.37.0")
		if readErr != nil || actual != want.String() {
			t.Fatalf("complete package = %s, %v; want %s", actual, readErr, want)
		}
	}
	publications, readErr := os.ReadFile(filepath.Join(workspace, "publications"))
	if readErr != nil || string(publications) != "amd64\narm64\n" {
		t.Fatalf("completed platform was republished: %s, %v", publications, readErr)
	}
	metadata, metadataErr := os.ReadFile(filepath.Join(resourceDir, "images.lock"))
	require.NoError(t, metadataErr)
	require.Contains(t, string(metadata), "\tamd64\t")
	require.Contains(t, string(metadata), "\tarm64\t")
}
