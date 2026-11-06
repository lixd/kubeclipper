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

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
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
	if conflictErr == nil || !strings.Contains(string(output), "etcd package tag conflict") {
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
	if deniedErr == nil || !strings.Contains(string(deniedOutput), "cannot inspect etcd package") {
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
	dir := t.TempDir()
	payload := filepath.Join(dir, "etcd")
	if err := os.WriteFile(payload, []byte("etcd fixture "+arch), 0600); err != nil {
		t.Fatal(err)
	}
	req := PublishRequest{
		Kind: "bootstrap", Name: "etcd", Version: "3.5.21", Arch: arch,
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
	script, err := filepath.Abs("../../../scripts/open-packaging/publish-bootstrap-etcd-release.sh")
	if err != nil {
		t.Fatal(err)
	}
	return func(canonical, target, revision string) ([]byte, error) {
		cmd := exec.CommandContext(t.Context(), "bash", script, canonical, target, "3.5.21")
		cmd.Env = append(os.Environ(), "KC_SOURCE_REVISION="+revision)
		return cmd.CombinedOutput()
	}
}
