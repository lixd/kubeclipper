/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fetcher

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	digest "github.com/opencontainers/go-digest"

	apis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
)

func selectionImage(t *testing.T) (v1.Image, []apis.ArtifactContent) {
	t.Helper()
	files := map[string][]byte{"server": []byte("server-bytes"), "agent": []byte("agent-bytes"), "kcctl": []byte("cli-bytes")}
	var contents []apis.ArtifactContent
	pkg := apis.PackageManifest{SchemaVersion: 1, Kind: "bootstrap", Name: "kubeclipper", Version: "vtest", Platform: apis.PackageManifestPlatform{OS: "linux", Arch: "amd64"}}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, name := range []string{"server", "agent", "kcctl"} {
		data := files[name]
		x := apis.ArtifactContent{Name: name, File: name, Digest: digest.FromBytes(data).String()}
		contents = append(contents, x)
		pkg.Contents = append(pkg.Contents, apis.PackageManifestFile{Name: name, File: name, Digest: x.Digest})
		if err := tw.WriteHeader(&tar.Header{Name: "opt/kubeclipper/resource/" + name, Mode: 0644, Size: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	data, err := json.Marshal(pkg)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately put the manifest last: extracted files must not be published early.
	if err = tw.WriteHeader(&tar.Header{Name: packageManifestPath(), Mode: 0644, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	_, _ = tw.Write(data)
	if err = tw.Close(); err != nil {
		t.Fatal(err)
	}
	layer, err := tarball.LayerFromReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	img, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		t.Fatal(err)
	}
	return img, contents
}

func TestRegistrySingleLayerSubsetsAndLocalRebuild(t *testing.T) {
	t.Setenv(downloader.CacheDirEnv, t.TempDir())
	img, contents := selectionImage(t)
	layers, err := img.Layers()
	if err != nil {
		t.Fatal(err)
	}
	ld, err := layers[0].Digest()
	if err != nil {
		t.Fatal(err)
	}
	var gets atomic.Int32
	handler := registry.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" && strings.HasSuffix(r.URL.Path, "/blobs/"+ld.String()) {
			gets.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	host := strings.TrimPrefix(server.URL, "http://")
	ref := host + "/packages/bootstrap/kubeclipper:vtest"
	if err = crane.Push(img, ref, crane.Insecure); err != nil {
		t.Fatal(err)
	}
	gets.Store(0)
	cfg := &deliveryregistry.Config{Registry: host, Scheme: "http"}
	fetch := NewOCIArtifactFetcherWithConfig(false, cfg)
	md, err := img.Digest()
	if err != nil {
		t.Fatal(err)
	}
	plan := &apis.ResolvedArtifactPlan{OS: "linux", Arch: "amd64", Components: []apis.ResolvedComponent{{Kind: "bootstrap", Name: "kubeclipper", Version: "vtest", Transport: apis.TransportRef{Type: apis.TransportOCI, Ref: ref, Digest: md.String()}, Contents: contents[:2]}}}
	result, err := fetch.Fetch(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	first := result.Components[0].BaseDir
	if gets.Load() != 1 {
		t.Fatalf("layer GET count=%d", gets.Load())
	}
	if len(result.Components[0].Files) != 2 {
		t.Fatal("unselected assets returned")
	}
	if _, err = os.Stat(filepath.Join(first, "contents", "kcctl")); !os.IsNotExist(err) {
		t.Fatal("unselected kcctl extracted")
	}
	// Protection must remain live after Fetch returns.
	if lock, e := downloader.LockCache(context.Background(), "lifecycle", true, true); e == nil {
		_ = lock.Release()
		t.Fatal("returned result has no protection")
	}
	if err = result.Release(); err != nil {
		t.Fatal(err)
	}
	if err = result.Release(); err != nil {
		t.Fatal(err)
	}
	plan.Components[0].Contents = []apis.ArtifactContent{contents[1], contents[0]}
	result, err = fetch.Fetch(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.Components[0].BaseDir != first {
		t.Fatal("selection order changed identity")
	}
	_ = result.Release()
	// Shut down the Registry; both subset extraction and rebuilding must now be local.
	server.Close()
	fetch.PullImage = func(string) (v1.Image, error) { return nil, fmt.Errorf("network forbidden") }
	plan.Components[0].Contents = []apis.ArtifactContent{contents[1]}
	result, err = fetch.Fetch(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.Components[0].BaseDir == first {
		t.Fatal("different subsets share snapshot")
	}
	_ = result.Release()
	if err = os.RemoveAll(first); err != nil {
		t.Fatal(err)
	}
	plan.Components[0].Contents = contents[:2]
	result, err = fetch.Fetch(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	if result.Components[0].BaseDir != first || gets.Load() != 1 {
		t.Fatal("local rebuild lost identity or refetched")
	}
}

func TestSnapshotFailureReleasesAllProtection(t *testing.T) {
	t.Setenv(downloader.CacheDirEnv, t.TempDir())
	img, contents := selectionImage(t)
	md, _ := img.Digest()
	good := apis.ResolvedComponent{Kind: "bootstrap", Name: "kubeclipper", Version: "vtest", Transport: apis.TransportRef{Type: apis.TransportOCI, Ref: "example.com/pkg:vtest", Digest: md.String()}, Contents: contents[:1]}
	bad := good
	bad.Name = "wrong-identity"
	f := NewOCIArtifactFetcher(false)
	f.PullImage = func(string) (v1.Image, error) { return img, nil }
	if _, err := f.Fetch(context.Background(), &apis.ResolvedArtifactPlan{OS: "linux", Arch: "amd64", Components: []apis.ResolvedComponent{good, bad}}); err == nil {
		t.Fatal("invalid component accepted")
	}
	lock, err := downloader.LockCache(context.Background(), "lifecycle", true, true)
	if err != nil {
		t.Fatal("failed Fetch leaked protection", err)
	}
	_ = lock.Release()
}

func TestExtractionRejectsInvalidRootFS(t *testing.T) {
	for _, mode := range []string{"absolute", "traversal", "symlink", "hardlink", "duplicate-target", "duplicate-manifest", "missing", "wrong-digest", "oversize", "duplicate-content"} {
		t.Run(mode, func(t *testing.T) {
			data := []byte("agent")
			x := apis.ArtifactContent{Name: "agent", File: "agent", Digest: digest.FromBytes(data).String()}
			pkg := apis.PackageManifest{SchemaVersion: 1, Kind: "bootstrap", Name: "kubeclipper", Version: "vtest", Platform: apis.PackageManifestPlatform{OS: "linux", Arch: "amd64"}, Contents: []apis.PackageManifestFile{{Name: x.Name, File: x.File, Digest: x.Digest}}}
			if mode == "wrong-digest" {
				pkg.Contents[0].Digest = "sha256:" + strings.Repeat("a", 64)
			}
			if mode == "duplicate-content" {
				pkg.Contents = append(pkg.Contents, pkg.Contents[0])
			}
			metadata, _ := json.Marshal(pkg)
			var buf bytes.Buffer
			tw := tar.NewWriter(&buf)
			write := func(name string, typ byte, payload []byte, size int64) {
				h := &tar.Header{Name: name, Typeflag: typ, Mode: 0600, Size: size}
				if typ == tar.TypeSymlink || typ == tar.TypeLink {
					h.Linkname = "/etc/passwd"
					h.Size = 0
				}
				if err := tw.WriteHeader(h); err != nil {
					t.Fatal(err)
				}
				if len(payload) > 0 {
					if _, err := tw.Write(payload); err != nil {
						t.Fatal(err)
					}
				}
			}
			write(packageManifestPath(), tar.TypeReg, metadata, int64(len(metadata)))
			if mode == "duplicate-manifest" {
				write(packageManifestPath(), tar.TypeReg, metadata, int64(len(metadata)))
			}
			name := "opt/kubeclipper/resource/agent"
			typ := byte(tar.TypeReg)
			size := int64(len(data))
			switch mode {
			case "absolute":
				name = "/absolute"
			case "traversal":
				name = "opt/../agent"
			case "symlink":
				typ = tar.TypeSymlink
				data = nil
			case "hardlink":
				typ = tar.TypeLink
				data = nil
			case "oversize":
				size = maxPackageFile + 1
				data = nil
			}
			if mode != "missing" {
				write(name, typ, data, size)
			}
			if mode == "duplicate-target" {
				write(name, typ, data, size)
			}
			_ = tw.Close()
			dir := t.TempDir()
			blob := filepath.Join(dir, "blob.tar")
			if err := os.WriteFile(blob, buf.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
			tmp := filepath.Join(dir, "tmp")
			if err := os.MkdirAll(filepath.Join(tmp, "contents"), 0700); err != nil {
				t.Fatal(err)
			}
			c := apis.ResolvedComponent{Kind: "bootstrap", Name: "kubeclipper", Version: "vtest", Contents: []apis.ArtifactContent{x}}
			if _, err := extractSnapshot(context.Background(), blob, tmp, c, "linux", "amd64", c.Contents); err == nil {
				t.Fatal("invalid rootfs accepted")
			}
			if _, err := os.Stat(filepath.Join(tmp, "receipt.json")); !os.IsNotExist(err) {
				t.Fatal("invalid extraction marked complete")
			}
		})
	}
}

func TestCleanupWaitsUntilSnapshotConsumerReleases(t *testing.T) {
	t.Setenv(downloader.CacheDirEnv, t.TempDir())
	img, contents := selectionImage(t)
	md, _ := img.Digest()
	f := NewOCIArtifactFetcher(false)
	f.PullImage = func(string) (v1.Image, error) { return img, nil }
	plan := &apis.ResolvedArtifactPlan{OS: "linux", Arch: "amd64", Components: []apis.ResolvedComponent{{Kind: "bootstrap", Name: "kubeclipper", Version: "vtest", Transport: apis.TransportRef{Type: apis.TransportOCI, Ref: "example.com/pkg:vtest", Digest: md.String()}, Contents: contents[:1]}}}
	result, err := f.Fetch(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	done := make(chan error, 1)
	go func() { done <- downloader.CleanupPackage("bootstrap", "kubeclipper", "vtest", "linux-amd64", false) }()
	select {
	case e := <-done:
		t.Fatal("cleanup ran before release", e)
	case <-time.After(50 * time.Millisecond):
	}
	p := result.Components[0].Files["server"]
	if _, err = os.ReadFile(p); err != nil {
		t.Fatal("consumer path lost", err)
	}
	if err = result.Release(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup still blocked after release")
	}
	if _, err = os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("snapshot not removed", err)
	}
}

func TestConcurrentSnapshotFetchSharesLayer(t *testing.T) {
	t.Setenv(downloader.CacheDirEnv, t.TempDir())
	img, contents := selectionImage(t)
	md, _ := img.Digest()
	plan := &apis.ResolvedArtifactPlan{OS: "linux", Arch: "amd64", Components: []apis.ResolvedComponent{{Kind: "bootstrap", Name: "kubeclipper", Version: "vtest", Transport: apis.TransportRef{Type: apis.TransportOCI, Ref: "example.com/pkg:vtest", Digest: md.String()}, Contents: contents[:2]}}}
	var pulls atomic.Int32
	f := NewOCIArtifactFetcher(false)
	f.PullImage = func(string) (v1.Image, error) { pulls.Add(1); return img, nil }
	done := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			r, err := f.Fetch(context.Background(), plan)
			if err == nil {
				err = r.Release()
			}
			done <- err
		}()
	}
	for i := 0; i < 4; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if pulls.Load() != 1 {
		t.Fatalf("concurrent pulls=%d", pulls.Load())
	}
}

// Exercise the on-disk extraction/publication boundaries in a separate process.
// The staged case uses the production extractor and receipt writer, without publishing.
func TestSnapshotProcessExitBoundaries(t *testing.T) {
	img, contents := selectionImage(t)
	md, _ := img.Digest()
	layers, _ := img.Layers()
	ld, _ := layers[0].Digest()
	c := apis.ResolvedComponent{Kind: "bootstrap", Name: "kubeclipper", Version: "vtest", Transport: apis.TransportRef{Type: apis.TransportOCI, Ref: "example.com/pkg:vtest", Digest: md.String()}, Contents: contents[:2]}
	plan := &apis.ResolvedArtifactPlan{OS: "linux", Arch: "amd64", Components: []apis.ResolvedComponent{c}}
	f := NewOCIArtifactFetcher(false)
	f.PullImage = func(string) (v1.Image, error) { return nil, fmt.Errorf("network forbidden") }
	r, selected, err := snapshotResult("linux", "amd64", c)
	if err != nil {
		t.Fatal(err)
	}
	if mode := os.Getenv("KC_SNAPSHOT_CHILD"); mode != "" {
		if mode == "published" {
			if _, err = f.Fetch(context.Background(), plan); err != nil {
				t.Fatal(err)
			}
			os.Exit(0)
		}
		tmp, err := os.MkdirTemp(filepath.Dir(r.BaseDir), ".extract-")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.MkdirAll(filepath.Join(tmp, "contents"), 0700); err != nil {
			t.Fatal(err)
		}
		store, err := openContent()
		if err != nil {
			t.Fatal(err)
		}
		pkg, err := extractSnapshot(context.Background(), store.path(digest.Digest(ld.String())), tmp, c, "linux", "amd64", selected)
		if err != nil {
			t.Fatal(err)
		}
		staged := r
		staged.ManifestPath = filepath.Join(tmp, "receipt.json")
		if err = writeFetchedManifest(staged, selected, pkg, false); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Files complete; no directory publication and no deferred cleanup.
	}
	for _, mode := range []string{"staged", "published"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(downloader.CacheDirEnv, t.TempDir())
			warm := NewOCIArtifactFetcher(false)
			warm.PullImage = func(string) (v1.Image, error) { return img, nil }
			seeded, err := warm.Fetch(context.Background(), plan)
			if err != nil {
				t.Fatal(err)
			}
			base := seeded.Components[0].BaseDir
			_ = seeded.Release()
			if err = os.RemoveAll(base); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestSnapshotProcessExitBoundaries$")
			cmd.Env = append(os.Environ(), "KC_SNAPSHOT_CHILD="+mode)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child: %v %s", err, out)
			}
			if _, err = os.Stat(filepath.Join(base, "receipt.json")); (mode == "published") != (err == nil) {
				t.Fatalf("publication state: %v", err)
			}
			result, err := f.Fetch(context.Background(), plan)
			if err != nil {
				t.Fatal(err)
			}
			if err = validateSnapshot(context.Background(), result.Components[0], selected); err != nil {
				t.Fatal(err)
			}
			_ = result.Release()
		})
	}
}

func TestSnapshotSelectionNormalizesEquivalentFiles(t *testing.T) {
	img, contents := selectionImage(t)
	md, _ := img.Digest()
	c := apis.ResolvedComponent{Kind: "bootstrap", Name: "kubeclipper", Version: "vtest", Transport: apis.TransportRef{Type: apis.TransportOCI, Ref: "example.com/pkg:vtest", Digest: md.String()}, Contents: contents[:1]}
	c.Contents = append([]apis.ArtifactContent(nil), c.Contents...)
	c.Contents[0].File = ""
	implicit, selected, err := snapshotResult("linux", "amd64", c)
	if err != nil {
		t.Fatal(err)
	}
	c.Contents[0].File = contentFile(c.Contents[0])
	explicit, canonical, err := snapshotResult("linux", "amd64", c)
	if err != nil {
		t.Fatal(err)
	}
	if implicit.BaseDir != explicit.BaseDir || selected[0].File != canonical[0].File {
		t.Fatal("equivalent selection identities differ")
	}
}
