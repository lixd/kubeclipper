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

package downloader

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestCacheLockCancellationAndCleanupExclusion(t *testing.T) {
	t.Setenv(CacheDirEnv, t.TempDir())
	lock, err := LockCache(context.Background(), "lifecycle", false, false)
	if err != nil {
		t.Fatal(err)
	}
	blob := filepath.Join(CacheDir(), "content", "blobs", "sha256", "example")
	if err = os.MkdirAll(filepath.Dir(blob), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(blob, []byte("payload"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = pruneCache(context.Background(), nil, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(blob); err != nil {
		t.Fatal("deleted during consumption", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err = LockCache(ctx, "lifecycle", true, false); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("lock wait ignored cancellation", err)
	}
	if err = lock.Release(); err != nil {
		t.Fatal(err)
	}
	if err = lock.Release(); err != nil {
		t.Fatal(err)
	}
	if err = pruneCache(context.Background(), nil, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(blob); !os.IsNotExist(err) {
		t.Fatal("released blob retained", err)
	}
}

func TestCacheRetentionAndProtectedObjects(t *testing.T) {
	t.Setenv(CacheDirEnv, t.TempDir())
	root := CacheDir()
	old := time.Now().Add(-8 * 24 * time.Hour)
	for _, n := range []string{"old", "new"} {
		p := filepath.Join(root, "content", "ingest", n, "data")
		if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(n), 0600); err != nil {
			t.Fatal(err)
		}
		if n == "old" {
			if err := os.Chtimes(p, old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	protected := filepath.Join(root, "content", "ingest", "new")
	if err := pruneCache(context.Background(), []string{protected}, 100, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "content", "ingest", "old")); !os.IsNotExist(err) {
		t.Fatal("old partial retained", err)
	}
	if err := pruneCache(context.Background(), []string{protected}, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(protected); err != nil {
		t.Fatal("protected object evicted", err)
	}
}

func TestCacheLockAcrossProcesses(t *testing.T) {
	if os.Getenv("KC_LOCK_CHILD") == "1" {
		lock, err := LockCache(context.Background(), "lifecycle", true, true)
		if err == nil {
			_ = lock.Release()
			os.Exit(2)
		}
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			os.Exit(0)
		}
		os.Exit(3)
	}
	t.Setenv(CacheDirEnv, t.TempDir())
	lock, err := LockCache(context.Background(), "lifecycle", false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCacheLockAcrossProcesses$")
	cmd.Env = append(os.Environ(), "KC_LOCK_CHILD=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child process did not observe lock: %v %s", err, out)
	}
}

func TestCacheLRUUsesAccessRecordAndAccountsStaging(t *testing.T) {
	t.Setenv(CacheDirEnv, t.TempDir())
	old := filepath.Join(CacheDir(), "content", "blobs", "sha256", "old")
	recent := filepath.Join(filepath.Dir(old), "recent")
	if err := os.MkdirAll(filepath.Dir(old), 0700); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{old, recent} {
		if err := os.WriteFile(p, []byte("payload"), 0600); err != nil {
			t.Fatal(err)
		}
		past := time.Now().Add(-time.Hour)
		if err := os.Chtimes(p, past, past); err != nil {
			t.Fatal(err)
		}
	}
	if err := TouchCache(old); err != nil {
		t.Fatal(err)
	}
	if err := pruneCache(context.Background(), nil, 7, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); err != nil {
		t.Fatal("recent use ignored", err)
	}
	if _, err := os.Stat(recent); !os.IsNotExist(err) {
		t.Fatal("LRU did not evict oldest access", err)
	}
	stage := filepath.Join(CacheDir(), "packages", ".extract-test")
	if err := os.MkdirAll(stage, 0700); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"receipt.json", "asset"} {
		if err := os.WriteFile(filepath.Join(stage, n), []byte("stage"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	objects, err := cacheObjects(CacheDir())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, o := range objects {
		if o.path == stage {
			count++
			if o.size != 10 {
				t.Fatalf("staging size=%d", o.size)
			}
		}
	}
	if count != 1 {
		t.Fatalf("staging counted %d times", count)
	}
	if err = pruneCache(context.Background(), nil, 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(stage); !os.IsNotExist(err) {
		t.Fatal("young staging not reclaimed under pressure", err)
	}
}

func TestPruneMetadataOnlyAndEmptyIngest(t *testing.T) {
	t.Setenv(CacheDirEnv, t.TempDir())
	now := time.Now()
	for _, name := range []string{"metadata", "empty", "young"} {
		p := filepath.Join(CacheDir(), "content", "ingest", name)
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
		if name == "metadata" {
			if err := os.WriteFile(filepath.Join(p, "ref"), []byte("ref"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chtimes(filepath.Join(p, "ref"), now.Add(-8*24*time.Hour), now.Add(-8*24*time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		if name != "young" {
			if err := os.Chtimes(p, now.Add(-8*24*time.Hour), now.Add(-8*24*time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
	}
	objects, err := cacheObjects(CacheDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 3 {
		t.Fatalf("ingest objects=%d", len(objects))
	}
	var total int64
	for _, o := range objects {
		total += o.size
	}
	if total != 3 {
		t.Fatalf("metadata bytes=%d", total)
	}
	if err = pruneCache(context.Background(), nil, CacheBudget, now); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"metadata", "empty"} {
		if _, err = os.Stat(filepath.Join(CacheDir(), "content", "ingest", name)); !os.IsNotExist(err) {
			t.Fatalf("old ingest retained: %s %v", name, err)
		}
	}
	if _, err = os.Stat(filepath.Join(CacheDir(), "content", "ingest", "young")); err != nil {
		t.Fatal("young ingest removed", err)
	}
}

func TestPruneLegacyCompleteAndInterruptedPackages(t *testing.T) {
	t.Setenv(CacheDirEnv, t.TempDir())
	for _, version := range []string{"complete", "interrupted", "native"} {
		dir := PackageContentsDir("cni", "calico", version, "linux-amd64")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, ChartFilename), []byte("archive"), 0600); err != nil {
			t.Fatal(err)
		}
		if version == "complete" {
			if err := os.WriteFile(PackageManifestPath("cni", "calico", version, "linux-amd64"), []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if version == "native" {
			if err := os.WriteFile(filepath.Join(dir, ChartFilename+".source.json"), []byte("source"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := PruneCache(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"complete", "interrupted"} {
		if _, err := os.Stat(PackageContentsDir("cni", "calico", version, "linux-amd64")); !os.IsNotExist(err) {
			t.Fatalf("legacy %s retained: %v", version, err)
		}
	}
	if _, err := os.Stat(PackageManifestPath("cni", "calico", "complete", "linux-amd64")); !os.IsNotExist(err) {
		t.Fatal("old receipt retained")
	}
	if _, err := os.Stat(ChartPath("cni", "calico", "native", "linux-amd64")); err != nil {
		t.Fatal("native Helm cache removed", err)
	}
}
