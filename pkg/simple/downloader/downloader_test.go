package downloader

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEnsureCacheDirCreatesPrivateDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	t.Setenv(CacheDirEnv, dir)
	if err := EnsureCacheDir(); err != nil {
		t.Fatalf("EnsureCacheDir() error: %v", err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != cacheDirMode {
		t.Fatalf("cache mode = %#o, want %#o", got, cacheDirMode)
	}
}

func TestValidatePackagePath(t *testing.T) {
	if err := ValidatePackagePath("k8s", "k8s", "v1.36.1", "linux-amd64"); err != nil {
		t.Fatalf("ValidatePackagePath() error: %v", err)
	}
	for _, invalid := range []struct {
		kind, name, version, arch string
	}{
		{kind: "../k8s", name: "k8s", version: "v1", arch: "linux-amd64"},
		{kind: "k8s", name: "/etc", version: "v1", arch: "linux-amd64"},
		{kind: "k8s", name: "k8s", version: "../../victim", arch: "linux-amd64"},
		{kind: "k8s", name: "k8s", version: "v1", arch: `linux\\amd64`},
	} {
		if err := ValidatePackagePath(invalid.kind, invalid.name, invalid.version, invalid.arch); err == nil {
			t.Fatalf("ValidatePackagePath(%q, %q, %q, %q) expected error", invalid.kind, invalid.name, invalid.version, invalid.arch)
		}
	}
}

func TestCleanupPackageRejectsTraversal(t *testing.T) {
	base := t.TempDir()
	t.Setenv(CacheDirEnv, filepath.Join(base, "cache"))
	victim := filepath.Join(base, "victim")
	if err := os.WriteFile(victim, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := CleanupPackage("k8s", "k8s", "../../victim", "linux-amd64", false); err == nil {
		t.Fatal("CleanupPackage() accepted a traversal version")
	}
	if data, err := os.ReadFile(victim); err != nil || string(data) != "keep" {
		t.Fatalf("victim after cleanup = %q, error = %v", data, err)
	}
}

func TestEnsureCacheDirRejectsSymlink(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "cache")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	t.Setenv(CacheDirEnv, link)
	if err := EnsureCacheDir(); err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("EnsureCacheDir() error = %v, want symlink rejection", err)
	}
}

func TestAtomicWriteFileReplacesDestinationSymlink(t *testing.T) {
	dir := t.TempDir()
	victim := filepath.Join(dir, "victim")
	if err := os.WriteFile(victim, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "payload")
	if err := os.Symlink(victim, destination); err != nil {
		t.Fatal(err)
	}
	if err := AtomicWriteFile(destination, []byte("payload"), 0644); err != nil {
		t.Fatalf("AtomicWriteFile() error: %v", err)
	}
	victimData, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	if string(victimData) != "unchanged" {
		t.Fatalf("victim content = %q", victimData)
	}
	info, err := os.Lstat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() {
		t.Fatalf("destination mode = %v, want regular file", info.Mode())
	}
}

func TestAtomicWritePreservesExistingFileOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "payload")
	if err := os.WriteFile(path, []byte("complete"), 0600); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("interrupted")
	err := AtomicWrite(path, 0644, func(writer io.Writer) error {
		if _, writeErr := writer.Write([]byte("partial")); writeErr != nil {
			return writeErr
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("AtomicWrite() error = %v, want %v", err, wantErr)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "complete" {
		t.Fatalf("payload = %q, want existing complete file", data)
	}
}

func TestPackageLockSerializesSameArtifact(t *testing.T) {
	t.Setenv(CacheDirEnv, filepath.Join(t.TempDir(), "cache"))
	first, err := AcquirePackageLock("k8s", "k8s", "v1.36.1", "linux-amd64")
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan *PackageLock, 1)
	errs := make(chan error, 1)
	go func() {
		second, lockErr := AcquirePackageLock("k8s", "k8s", "v1.36.1", "linux-amd64")
		if lockErr != nil {
			errs <- lockErr
			return
		}
		acquired <- second
	}()
	select {
	case second := <-acquired:
		if unlockErr := second.Unlock(); unlockErr != nil {
			t.Errorf("unlock unexpectedly acquired package lock: %v", unlockErr)
		}
		t.Fatal("second package lock acquired before first was released")
	case err = <-errs:
		t.Fatalf("second AcquirePackageLock() error: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err = first.Unlock(); err != nil {
		t.Fatal(err)
	}
	select {
	case second := <-acquired:
		if err = second.Unlock(); err != nil {
			t.Fatal(err)
		}
	case err = <-errs:
		t.Fatalf("second AcquirePackageLock() error: %v", err)
	case <-time.After(time.Second):
		t.Fatal("second package lock did not acquire after first was released")
	}
}

func TestCleanupConfigsAtRemovesManifestFilesAndArchive(t *testing.T) {
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "etc", "component")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	configFile := filepath.Join(configDir, "component.yaml")
	if err := os.WriteFile(configFile, []byte("data"), 0644); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	archivePath := filepath.Join(tmpDir, "packages", ConfigFilename)
	if err := os.MkdirAll(filepath.Dir(archivePath), 0755); err != nil {
		t.Fatalf("MkdirAll() error: %v", err)
	}
	if err := os.WriteFile(archivePath, []byte("archive"), 0644); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	manifest := []ManifestElement{{Name: filepath.Base(configFile), Path: configDir, Digest: "ignored"}}
	if err := writeManifestArchive(archivePath, manifest); err != nil {
		t.Fatalf("writeManifestArchive() error: %v", err)
	}

	if err := cleanupConfigsAt(archivePath, false); err != nil {
		t.Fatalf("cleanupConfigsAt() error: %v", err)
	}

	if _, err := os.Stat(configFile); !os.IsNotExist(err) {
		t.Fatalf("config file still exists, err=%v", err)
	}
	if _, err := os.Stat(archivePath); !os.IsNotExist(err) {
		t.Fatalf("archive still exists, err=%v", err)
	}
}

func TestCleanupPathDryRunDoesNotRemoveFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "temporary-file")
	if err := os.WriteFile(path, []byte("image"), 0644); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	if err := cleanupPath(path, true); err != nil {
		t.Fatalf("cleanupPath() error: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("Stat() error: %v", err)
	}
}

func TestReadManifestElementsFromArchive(t *testing.T) {
	tmpDir := t.TempDir()
	archivePath := filepath.Join(tmpDir, ConfigFilename)
	want := []ManifestElement{{Name: "component.yaml", Path: "/etc/component", Digest: "sha256:test"}}
	if err := writeManifestArchive(archivePath, want); err != nil {
		t.Fatalf("writeManifestArchive() error: %v", err)
	}

	got, err := ReadManifestElementsFromArchive(archivePath)
	if err != nil {
		t.Fatalf("ReadManifestElementsFromArchive() error: %v", err)
	}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("manifest = %+v, want %+v", got, want)
	}
}

func writeManifestArchive(path string, manifest []ManifestElement) error {
	data, err := json.Marshal(manifest)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	gzw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gzw)
	if err = tw.WriteHeader(&tar.Header{
		Name: "opt/kc/manifest/component/config/manifest.json",
		Mode: 0644,
		Size: int64(len(data)),
	}); err != nil {
		return err
	}
	if _, err = tw.Write(data); err != nil {
		return err
	}
	if err = tw.Close(); err != nil {
		return err
	}
	if err = gzw.Close(); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0644)
}
