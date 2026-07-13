package downloader

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

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
