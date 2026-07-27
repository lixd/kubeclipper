package downloader

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"

	"github.com/kubeclipper/kubeclipper/pkg/logger"
)

const (
	ManifestFilename = "manifest.json"
	ConfigFilename   = "configs.tar.gz"
	ChartFilename    = "charts.tgz"
	ContentsDirname  = "contents"
	CacheDirEnv      = "KUBECLIPPER_CACHE_DIR"
	RootCacheDir     = "/var/lib/kubeclipper/cache"
	cacheDirMode     = 0700
	cacheFileMode    = 0600
	cacheLockName    = ".cache.lock"
)

type PackageLock struct {
	file *os.File
}

type ManifestElement struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
	Path   string `json:"path"`
}

func PackageDir(kind, name, version, arch string) string {
	return filepath.Join(CacheDir(), "packages", kind, name, version, arch)
}

func ValidatePackagePath(kind, name, version, arch string) error {
	for label, value := range map[string]string{
		"kind": kind, "name": name, "version": version, "architecture": arch,
	} {
		if value == "" || value == "." || value == ".." || filepath.IsAbs(value) ||
			filepath.Base(value) != value || strings.ContainsAny(value, `/\\`) {
			return fmt.Errorf("package %s %q must be a relative path segment", label, value)
		}
	}
	return nil
}

// CacheDir returns a private per-user cache for kcctl and a root-only cache for
// the agent. Tests and release tools may override it with CacheDirEnv.
func CacheDir() string {
	if configured := os.Getenv(CacheDirEnv); configured != "" {
		return filepath.Clean(configured)
	}
	if os.Geteuid() == 0 {
		return RootCacheDir
	}
	if dir, err := os.UserCacheDir(); err == nil {
		return filepath.Join(dir, "kubeclipper")
	}
	return RootCacheDir
}

// EnsureCacheDir creates the cache root with private permissions and rejects a
// pre-created symlink before any privileged process writes below it.
func EnsureCacheDir() error {
	dir := CacheDir()
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("kubeclipper cache directory %q must be absolute", dir)
	}
	info, err := os.Lstat(dir)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(dir, cacheDirMode); err != nil {
			return fmt.Errorf("create kubeclipper cache directory: %w", err)
		}
		info, err = os.Lstat(dir)
	}
	if err != nil {
		return fmt.Errorf("inspect kubeclipper cache directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("kubeclipper cache directory %q must not be a symlink", dir)
	}
	if !info.IsDir() {
		return fmt.Errorf("kubeclipper cache path %q is not a directory", dir)
	}
	if err = os.Chmod(dir, cacheDirMode); err != nil {
		return fmt.Errorf("protect kubeclipper cache directory: %w", err)
	}
	return nil
}

// AcquirePackageLock serializes cache updates for one artifact across agent or
// kcctl processes. The private cache root prevents untrusted lock replacement.
func AcquirePackageLock(kind, name, version, arch string) (*PackageLock, error) {
	if err := ValidatePackagePath(kind, name, version, arch); err != nil {
		return nil, err
	}
	if err := EnsureCacheDir(); err != nil {
		return nil, err
	}
	dir := PackageDir(kind, name, version, arch)
	if err := os.MkdirAll(dir, cacheDirMode); err != nil {
		return nil, fmt.Errorf("create package cache directory: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(dir, cacheLockName), os.O_CREATE|os.O_RDWR, cacheFileMode)
	if err != nil {
		return nil, fmt.Errorf("open package cache lock: %w", err)
	}
	if err = unix.Flock(int(file.Fd()), unix.LOCK_EX); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock package cache: %w", err)
	}
	return &PackageLock{file: file}, nil
}

func (l *PackageLock) Unlock() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	if err != nil {
		return fmt.Errorf("unlock package cache: %w", err)
	}
	return closeErr
}

// AtomicWriteFile replaces path only after the complete payload has been
// written and synced. Rename replaces a malicious destination symlink rather
// than following it.
func AtomicWriteFile(path string, data []byte, mode os.FileMode) error {
	return AtomicWrite(path, mode, func(writer io.Writer) error {
		_, err := writer.Write(data)
		return err
	})
}

func AtomicWrite(path string, mode os.FileMode, write func(io.Writer) error) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, cacheDirMode); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".kc-cache-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err = tmp.Chmod(mode); err == nil {
		err = write(tmp)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr = directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func PackageContentsDir(kind, name, version, arch string) string {
	return filepath.Join(PackageDir(kind, name, version, arch), ContentsDirname)
}

func PackageManifestPath(kind, name, version, arch string) string {
	return filepath.Join(PackageDir(kind, name, version, arch), ManifestFilename)
}

func ConfigPath(kind, name, version, arch string) string {
	return filepath.Join(PackageContentsDir(kind, name, version, arch), ConfigFilename)
}

func ChartPath(kind, name, version, arch string) string {
	return filepath.Join(PackageContentsDir(kind, name, version, arch), ChartFilename)
}

func ChartDir(kind, name, version, arch string) string {
	return ChartPath(kind, name, version, arch)
}

func CleanupConfigs(kind, name, version, arch string, dryRun bool) error {
	if err := ValidatePackagePath(kind, name, version, arch); err != nil {
		return err
	}
	return cleanupConfigsAt(ConfigPath(kind, name, version, arch), dryRun)
}

func CleanupCharts(kind, name, version, arch string, dryRun bool) error {
	if err := ValidatePackagePath(kind, name, version, arch); err != nil {
		return err
	}
	chartPath := ChartPath(kind, name, version, arch)
	if err := cleanupPath(chartPath, dryRun); err != nil {
		return err
	}
	return cleanupPath(chartPath+".source.json", dryRun)
}

func CleanupPackage(kind, name, version, arch string, dryRun bool) error {
	if err := ValidatePackagePath(kind, name, version, arch); err != nil {
		return err
	}
	if err := CleanupConfigs(kind, name, version, arch, dryRun); err != nil {
		return err
	}
	if err := CleanupCharts(kind, name, version, arch, dryRun); err != nil {
		return err
	}
	return cleanupPath(PackageDir(kind, name, version, arch), dryRun)
}

func cleanupConfigsAt(archivePath string, dryRun bool) error {
	if dryRun {
		logger.Debug("dry run cleanup configs", zap.String("archivePath", archivePath))
		return nil
	}
	manifest, err := ReadManifestElementsFromArchive(archivePath)
	if err != nil {
		return err
	}
	for _, f := range manifest {
		path := filepath.Join(f.Path, f.Name)
		if path == "" || path == "/" {
			continue
		}
		if err = os.RemoveAll(path); err == nil {
			logger.Debugf("remove %s successfully", path)
		}
	}
	return cleanupPath(archivePath, false)
}

func ReadManifestElementsFromArchive(archivePath string) (manifest []ManifestElement, err error) {
	file, err := os.Open(archivePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	gzr, err := gzip.NewReader(file)
	if err != nil {
		return nil, err
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			return nil, fmt.Errorf("manifest %q not found in archive %s", ManifestFilename, archivePath)
		}
		if err != nil {
			return nil, err
		}
		if header.Typeflag != tar.TypeReg || filepath.Base(header.Name) != ManifestFilename {
			continue
		}
		var manifest []ManifestElement
		if err = json.NewDecoder(tr).Decode(&manifest); err != nil {
			return nil, err
		}
		return manifest, nil
	}
}

func cleanupPath(path string, dryRun bool) error {
	if dryRun {
		logger.Debug("dry run cleanup path", zap.String("path", path))
		return nil
	}
	return os.RemoveAll(path)
}
