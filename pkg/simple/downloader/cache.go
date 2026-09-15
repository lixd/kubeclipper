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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	ManifestFilename = "manifest.json"
	ConfigFilename   = "configs.tar.gz"
	BaseDstDir       = "/tmp/kc-downloader"
	ChartFilename    = "charts.tgz"
	CacheDirEnv      = "KUBECLIPPER_CACHE_DIR"
	rootCacheDir     = "/var/lib/kubeclipper/cache"
	packageContents  = "contents"
	packageLock      = ".cache.lock"
	cacheDirMode     = 0700
	cacheFileMode    = 0600
)

type PackageLock struct {
	file *os.File
}

func CacheDir() string {
	if configured := os.Getenv(CacheDirEnv); configured != "" {
		return filepath.Clean(configured)
	}
	if os.Geteuid() == 0 {
		return rootCacheDir
	}
	if dir, err := os.UserCacheDir(); err == nil {
		return filepath.Join(dir, "kubeclipper")
	}
	return rootCacheDir
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

func PackageDir(kind, name, version, arch string) string {
	return filepath.Join(CacheDir(), "packages", kind, name, version, arch)
}

func PackageContentsDir(kind, name, version, arch string) string {
	return filepath.Join(PackageDir(kind, name, version, arch), packageContents)
}

func PackageManifestPath(kind, name, version, arch string) string {
	return filepath.Join(PackageDir(kind, name, version, arch), ManifestFilename)
}

func ChartPath(kind, name, version, arch string) string {
	return filepath.Join(PackageContentsDir(kind, name, version, arch), ChartFilename)
}

func ChartDir(name, version string) string {
	return filepath.Join(BaseDstDir, "."+name, version)
}

func AcquirePackageLock(kind, name, version, arch string) (*PackageLock, error) {
	if err := ValidatePackagePath(kind, name, version, arch); err != nil {
		return nil, err
	}
	dir := PackageDir(kind, name, version, arch)
	if err := os.MkdirAll(dir, cacheDirMode); err != nil {
		return nil, fmt.Errorf("create package cache directory: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(dir, packageLock), os.O_CREATE|os.O_RDWR, cacheFileMode)
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
	return os.Rename(tmpPath, path)
}

func CleanupCharts(kind, name, version, arch string, dryRun bool) error {
	if err := ValidatePackagePath(kind, name, version, arch); err != nil {
		return err
	}
	if dryRun {
		return nil
	}
	for _, path := range []string{
		ChartPath(kind, name, version, arch),
		ChartPath(kind, name, version, arch) + ".source.json",
	} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func CleanupPackage(kind, name, version, arch string, dryRun bool) error {
	if err := ValidatePackagePath(kind, name, version, arch); err != nil {
		return err
	}
	if dryRun {
		return nil
	}
	return os.RemoveAll(PackageDir(kind, name, version, arch))
}
