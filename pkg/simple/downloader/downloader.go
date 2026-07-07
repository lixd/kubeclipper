package downloader

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.uber.org/zap"

	"github.com/kubeclipper/kubeclipper/pkg/logger"
)

const (
	ManifestFilename = "manifest.json"
	ImageFilename    = "images.tar.gz"
	ConfigFilename   = "configs.tar.gz"
	BaseDstDir       = "/tmp/kc-downloader"
	ChartFilename    = "charts.tgz"
	ContentsDirname  = "contents"
)

type ManifestElement struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
	Path   string `json:"path"`
}

func PackageDir(kind, name, version, arch string) string {
	return filepath.Join(BaseDstDir, "packages", kind, name, version, arch)
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

func ImagePath(kind, name, version, arch string) string {
	return filepath.Join(PackageContentsDir(kind, name, version, arch), ImageFilename)
}

func ChartPath(kind, name, version, arch string) string {
	return filepath.Join(PackageContentsDir(kind, name, version, arch), ChartFilename)
}

func ChartDir(kind, name, version, arch string) string {
	return ChartPath(kind, name, version, arch)
}

func CleanupConfigs(kind, name, version, arch string, dryRun bool) error {
	return cleanupConfigsAt(ConfigPath(kind, name, version, arch), dryRun)
}

func CleanupImages(kind, name, version, arch string, dryRun bool) error {
	return cleanupPath(ImagePath(kind, name, version, arch), dryRun)
}

func CleanupCharts(kind, name, version, arch string, dryRun bool) error {
	return cleanupPath(ChartPath(kind, name, version, arch), dryRun)
}

func CleanupPackage(kind, name, version, arch string, dryRun bool) error {
	if err := CleanupConfigs(kind, name, version, arch, dryRun); err != nil {
		return err
	}
	if err := CleanupImages(kind, name, version, arch, dryRun); err != nil {
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
