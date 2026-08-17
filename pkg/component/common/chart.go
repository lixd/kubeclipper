/*
 *
 *  * Copyright 2024 KubeClipper Authors.
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

package common

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	"github.com/kubeclipper/kubeclipper/pkg/component/utils"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryfetcher "github.com/kubeclipper/kubeclipper/pkg/delivery/fetcher"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/logger"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
	"github.com/kubeclipper/kubeclipper/pkg/utils/strutil"
)

const chartCacheFileMode = 0o644

const DefaultHelmChartRepo = "kubeclipper"

const (
	chartName  = "chart"
	AgentChart = "AgentChart"
)

func init() {
	if err := component.RegisterAgentStep(fmt.Sprintf(component.RegisterStepKeyFormat, chartName, version, AgentChart), &Chart{}); err != nil {
		panic(err)
	}
}

type Chart struct {
	Kind      string                         `json:"kind,omitempty"`
	PkgName   string                         `json:"pkgName"`
	Version   string                         `json:"version"`
	Arch      string                         `json:"arch,omitempty"`
	Offline   bool                           `json:"offline"`
	Transport deliveryapis.TransportRef      `json:"transport,omitempty"`
	Contents  []deliveryapis.ArtifactContent `json:"contents,omitempty"`
}

func (i *Chart) Install(ctx context.Context, opts component.Options) ([]byte, error) {
	if i.Transport.Type == "" {
		return nil, fmt.Errorf("install %s-%s chart requires resolved artifact transport", i.PkgName, i.Version)
	}
	if i.Transport.Type != deliveryapis.TransportOCI {
		return nil, fmt.Errorf("install %s-%s chart unsupported resolved transport %q", i.PkgName, i.Version, i.Transport.Type)
	}
	if _, err := i.downloadResolvedChart(ctx, opts); err != nil {
		return nil, fmt.Errorf("download %s-%s resolved chart packages failed: %v", i.PkgName, i.Version, err)
	}
	logger.Infof("%s-%s chart packages offline install successfully", i.PkgName, i.Version)
	return nil, nil
}

func (i *Chart) downloadResolvedChart(ctx context.Context, opts component.Options) (string, error) {
	if content, ok := chartContent(i.Contents); ok && content.Transport.Type == deliveryapis.TransportHelmOCI {
		return i.downloadHelmOCIChart(ctx, opts, content)
	}
	contents := i.Contents
	if len(contents) == 0 {
		return "", fmt.Errorf("resolved chart contents are required")
	}
	result, err := deliveryfetcher.FetchComponent(ctx, runtime.GOARCH, deliveryapis.ResolvedComponent{
		Kind:      i.ArtifactKind(),
		Name:      i.PkgName,
		Version:   i.Version,
		Arch:      i.Arch,
		Transport: i.Transport,
		Contents:  contents,
	}, opts.DryRun)
	if err != nil {
		return "", err
	}
	path := result.Files[deliveryapis.ContentCharts]
	if path == "" {
		return "", fmt.Errorf("resolved chart content is missing")
	}
	return path, nil
}

func (i *Chart) downloadHelmOCIChart(ctx context.Context, opts component.Options, content deliveryapis.ArtifactContent) (chartPath string, err error) {
	ref := strings.TrimPrefix(strings.TrimSpace(content.Transport.Ref), "oci://")
	if ref == "" {
		return "", fmt.Errorf("resolved helm chart ref is required")
	}
	if err := downloader.ValidatePackagePath(i.ArtifactKind(), i.PkgName, i.Version, i.ArtifactPlatform()); err != nil {
		return "", err
	}
	chartPath = downloader.ChartPath(i.ArtifactKind(), i.PkgName, i.Version, i.ArtifactPlatform())
	if opts.DryRun {
		return chartPath, nil
	}
	lock, err := downloader.AcquirePackageLock(i.ArtifactKind(), i.PkgName, i.Version, i.ArtifactPlatform())
	if err != nil {
		return "", err
	}
	defer func() {
		err = errors.Join(err, lock.Unlock())
	}()
	return chartPath, pullHelmOCIChartArchive(ctx, ref, i.Version, content.Transport.Digest, chartPath)
}

func pullHelmOCIChartArchive(ctx context.Context, ref, version, digest, chartPath string) error {
	if validCachedHelmChart(chartPath, ref, version, digest) {
		return nil
	}
	config, err := deliveryregistry.ResolveReference(ref)
	if err != nil {
		return err
	}
	if validationErr := config.ValidateReference(ref); validationErr != nil {
		return validationErr
	}
	opts, err := config.CraneOptions(ctx)
	if err != nil {
		return err
	}
	pullRef := ref + ":" + version
	if digest != "" {
		pullRef = ref + "@" + digest
	}
	image, err := crane.Pull(pullRef, opts...)
	if err != nil {
		return err
	}
	layers, err := image.Layers()
	if err != nil {
		return err
	}
	for _, layer := range layers {
		mediaType, err := layer.MediaType()
		if err != nil {
			return err
		}
		if mediaType != types.MediaType(deliveryapis.MediaTypeHelmChartLayer) {
			continue
		}
		reader, err := layer.Compressed()
		if err != nil {
			return err
		}
		defer reader.Close()
		if err := downloader.AtomicWrite(chartPath, chartCacheFileMode, func(writer io.Writer) error {
			_, copyErr := io.Copy(writer, reader)
			return copyErr
		}); err != nil {
			return err
		}
		return writeHelmChartSource(chartPath, ref, version, digest)
	}
	return fmt.Errorf("helm chart layer %q not found in %s:%s", deliveryapis.MediaTypeHelmChartLayer, ref, version)
}

type helmChartSource struct {
	Ref           string `json:"ref"`
	Version       string `json:"version"`
	Digest        string `json:"digest,omitempty"`
	PayloadDigest string `json:"payloadDigest"`
}

func helmChartSourcePath(chartPath string) string {
	return chartPath + ".source.json"
}

func validCachedHelmChart(chartPath, ref, version, digest string) bool {
	info, err := os.Stat(chartPath)
	if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
		return false
	}
	data, err := os.ReadFile(helmChartSourcePath(chartPath))
	if err != nil {
		return false
	}
	var source helmChartSource
	if err = json.Unmarshal(data, &source); err != nil {
		return false
	}
	payloadDigest, err := fileSHA256(chartPath)
	if err != nil {
		return false
	}
	return source == (helmChartSource{Ref: ref, Version: version, Digest: digest, PayloadDigest: payloadDigest})
}

func writeHelmChartSource(chartPath, ref, version, digest string) error {
	payloadDigest, err := fileSHA256(chartPath)
	if err != nil {
		return err
	}
	data, err := json.Marshal(helmChartSource{Ref: ref, Version: version, Digest: digest, PayloadDigest: payloadDigest})
	if err != nil {
		return err
	}
	return downloader.AtomicWriteFile(helmChartSourcePath(chartPath), data, chartCacheFileMode)
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", hash.Sum(nil)), nil
}

func chartArchives(dir string) (map[string]os.FileInfo, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	archives := make(map[string]os.FileInfo)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".tgz") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		archives[filepath.Join(dir, entry.Name())] = info
	}
	return archives, nil
}

func resolvePulledChartArchive(dstDir, ref, version, contentFile string, before map[string]os.FileInfo) (string, error) {
	candidates := []string{filepath.Join(dstDir, filepath.Base(ref)+"-"+version+".tgz")}
	if contentFile != "" {
		candidates = append(candidates, filepath.Join(dstDir, contentFile))
	}
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
	}

	after, err := chartArchives(dstDir)
	if err != nil {
		return "", err
	}
	var created []string
	for path := range after {
		if _, ok := before[path]; !ok {
			created = append(created, path)
		}
	}
	if len(created) == 1 {
		return created[0], nil
	}
	sort.Strings(created)
	if len(created) > 1 {
		return "", fmt.Errorf("helm pull produced multiple chart archives: %s", strings.Join(created, ", "))
	}
	return "", fmt.Errorf("helm pull did not produce a chart archive in %s", dstDir)
}

func chartContent(contents []deliveryapis.ArtifactContent) (deliveryapis.ArtifactContent, bool) {
	for _, content := range contents {
		if content.Name == deliveryapis.ContentCharts {
			return content, true
		}
	}
	return deliveryapis.ArtifactContent{}, false
}

func (i *Chart) Uninstall(ctx context.Context, opts component.Options) ([]byte, error) {
	if err := downloader.CleanupCharts(i.ArtifactKind(), i.PkgName, i.Version, i.ArtifactPlatform(), opts.DryRun); err != nil {
		logger.Errorf("remove %s-%s chart file failed", i.PkgName, i.Version, zap.Error(err))
	}

	return nil, nil
}

func (i *Chart) ArtifactKind() string {
	if i.Kind != "" {
		return i.Kind
	}
	return chartName
}

func (i *Chart) ArtifactPlatform() string {
	if i.Kind != "" {
		return deliveryapis.DefaultPackageOS + "-" + archOrRuntime(i.Arch)
	}
	return archOrRuntime(i.Arch)
}

func archOrRuntime(arch string) string {
	if arch != "" {
		return arch
	}
	return runtime.GOARCH
}

func (i *Chart) NewInstance() component.ObjectMeta {
	return &Chart{}
}

func (i *Chart) InstallStepsV2(nodes []v1.StepNode) ([]v1.Step, error) {
	customCommand, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	return []v1.Step{
		{
			ID:         strutil.GetUUID(),
			Name:       fmt.Sprintf("%s-chartLoad", i.PkgName),
			Timeout:    metav1.Duration{Duration: 3 * time.Minute},
			ErrIgnore:  false,
			RetryTimes: 1,
			Nodes:      nodes,
			Action:     v1.ActionInstall,
			Commands: []v1.Command{
				{
					Type:          v1.CommandCustom,
					Identity:      fmt.Sprintf(component.RegisterStepKeyFormat, chartName, version, AgentChart),
					CustomCommand: customCommand,
				},
			},
		},
	}, nil
}

func (i *Chart) InstallSteps(nodeList component.NodeList) ([]v1.Step, error) {
	customCommand, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	return []v1.Step{
		{
			ID:         strutil.GetUUID(),
			Name:       fmt.Sprintf("%s-chartLoad", i.PkgName),
			Timeout:    metav1.Duration{Duration: 3 * time.Minute},
			ErrIgnore:  false,
			RetryTimes: 1,
			Nodes:      utils.UnwrapNodeList(nodeList),
			Action:     v1.ActionInstall,
			Commands: []v1.Command{
				{
					Type:          v1.CommandCustom,
					Identity:      fmt.Sprintf(component.RegisterStepKeyFormat, chartName, version, AgentChart),
					CustomCommand: customCommand,
				},
			},
		},
	}, nil
}

func GetAddHelmRepoStep(nodes []v1.StepNode, repo string) v1.Step {
	return v1.Step{
		ID:         strutil.GetUUID(),
		Name:       "addHelmRepo",
		Timeout:    metav1.Duration{Duration: 10 * time.Second},
		ErrIgnore:  false,
		RetryTimes: 0,
		Nodes:      nodes,
		Action:     v1.ActionInstall,
		Commands: []v1.Command{
			{
				Type:         v1.CommandShell,
				ShellCommand: []string{"bin/sh", "-c", fmt.Sprintf("helm repo remove %s || true", DefaultHelmChartRepo)}, // forward action, ignore errors
			},
			{
				Type:         v1.CommandShell,
				ShellCommand: []string{"/bin/sh", "-c", fmt.Sprintf("helm repo add %s %s", DefaultHelmChartRepo, repo)},
			},
			{
				Type:         v1.CommandShell,
				ShellCommand: []string{"bin/sh", "-c", fmt.Sprintf("helm repo update %s", DefaultHelmChartRepo)},
			},
		},
	}
}
