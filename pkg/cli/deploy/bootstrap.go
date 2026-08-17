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

package deploy

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/kubeclipper/kubeclipper/pkg/cli/config"
	"github.com/kubeclipper/kubeclipper/pkg/cli/logger"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryfetcher "github.com/kubeclipper/kubeclipper/pkg/delivery/fetcher"
	deliveryindexer "github.com/kubeclipper/kubeclipper/pkg/delivery/indexer"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"
)

const bootstrapKind = "bootstrap"

const (
	bootstrapPackageKubeClipper = "kubeclipper"
	bootstrapPackageEtcd        = "etcd"
)

type bootstrapAsset struct {
	PackageName    string
	Name           string
	RemotePath     string
	ConsoleArchive bool
}

var deployBootstrapAssets = []bootstrapAsset{
	{PackageName: bootstrapPackageKubeClipper, Name: "kubeclipper-server", RemotePath: "/usr/local/bin/kubeclipper-server"},
	{PackageName: bootstrapPackageKubeClipper, Name: "kubeclipper-agent", RemotePath: "/usr/local/bin/kubeclipper-agent"},
	{PackageName: bootstrapPackageEtcd, Name: "etcd", RemotePath: "/usr/local/bin/etcd"},
	{PackageName: bootstrapPackageEtcd, Name: "etcdctl", RemotePath: "/usr/local/bin/etcdctl"},
	{PackageName: bootstrapPackageEtcd, Name: "etcdutl", RemotePath: "/usr/local/bin/etcdutl"},
	{PackageName: "console", Name: "caddy", RemotePath: "/usr/local/bin/caddy"},
	{PackageName: "console", Name: "kc-console", ConsoleArchive: true},
	{PackageName: "registry", Name: "registry", RemotePath: "/usr/local/bin/registry"},
}

var joinBootstrapAssets = []bootstrapAsset{
	{PackageName: bootstrapPackageKubeClipper, Name: "kubeclipper-agent", RemotePath: "/usr/local/bin/kubeclipper-agent"},
}

type BootstrapInstallOptions struct {
	Registry       string
	Arch           string
	SSH            *sshutils.SSH
	Hosts          []string
	NeedAgent      bool
	RegistryConfig *deliveryregistry.Config
	RemoteTempDir  string
}

func RuntimeArch() string {
	return runtime.GOARCH
}

func InstallBootstrapAssetsFromRegistry(ctx context.Context, opts BootstrapInstallOptions) error {
	assets := deployBootstrapAssets
	if !opts.NeedAgent {
		assets = joinBootstrapAssets
	}
	if strings.TrimSpace(opts.Registry) == "" {
		return fmt.Errorf("--package-registry must be specified")
	}
	if opts.Arch == "" {
		opts.Arch = RuntimeArch()
	}
	if len(opts.Hosts) == 0 {
		return fmt.Errorf("bootstrap install hosts are required")
	}
	logger.Infof("refresh bootstrap assets from OCI registry %s", opts.Registry)
	indexer := deliveryindexer.NewRegistryPackageInventoryIndexerWithConfig(opts.RegistryConfig)
	inventory, err := indexer.Refresh(ctx, opts.Registry)
	if err != nil {
		return fmt.Errorf("refresh bootstrap assets from registry %s: %w", opts.Registry, err)
	}
	components, missing := resolveBootstrapAssetComponents(inventory, assets, opts.Arch)
	if len(missing) > 0 {
		return fmt.Errorf("package registry %s is missing bootstrap assets for arch %s: %s", opts.Registry, opts.Arch, strings.Join(missing, ", "))
	}
	result, err := deliveryfetcher.NewOCIArtifactFetcherWithConfig(false, opts.RegistryConfig).Fetch(ctx, &deliveryapis.ResolvedArtifactPlan{
		OS:         deliveryapis.DefaultPackageOS,
		Arch:       opts.Arch,
		Components: components,
	})
	if err != nil {
		return fmt.Errorf("fetch bootstrap assets from registry %s: %w", opts.Registry, err)
	}
	fetched := make(map[string]string, len(result.Components))
	for _, component := range result.Components {
		for contentName, path := range component.Files {
			if path != "" {
				fetched[bootstrapAssetKey(component.Name, contentName)] = path
			}
		}
	}
	for _, asset := range assets {
		localPath := fetched[bootstrapAssetKey(asset.packageName(), asset.Name)]
		if localPath == "" {
			return fmt.Errorf("fetched bootstrap asset %q has no binary payload", asset.Name)
		}
		if err := sendBootstrapAsset(opts.SSH, opts.Hosts, asset, localPath, opts.RemoteTempDir); err != nil {
			return err
		}
	}
	return nil
}

func resolveBootstrapAssetComponents(inventory *deliveryapis.PackageInventory, assets []bootstrapAsset, arch string) ([]deliveryapis.ResolvedComponent, []string) {
	assetsByPackage := make(map[string][]bootstrapAsset)
	packageNames := make([]string, 0, len(assets))
	for _, asset := range assets {
		packageName := asset.packageName()
		if _, ok := assetsByPackage[packageName]; !ok {
			packageNames = append(packageNames, packageName)
		}
		assetsByPackage[packageName] = append(assetsByPackage[packageName], asset)
	}

	components := make([]deliveryapis.ResolvedComponent, 0, len(assetsByPackage))
	missing := make([]string, 0)
	for _, packageName := range packageNames {
		packageAssets := assetsByPackage[packageName]
		pkg, ok := selectBootstrapPackage(inventory, packageName, packageAssets, arch)
		if !ok {
			for _, asset := range packageAssets {
				missing = append(missing, fmt.Sprintf("%s/%s:%s", bootstrapKind, packageName, asset.Name))
			}
			continue
		}
		components = append(components, deliveryapis.ResolvedComponent{
			Slot:      "bootstrap-" + packageName,
			Kind:      pkg.Kind,
			Name:      pkg.Name,
			Version:   pkg.Version,
			OS:        pkg.OS,
			Arch:      pkg.Arch,
			Required:  true,
			Transport: pkg.Transport,
			Contents:  pkg.Contents,
		})
	}
	return components, missing
}

func selectBootstrapPackage(
	inventory *deliveryapis.PackageInventory,
	packageName string,
	assets []bootstrapAsset,
	arch string,
) (deliveryapis.PackageEntry, bool) {
	if inventory == nil {
		return deliveryapis.PackageEntry{}, false
	}
	var candidates []deliveryapis.PackageEntry
	for _, pkg := range inventory.Spec.Packages {
		if pkg.Kind != bootstrapKind || pkg.Name != packageName || pkg.Arch != arch {
			continue
		}
		if !hasBootstrapAssetContents(pkg.Contents, assets) {
			continue
		}
		candidates = append(candidates, pkg)
	}
	if len(candidates) == 0 {
		return deliveryapis.PackageEntry{}, false
	}
	sort.Slice(candidates, func(i, j int) bool {
		if cmp, ok := deliveryapis.CompareVersions(candidates[i].Version, candidates[j].Version); ok && cmp != 0 {
			return cmp > 0
		}
		return candidates[i].Version > candidates[j].Version
	})
	return candidates[0], true
}

func hasBootstrapAssetContents(contents []deliveryapis.ArtifactContent, assets []bootstrapAsset) bool {
	contentSet := make(map[string]struct{}, len(contents))
	for _, content := range contents {
		contentSet[content.Name] = struct{}{}
	}
	for _, asset := range assets {
		if _, ok := contentSet[asset.Name]; !ok {
			return false
		}
	}
	return true
}

func sendBootstrapAsset(sshConfig *sshutils.SSH, hosts []string, asset bootstrapAsset, localPath, remoteTempDir string) error {
	if strings.TrimSpace(remoteTempDir) == "" {
		remoteTempDir = config.DefaultPkgPath
	}
	remoteFile := filepath.Join(remoteTempDir, filepath.Base(localPath))
	var hook string
	if asset.ConsoleArchive {
		hook = fmt.Sprintf("rm -rf %s && mkdir -p %s && tar -xf %s -C %s && test -d %s",
			filepath.Join(remoteTempDir, "kc", "kc-console"),
			filepath.Join(remoteTempDir, "kc"),
			remoteFile,
			filepath.Join(remoteTempDir, "kc"),
			filepath.Join(remoteTempDir, "kc", "kc-console"),
		)
	} else {
		hook = fmt.Sprintf("mkdir -p /usr/lib/systemd/system && install -m 0755 %s %s", remoteFile, asset.RemotePath)
	}
	if err := utils.SendPackageV2WithTempDir(sshConfig, localPath, hosts, remoteTempDir, nil, &hook, remoteTempDir); err != nil {
		return fmt.Errorf("send bootstrap asset %s: %w", asset.Name, err)
	}
	return nil
}

func (a bootstrapAsset) packageName() string {
	if strings.TrimSpace(a.PackageName) != "" {
		return a.PackageName
	}
	return a.Name
}

func bootstrapAssetKey(packageName, contentName string) string {
	return packageName + "/" + contentName
}
