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
	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"
)

const bootstrapKind = "binary"

type bootstrapAsset struct {
	Name           string
	RemotePath     string
	ConsoleArchive bool
}

var deployBootstrapAssets = []bootstrapAsset{
	{Name: "kcctl", RemotePath: "/usr/local/bin/kcctl"},
	{Name: "caddy", RemotePath: "/usr/local/bin/caddy"},
	{Name: "registry", RemotePath: "/usr/local/bin/registry"},
	{Name: "kubeclipper-agent", RemotePath: "/usr/local/bin/kubeclipper-agent"},
	{Name: "etcdutl", RemotePath: "/usr/local/bin/etcdutl"},
	{Name: "etcd", RemotePath: "/usr/local/bin/etcd"},
	{Name: "kubeclipper-server", RemotePath: "/usr/local/bin/kubeclipper-server"},
	{Name: "etcdctl", RemotePath: "/usr/local/bin/etcdctl"},
	{Name: "kc-console", ConsoleArchive: true},
}

var joinBootstrapAssets = []bootstrapAsset{
	{Name: "kubeclipper-agent", RemotePath: "/usr/local/bin/kubeclipper-agent"},
}

type BootstrapInstallOptions struct {
	Registry  string
	Arch      string
	SSH       *sshutils.SSH
	Hosts     []string
	NeedAgent bool
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
	inventory, err := deliveryindexer.NewRegistryPackageInventoryIndexer(nil).Refresh(ctx, opts.Registry)
	if err != nil {
		return fmt.Errorf("refresh bootstrap assets from registry %s: %w", opts.Registry, err)
	}
	components, missing := resolveBootstrapAssetComponents(inventory, assets, opts.Arch)
	if len(missing) > 0 {
		return fmt.Errorf("package registry %s is missing bootstrap assets for arch %s: %s", opts.Registry, opts.Arch, strings.Join(missing, ", "))
	}
	result, err := deliveryfetcher.NewOCIArtifactFetcher(false).Fetch(ctx, &deliveryapis.ResolvedArtifactPlan{
		OS:         deliveryapis.DefaultPackageOS,
		Arch:       opts.Arch,
		Components: components,
	})
	if err != nil {
		return fmt.Errorf("fetch bootstrap assets from registry %s: %w", opts.Registry, err)
	}
	fetched := make(map[string]string, len(result.Components))
	for _, component := range result.Components {
		if path := component.Files[deliveryapis.ContentBinary]; path != "" {
			fetched[component.Name] = path
		}
	}
	for _, asset := range assets {
		localPath := fetched[asset.Name]
		if localPath == "" {
			return fmt.Errorf("fetched bootstrap asset %q has no binary payload", asset.Name)
		}
		if err = sendBootstrapAsset(opts.SSH, opts.Hosts, asset, localPath); err != nil {
			return err
		}
	}
	return nil
}

func resolveBootstrapAssetComponents(inventory *deliveryapis.PackageInventory, assets []bootstrapAsset, arch string) ([]deliveryapis.ResolvedComponent, []string) {
	components := make([]deliveryapis.ResolvedComponent, 0, len(assets))
	missing := make([]string, 0)
	for _, asset := range assets {
		pkg, ok := selectBootstrapPackage(inventory, asset.Name, arch)
		if !ok {
			missing = append(missing, fmt.Sprintf("%s/%s", bootstrapKind, asset.Name))
			continue
		}
		components = append(components, deliveryapis.ResolvedComponent{
			Slot:      "bootstrap-" + asset.Name,
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

func selectBootstrapPackage(inventory *deliveryapis.PackageInventory, name, arch string) (deliveryapis.PackageEntry, bool) {
	if inventory == nil {
		return deliveryapis.PackageEntry{}, false
	}
	var candidates []deliveryapis.PackageEntry
	for _, pkg := range inventory.Spec.Packages {
		if pkg.Kind != bootstrapKind || pkg.Name != name || pkg.Arch != arch {
			continue
		}
		if !hasBinaryContent(pkg.Contents) {
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

func hasBinaryContent(contents []deliveryapis.ArtifactContent) bool {
	for _, content := range contents {
		if content.Name == deliveryapis.ContentBinary {
			return true
		}
	}
	return false
}

func sendBootstrapAsset(sshConfig *sshutils.SSH, hosts []string, asset bootstrapAsset, localPath string) error {
	remoteFile := filepath.Join(config.DefaultPkgPath, filepath.Base(localPath))
	var hook string
	if asset.ConsoleArchive {
		hook = fmt.Sprintf("rm -rf %s && mkdir -p %s && tar -xf %s -C %s && test -d %s",
			filepath.Join(config.DefaultPkgPath, "kc", "kc-console"),
			filepath.Join(config.DefaultPkgPath, "kc"),
			remoteFile,
			filepath.Join(config.DefaultPkgPath, "kc"),
			filepath.Join(config.DefaultPkgPath, "kc", "kc-console"),
		)
	} else {
		hook = fmt.Sprintf("mkdir -p /usr/lib/systemd/system && install -m 0755 %s %s", remoteFile, asset.RemotePath)
	}
	if err := utils.SendPackageV2(sshConfig, localPath, hosts, config.DefaultPkgPath, nil, &hook); err != nil {
		return fmt.Errorf("send bootstrap asset %s: %w", asset.Name, err)
	}
	return nil
}
