/*
 *
 *  * Copyright 2021 KubeClipper Authors.
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

package resource

import (
	"context"
	"fmt"
	"strings"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryindexer "github.com/kubeclipper/kubeclipper/pkg/delivery/indexer"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"

	"github.com/kubeclipper/kubeclipper/pkg/scheme"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/logger"
	"github.com/kubeclipper/kubeclipper/pkg/cli/printer"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

const (
	longDescription = `
  OCI resource operation.

  Currently, you can list and inspect offline resource packs discovered from OCI Registry.
  Uploading and deleting packages are native Registry operations; KubeClipper derives inventory by scanning kubeclipper/packages/.`
	resourceExample = `
  # List offline resource packs
  kcctl resource list --registry registry.local:5000

  # Inspect a specific offline resource package
  kcctl resource inspect --name containerd --version 2.1.0 --registry registry.local:5000

  # Force refresh registry-derived inventory
  kcctl resource refresh --registry registry.local:5000

  Please read 'kcctl resource -h' get more resource flags.`
	listLongDescription = `
  List offline resource packs

  You can list OCI-published offline resource packs.`
	resourceListExample = `
  # List offline resource
  kcctl resource list --registry registry.local:5000

  # List offline resource use specified output format
  kcctl resource list --registry registry.local:5000 --output 'YAML|TABLE|JSON'

  Please read 'kcctl resource list -h' get more resource list flags`
	inspectLongDescription = `
  Inspect offline resource packs

  You can inspect OCI-published offline resource packs directly from registry-derived inventory.`
	resourceInspectExample = `
  # Inspect a specific offline resource package
  kcctl resource inspect --name containerd --version 2.1.0 --registry registry.local:5000

  # Inspect a specific arch
  kcctl resource inspect --name k8s --version v1.36.0 --arch amd64 --registry registry.local:5000 -o yaml
`
	refreshLongDescription = `
  Refresh offline resource inventory

  You can force-refresh registry-derived OCI offline resource inventory.`
	resourceRefreshExample = `
  # Force refresh registry-derived inventory
  kcctl resource refresh --registry registry.local:5000
`
)

type ResourceOptions struct {
	options.IOStreams
	PrintFlags   *printer.PrintFlags
	deployConfig *options.DeployConfig
	cliOpts      *options.CliOptions
	indexer      RegistryPackageInventoryIndexer

	List    string
	Inspect string

	Type    string
	Name    string
	Version string
	Arch    string

	Registry string

	Refresh bool

	registryFiles deliveryregistry.FileOptions
}

type RegistryPackageInventoryIndexer interface {
	Index(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error)
}

type RefreshingRegistryPackageInventoryIndexer interface {
	RegistryPackageInventoryIndexer
	Refresh(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error)
}

func NewResourceOptions(streams options.IOStreams) *ResourceOptions {
	return &ResourceOptions{
		cliOpts:      options.NewCliOptions(),
		IOStreams:    streams,
		PrintFlags:   printer.NewPrintFlags(),
		deployConfig: options.NewDeployOptions(),
		Arch:         "",
	}
}

func NewCmdResource(streams options.IOStreams) *cobra.Command {
	o := NewResourceOptions(streams)
	cmd := &cobra.Command{
		Use:                   "resource",
		DisableFlagsInUseLine: true,
		Short:                 "OCI offline resource inventory",
		Long:                  longDescription,
		Example:               resourceExample,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete())
			utils.CheckErr(o.ValidateArgs(cmd))
			utils.CheckErr(o.ResourcePkgRules())
		},
	}

	cmd.AddCommand(NewCmdResourceList(o))
	cmd.AddCommand(NewCmdResourceInspect(o))
	cmd.AddCommand(NewCmdResourceRefresh(o))

	return cmd
}

func NewCmdResourceList(o *ResourceOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "list  [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "offline resource list",
		Long:                  listLongDescription,
		Example:               resourceListExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.CompleteList())
			utils.CheckErr(o.ValidateArgsList(cmd))
			utils.CheckErr(o.ResourceList())
		},
	}

	o.PrintFlags.AddFlags(cmd)
	o.cliOpts.AddFlags(cmd.Flags())
	cmd.Flags().StringVar(&o.Type, "type", o.Type, "offline resource type.")
	cmd.Flags().StringVar(&o.Name, "name", o.Name, "offline resource name.")
	cmd.Flags().StringVar(&o.Version, "version", o.Version, "offline resource version.")
	cmd.Flags().StringVar(&o.Arch, "arch", o.Arch, "offline resource arch.")
	cmd.Flags().StringVar(&o.Registry, "registry", o.Registry, "OCI registry host:port for offline packages")
	cmd.Flags().BoolVar(&o.Refresh, "refresh", o.Refresh, "refresh registry-derived offline package inventory")
	addRegistryFlags(cmd, &o.registryFiles)

	utils.CheckErr(cmd.RegisterFlagCompletionFunc("type", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return o.listType(toComplete), cobra.ShellCompDirectiveNoFileComp
	}))
	utils.CheckErr(cmd.RegisterFlagCompletionFunc("name", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return o.listName(toComplete), cobra.ShellCompDirectiveNoFileComp
	}))
	utils.CheckErr(cmd.RegisterFlagCompletionFunc("version", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return o.listVersion(toComplete), cobra.ShellCompDirectiveNoFileComp
	}))
	utils.CheckErr(cmd.RegisterFlagCompletionFunc("arch", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return o.listArch(toComplete), cobra.ShellCompDirectiveNoFileComp
	}))

	return cmd
}

func NewCmdResourceInspect(o *ResourceOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "inspect (--name <pkg-name>) (--version <pkg-version>) [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "offline resource inspect",
		Long:                  inspectLongDescription,
		Example:               resourceInspectExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete())
			utils.CheckErr(o.ValidateArgsInspect(cmd))
			utils.CheckErr(o.ResourceInspect())
		},
	}

	o.cliOpts.AddFlags(cmd.Flags())
	o.PrintFlags.AddFlags(cmd)
	cmd.Flags().StringVar(&o.Type, "type", o.Type, "offline resource type.")
	cmd.Flags().StringVar(&o.Name, "name", o.Name, "offline resource name.")
	cmd.Flags().StringVar(&o.Version, "version", o.Version, "offline resource version.")
	cmd.Flags().StringVar(&o.Arch, "arch", o.Arch, "offline resource arch.")
	cmd.Flags().StringVar(&o.Registry, "registry", o.Registry, "OCI registry host:port for offline packages")
	cmd.Flags().BoolVar(&o.Refresh, "refresh", o.Refresh, "refresh registry-derived offline package inventory")
	addRegistryFlags(cmd, &o.registryFiles)

	utils.CheckErr(cmd.MarkFlagRequired("name"))
	utils.CheckErr(cmd.MarkFlagRequired("version"))
	return cmd
}

func NewCmdResourceRefresh(o *ResourceOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "refresh [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "offline resource refresh",
		Long:                  refreshLongDescription,
		Example:               resourceRefreshExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete())
			utils.CheckErr(o.ValidateArgsRefresh(cmd))
			utils.CheckErr(o.ResourceRefresh())
		},
	}

	o.cliOpts.AddFlags(cmd.Flags())
	cmd.Flags().StringVar(&o.Registry, "registry", o.Registry, "OCI registry host:port for offline packages")
	addRegistryFlags(cmd, &o.registryFiles)
	return cmd
}

func addRegistryFlags(cmd *cobra.Command, opts *deliveryregistry.FileOptions) {
	cmd.Flags().StringVar(&opts.Scheme, "registry-scheme", opts.Scheme, "registry transport scheme: https or http (default https)")
	cmd.Flags().StringVar(&opts.Username, "registry-username", opts.Username, "registry username or robot account")
	cmd.Flags().StringVar(&opts.PasswordFile, "registry-password-file", opts.PasswordFile, "file containing the registry password or token")
	cmd.Flags().StringVar(&opts.CAFile, "registry-ca-file", opts.CAFile, "PEM CA file used to verify the registry")
	cmd.Flags().BoolVar(&opts.SkipTLSVerify, "registry-skip-tls-verify", opts.SkipTLSVerify,
		"skip registry TLS verification (not recommended)")
}

func (o *ResourceOptions) Complete() error {
	if o.indexer != nil || o.Registry == "" {
		return nil
	}
	var config *deliveryregistry.Config
	var err error
	if o.registryFiles.Specified() {
		config, err = o.registryFiles.Resolve(o.Registry)
	} else {
		config, err = deliveryregistry.Resolve(o.Registry)
	}
	if err != nil {
		return err
	}
	o.indexer = deliveryindexer.NewRegistryPackageInventoryIndexerWithConfig(config)
	return nil
}

func (o *ResourceOptions) CompleteList() error {
	return o.Complete()
}

func (o *ResourceOptions) ValidateArgs(cmd *cobra.Command) error {
	if o.Registry == "" {
		return utils.UsageErrorf(cmd, "--registry must be specified")
	}
	return nil
}

func (o *ResourceOptions) ValidateArgsList(cmd *cobra.Command) error {
	return o.ValidateArgs(cmd)
}

func (o *ResourceOptions) ValidateArgsInspect(cmd *cobra.Command) error {
	if o.Name == "" {
		return utils.UsageErrorf(cmd, "the name of resource must be specified")
	}
	if o.Version == "" {
		return utils.UsageErrorf(cmd, "the version of resource must be specified")
	}
	return o.ValidateArgs(cmd)
}

func (o *ResourceOptions) ValidateArgsRefresh(cmd *cobra.Command) error {
	return o.ValidateArgs(cmd)
}

func (o *ResourceOptions) ResourcePkgRules() error {
	logger.Info(">>> offline resources are discovered from OCI Registry under kubeclipper/packages/")
	logger.Info(">>> use 'kcctl resource list --registry <registry>', 'kcctl resource inspect --registry <registry> --name <name> --version <version>', or 'kcctl resource refresh --registry <registry>'")
	return nil
}

func (o *ResourceOptions) ResourceList() error {
	return o.ResourceListFromRegistry()
}

func (o *ResourceOptions) ResourceListFromRegistry() error {
	inventory, err := o.loadRegistryInventory(context.Background())
	if err != nil {
		return err
	}
	metas := o.ComponentMetasFromInventory(o.Registry, inventory)
	return o.PrintFlags.Print(o.filter([]*kc.ComponentMetas{metas})[metas.Node], o.IOStreams.Out)
}

func (o *ResourceOptions) ComponentMetasFromInventory(node string, inventory *deliveryapis.PackageInventory) *kc.ComponentMetas {
	metas := &kc.ComponentMetas{Node: node}
	for _, pkg := range inventory.Spec.Packages {
		if pkg.Transport.Type != deliveryapis.TransportOCI {
			continue
		}
		if pkg.Kind == "" || pkg.Name == "" || pkg.Version == "" || pkg.Arch == "" {
			continue
		}
		metas.PackageMetadata.Addons = append(metas.PackageMetadata.Addons, scheme.MetaResource{
			Type:    pkg.Kind,
			Name:    pkg.Name,
			Version: pkg.Version,
			Arch:    pkg.Arch,
		})
	}
	return metas
}

func (o *ResourceOptions) filter(data []*kc.ComponentMetas) map[string]printer.ResourcePrinter {
	var metaMap = make(map[string]printer.ResourcePrinter)

	for _, metas := range data {
		n := &kc.ComponentMetas{
			Node: metas.Node,
		}

		for _, resource := range metas.Addons {
			if o.Type != "" && resource.Type != o.Type {
				continue
			}
			if o.Name != "" && resource.Name != o.Name {
				continue
			}
			if o.Version != "" && resource.Version != o.Version {
				continue
			}
			if o.Arch != "" && resource.Arch != o.Arch {
				continue
			}
			n.PackageMetadata.Addons = append(n.PackageMetadata.Addons, resource)
		}
		n.TotalCount = len(n.PackageMetadata.Addons)
		metaMap[n.Node] = n
	}
	return metaMap
}

func (o *ResourceOptions) ResourceInspect() error {
	inventory, err := o.loadRegistryInventory(context.Background())
	if err != nil {
		return err
	}
	entries := o.matchInventoryPackages(inventory)
	if len(entries) == 0 {
		return fmt.Errorf("resource %s-%s not found in registry inventory", o.Name, o.Version)
	}
	return o.PrintFlags.Print(resourceInspectPrinter{entries: entries}, o.Out)
}

func (o *ResourceOptions) ResourceRefresh() error {
	inventory, err := o.refreshRegistryInventory(context.Background())
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(o.Out, "refreshed %d OCI packages from %s\n", len(inventory.Spec.Packages), o.Registry)
	return err
}

func (o *ResourceOptions) listType(toComplete string) []string {
	set := sets.NewString()
	resources, err := o.resourceList()
	if err != nil {
		return set.List()
	}
	for _, v := range resources {
		if strings.HasPrefix(v.Type, toComplete) {
			set.Insert(v.Type)
		}
	}
	return set.List()
}

func (o *ResourceOptions) listName(toComplete string) []string {
	set := sets.NewString()
	resources, err := o.resourceList()
	if err != nil {
		return set.List()
	}
	for _, v := range resources {
		if strings.HasPrefix(v.Name, toComplete) {
			set.Insert(v.Name)
		}
	}
	return set.List()
}

func (o *ResourceOptions) listVersion(toComplete string) []string {
	set := sets.NewString()
	resources, err := o.resourceList()
	if err != nil {
		return set.List()
	}
	for _, v := range resources {
		if strings.HasPrefix(v.Version, toComplete) {
			set.Insert(v.Version)
		}
	}
	return set.List()
}

func (o *ResourceOptions) listArch(toComplete string) []string {
	set := sets.NewString()
	resources, err := o.resourceList()
	if err != nil {
		return set.List()
	}
	for _, v := range resources {
		if strings.HasPrefix(v.Arch, toComplete) {
			set.Insert(v.Arch)
		}
	}
	return set.List()
}

func (o *ResourceOptions) resourceList() ([]scheme.MetaResource, error) {
	resources, err := o.registryResourceList()
	if err != nil {
		return nil, err
	}
	return o.filterMetaResources(resources), nil
}

func (o *ResourceOptions) registryResourceList() ([]scheme.MetaResource, error) {
	if o.Registry == "" {
		return nil, nil
	}
	inventory, err := o.loadRegistryInventory(context.Background())
	if err != nil {
		return nil, err
	}
	metas := o.ComponentMetasFromInventory(o.Registry, inventory)
	return metas.PackageMetadata.Addons, nil
}

func (o *ResourceOptions) filterMetaResources(resources []scheme.MetaResource) []scheme.MetaResource {
	list := make([]scheme.MetaResource, 0, len(resources))
	for _, v := range resources {
		if o.Type != "" && v.Type != o.Type {
			continue
		}
		if o.Name != "" && v.Name != o.Name {
			continue
		}
		if o.Version != "" && v.Version != o.Version {
			continue
		}
		if o.Arch != "" && v.Arch != o.Arch {
			continue
		}
		list = append(list, v)
	}
	return list
}

func (o *ResourceOptions) matchInventoryPackages(inventory *deliveryapis.PackageInventory) []deliveryapis.PackageEntry {
	if inventory == nil {
		return nil
	}
	entries := make([]deliveryapis.PackageEntry, 0, len(inventory.Spec.Packages))
	for _, pkg := range inventory.Spec.Packages {
		if pkg.Transport.Type != deliveryapis.TransportOCI {
			continue
		}
		if o.Type != "" && pkg.Kind != o.Type {
			continue
		}
		if o.Name != "" && pkg.Name != o.Name {
			continue
		}
		if o.Version != "" && pkg.Version != o.Version {
			continue
		}
		if o.Arch != "" && pkg.Arch != o.Arch {
			continue
		}
		entries = append(entries, pkg)
	}
	return entries
}

func (o *ResourceOptions) loadRegistryInventory(ctx context.Context) (*deliveryapis.PackageInventory, error) {
	indexer := o.indexer
	if indexer == nil {
		indexer = deliveryindexer.NewRegistryPackageInventoryIndexer(nil)
	}
	if o.Refresh {
		return o.refreshRegistryInventory(ctx)
	}
	return indexer.Index(ctx, o.Registry)
}

func (o *ResourceOptions) refreshRegistryInventory(ctx context.Context) (*deliveryapis.PackageInventory, error) {
	indexer := o.indexer
	if indexer == nil {
		indexer = deliveryindexer.NewRegistryPackageInventoryIndexer(nil)
	}
	refresher, ok := indexer.(RefreshingRegistryPackageInventoryIndexer)
	if !ok {
		return nil, fmt.Errorf("registry package inventory indexer does not support refresh")
	}
	return refresher.Refresh(ctx, o.Registry)
}

type resourceInspectPrinter struct {
	entries []deliveryapis.PackageEntry
}

func (p resourceInspectPrinter) JSONPrint() ([]byte, error) {
	return printer.JSONPrinter(p.entries)
}

func (p resourceInspectPrinter) YAMLPrint() ([]byte, error) {
	return printer.YAMLPrinter(p.entries)
}

func (p resourceInspectPrinter) TablePrint() ([]string, [][]string) {
	rows := make([][]string, 0, len(p.entries))
	for _, entry := range p.entries {
		rows = append(rows, []string{
			entry.Kind,
			entry.Name,
			entry.Version,
			entryOS(entry),
			entry.Arch,
			entry.Transport.Ref,
			entry.Transport.Digest,
			joinContentNames(entry.Contents),
		})
	}
	return []string{"TYPE", "NAME", "VERSION", "OS", "ARCH", "REF", "DIGEST", "CONTENTS"}, rows
}

func entryOS(entry deliveryapis.PackageEntry) string {
	if entry.OS == "" {
		return deliveryapis.DefaultPackageOS
	}
	return entry.OS
}

func joinContentNames(contents []deliveryapis.ArtifactContent) string {
	names := make([]string, 0, len(contents))
	for _, content := range contents {
		if content.Name == "" {
			continue
		}
		names = append(names, content.Name)
	}
	return strings.Join(names, ",")
}
