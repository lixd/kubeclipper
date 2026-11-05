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

package registry

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	containerv1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/yaml"

	"github.com/kubeclipper/kubeclipper/pkg/cli/registry/client"

	pkgerr "github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubeclipper/kubeclipper/pkg/cli/printer"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/pkg/cli/sudo"
	"github.com/kubeclipper/kubeclipper/pkg/utils/httputil"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/config"
	"github.com/kubeclipper/kubeclipper/pkg/cli/logger"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	deliverybootstrap "github.com/kubeclipper/kubeclipper/pkg/delivery/bootstrap"
	deliveryindexer "github.com/kubeclipper/kubeclipper/pkg/delivery/indexer"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/delivery/releasemanifest"
	"github.com/kubeclipper/kubeclipper/pkg/delivery/source"
	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"
)

const (
	longDescription = `
  Docker registry operation.

  Deploy and manage a Registry; export releases and sync online or offline content.
  Use docker engine API V2, visit the website(https://docs.docker.com/registry/spec/api/) for more information.`
	registryExample = `
  kcctl registry deploy --node 10.0.0.10 --pk-file key --package-registry ghcr.io/kubeclipper/kubeclipper
  kcctl registry export
  kcctl registry deploy --node 10.0.0.10 --pk-file key --bundle ./offline/registry-3.1.1.tar.gz
  kcctl registry sync --bundle ./offline/kubeclipper-offline-v2.0.11.tar.gz --target 10.0.0.10:5000/kubeclipper --target-scheme http`
	deployLongDescription = `Deploy Registry from the bootstrap/registry package, online or from its offline OCI bundle.`
	deployExample         = `
  kcctl registry deploy --node 10.0.0.10 --pk-file key --package-registry registry.cn-hangzhou.aliyuncs.com/kubeclipper-release
  kcctl registry deploy --node 10.0.0.10 --pk-file key --bundle registry-3.1.1.tar.gz`
	cleanLongDescription = `
  Clean docker registry by flags.`
	cleanExample = `
  # Clean docker registry
  kcctl registry clean --pk-file key --node 10.0.0.111
  # Clean docker registry using saved config (node and SSH from registry-config.yaml)
  kcctl registry clean
  # Clean docker registry, specify data directory.
  # If you used custom data directory when deploy,then must specify it in this cmd to clear data.
  kcctl registry clean --pk-file key --node 10.0.0.111 --registry-volume /opt/registry --data-root /var/lib/docker
  # Forced to clean docker registry
  kcctl registry clean --pk-file key --node 10.0.0.111  --force

  Please read 'kcctl registry clean -h' get more registry clean flags.`
	listLongDescription = `
  Lists docker repositories or images by flags.`
	listExample = `
  # Lists docker repositories (uses saved config or --node)
  kcctl registry list
  # Lists docker repositories with explicit node
  kcctl registry list --node 10.0.0.111
  # Lists docker images
  kcctl registry list --type image --name etcd

  Please read 'kcctl registry list -h' get more registry list flags.`
	deleteLongDescription = `
  Delete the docker image by name and tag.`
	deleteExample = `
  # Delete docker image
  kcctl registry delete --pk-file key --node 10.0.0.111 --name etcd --tag 3.5.1-0
  # Delete using saved config
  kcctl registry delete --name etcd --tag 3.5.1-0

  Please read 'kcctl registry delete -h' get more registry delete flags.`
)

type RegistryOptions struct {
	options.IOStreams
	PrintFlags *printer.PrintFlags
	SSHConfig  *sshutils.SSH

	Node string

	PackageRegistry      string
	PackageRegistryFiles deliveryregistry.FileOptions
	BundlePath           string
	Arch                 string

	DataRoot     string
	RegistryPort int

	Type   string
	Name   string
	Tag    string
	Number int

	// tracks which flags were explicitly set by the user
	nodeChanged     bool
	portChanged     bool
	sshUserChanged  bool
	pkFileChanged   bool
	pkPasswdChanged bool
}

var (
	allowType = sets.NewString("image", "repository")
)

func NewRegistryOptions(streams options.IOStreams) *RegistryOptions {
	return &RegistryOptions{
		IOStreams:    streams,
		PrintFlags:   printer.NewPrintFlags(),
		SSHConfig:    sshutils.NewSSH(),
		DataRoot:     "/var/lib/registry",
		RegistryPort: 5000,
		Type:         "repository",
	}
}

func NewCmdRegistry(streams options.IOStreams) *cobra.Command {
	o := NewRegistryOptions(streams)
	cmd := &cobra.Command{
		Use:                   "registry",
		DisableFlagsInUseLine: true,
		Short:                 "Registry operation",
		Long:                  longDescription,
		Example:               registryExample,
		Args:                  cobra.NoArgs,
	}

	cmd.AddCommand(NewCmdRegistryDeploy(o))
	cmd.AddCommand(NewCmdRegistryClean(o))
	cmd.AddCommand(NewCmdRegistryExport(o))
	cmd.AddCommand(NewCmdRegistryList(o))
	cmd.AddCommand(NewCmdRegistryDelete(o))
	cmd.AddCommand(NewCmdRegistrySync(o))

	return cmd
}

func NewCmdRegistryDeploy(o *RegistryOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "deploy --node <node> [--package-registry <registry> | --bundle <registry.tar.gz>] [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "registry deploy",
		Long:                  deployLongDescription,
		Example:               deployExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			o.trackChangedFlags(cmd)
			utils.CheckErr(o.Complete(cmd))
			utils.CheckErr(o.ValidateArgsDeploy())
			if !o.sudoPreCheck() {
				return
			}
			utils.CheckErr(o.Install())
		},
	}

	options.AddFlagsToSSH(o.SSHConfig, cmd.Flags())
	cmd.Flags().StringVar(&o.Node, "node", o.Node, "node to deploy registry.")
	cmd.Flags().StringVar(&o.PackageRegistry, "package-registry", "", "package registry prefix overriding the KCZONE preset")
	cmd.Flags().StringVar(&o.BundlePath, "bundle", "", "separate Registry bootstrap OCI bundle")
	cmd.Flags().StringVar(&o.Arch, "arch", "", "target architecture: amd64 or arm64; detected over SSH when omitted")
	cmd.Flags().StringVar(&o.PackageRegistryFiles.Scheme, "package-registry-scheme", "", "package registry scheme: https or http")
	cmd.Flags().StringVar(&o.PackageRegistryFiles.Username, "package-registry-username", "", "package registry username")
	cmd.Flags().StringVar(&o.PackageRegistryFiles.PasswordFile, "package-registry-password-file", "",
		"file containing the package registry password")
	cmd.Flags().StringVar(&o.PackageRegistryFiles.CAFile, "package-registry-ca-file", "", "package registry CA certificate file")
	cmd.Flags().StringVar(&o.DataRoot, "data-root", o.DataRoot, "set registry data root directory.")
	cmd.Flags().IntVar(&o.RegistryPort, "registry-port", o.RegistryPort, "set registry port")

	utils.CheckErr(cmd.MarkFlagRequired("node"))
	return cmd
}

func NewCmdRegistryClean(o *RegistryOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "clean (--user | -u <user>) (--passwd <passwd>) (--pk-file <pk-file>) (--pk-passwd <pk-passwd>) [--node <node>]",
		DisableFlagsInUseLine: true,
		Short:                 "registry clean",
		Long:                  cleanLongDescription,
		Example:               cleanExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			o.trackChangedFlags(cmd)
			utils.CheckErr(o.Complete(cmd))
			utils.CheckErr(o.ValidateArgs())
			if !o.sudoPreCheck() {
				return
			}
			utils.CheckErr(o.Uninstall())
		},
	}

	options.AddFlagsToSSH(o.SSHConfig, cmd.Flags())
	cmd.Flags().StringVar(&o.Node, "node", o.Node, "registry node. If not specified, uses the current registry from config.")
	return cmd
}

func NewCmdRegistryList(o *RegistryOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "list [--node <node>] [--name <name>] [--registry-port <registry-port>] [--type <type>] [--number <number>] [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "registry list repository or image",
		Long:                  listLongDescription,
		Example:               listExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			o.trackChangedFlags(cmd)
			utils.CheckErr(o.Complete(cmd))
			utils.CheckErr(o.ValidateArgsList())
			if !o.healthPreCheck() {
				return
			}
			utils.CheckErr(o.List())
		},
	}
	o.PrintFlags.AddFlags(cmd)
	options.AddFlagsToSSH(o.SSHConfig, cmd.Flags())
	cmd.Flags().StringVar(&o.Node, "node", o.Node, "registry node. If not specified, uses the current registry from config.")
	cmd.Flags().IntVar(&o.RegistryPort, "registry-port", o.RegistryPort, "registry port")

	cmd.Flags().StringVar(&o.Type, "type", o.Type, "image or repository (default: repository)")
	cmd.Flags().StringVar(&o.Name, "name", o.Name, "image name")
	cmd.Flags().IntVar(&o.Number, "number", o.Number, "number of entries in each response. It not present, all entries will be returned.")

	utils.CheckErr(cmd.RegisterFlagCompletionFunc("type", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return allowType.List(), cobra.ShellCompDirectiveNoFileComp
	}))
	utils.CheckErr(cmd.RegisterFlagCompletionFunc("name", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return o.listRepos(toComplete), cobra.ShellCompDirectiveNoFileComp
	}))

	return cmd
}

func NewCmdRegistryDelete(o *RegistryOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "delete [--node <node>] (--name <name>) (--tag <tag>) [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "registry delete image",
		Long:                  deleteLongDescription,
		Example:               deleteExample,
		Run: func(cmd *cobra.Command, args []string) {
			o.trackChangedFlags(cmd)
			utils.CheckErr(o.Complete(cmd))
			utils.CheckErr(o.ValidateArgsDelete(cmd))
			if !o.healthPreCheck() {
				return
			}
			utils.CheckErr(o.Delete())
		},
	}

	options.AddFlagsToSSH(o.SSHConfig, cmd.Flags())
	cmd.Flags().IntVar(&o.RegistryPort, "registry-port", o.RegistryPort, "registry port")
	cmd.Flags().StringVar(&o.Node, "node", o.Node, "registry node. If not specified, uses the current registry from config.")
	cmd.Flags().StringVar(&o.Name, "name", o.Name, "image name")
	cmd.Flags().StringVar(&o.Tag, "tag", o.Tag, "image tag")

	utils.CheckErr(cmd.RegisterFlagCompletionFunc("name", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return o.listRepos(toComplete), cobra.ShellCompDirectiveNoFileComp
	}))
	utils.CheckErr(cmd.RegisterFlagCompletionFunc("tag", func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		return o.listTags(toComplete), cobra.ShellCompDirectiveNoFileComp
	}))

	utils.CheckErr(cmd.MarkFlagRequired("name"))
	utils.CheckErr(cmd.MarkFlagRequired("tag"))
	return cmd
}

// trackChangedFlags records which flags were explicitly set by the user.
func (o *RegistryOptions) trackChangedFlags(cmd *cobra.Command) {
	o.nodeChanged = cmd.Flags().Changed("node")
	o.portChanged = cmd.Flags().Changed("registry-port")
	o.sshUserChanged = cmd.Flags().Changed("user")
	o.pkFileChanged = cmd.Flags().Changed("pk-file")
	o.pkPasswdChanged = cmd.Flags().Changed("pk-passwd")

}

// Complete loads registry config and fills in unset options from the matching registry entry.
// When --node is specified, it looks up that node's entry; otherwise uses the current entry.
func (o *RegistryOptions) Complete(_ *cobra.Command) error {
	cfg, err := LoadRegistryConfig()
	if err != nil {
		return fmt.Errorf("load registry config: %w", err)
	}
	var entry *RegistryEntry
	if o.nodeChanged && o.Node != "" {
		entry = findRegistry(cfg, o.Node)
	} else {
		entry = GetCurrentRegistry(cfg)
	}
	if entry != nil {
		applyEntryToOptions(o, entry, o.nodeChanged, o.portChanged, o.sshUserChanged, o.pkFileChanged, o.pkPasswdChanged)
	}
	return nil
}

func (o *RegistryOptions) healthPreCheck() bool {
	return o.healthz(true)
}

func (o *RegistryOptions) sudoPreCheck() bool {
	return sudo.PreCheck("sudo", o.SSHConfig, o.IOStreams, []string{o.Node})
}

func (o *RegistryOptions) ValidateArgs() error {
	if o.Node == "" {
		return fmt.Errorf("--node must be specified, or run 'kcctl registry deploy' first to save registry config")
	}
	if o.SSHConfig.PkFile == "" && o.SSHConfig.Password == "" {
		return fmt.Errorf("one of --pk-file or --passwd must be specified")
	}
	return nil
}

func (o *RegistryOptions) ValidateArgsDeploy() error {
	if o.SSHConfig.PkFile == "" && o.SSHConfig.Password == "" {
		return fmt.Errorf("one of --pk-file or --passwd must be specified")
	}
	if o.Node == "" {
		return fmt.Errorf("--node must be specified")
	}
	if o.Arch != "" && o.Arch != releasemanifest.ArchAMD64 && o.Arch != releasemanifest.ArchARM64 {
		return fmt.Errorf("--arch must be amd64 or arm64")
	}
	if o.BundlePath != "" && (o.PackageRegistry != "" || o.PackageRegistryFiles.Specified()) {
		return fmt.Errorf("--bundle and package registry options are mutually exclusive")
	}
	return nil
}

func (o *RegistryOptions) ValidateArgsList() error {
	if o.Node == "" {
		return fmt.Errorf("--node must be specified, or run 'kcctl registry deploy' first to save registry config")
	}
	if o.Type != "image" && o.Type != "repository" {
		return fmt.Errorf("--type must be one of image,repository")
	}
	if o.Type == "image" && o.Name == "" {
		return fmt.Errorf("when type=image,--name is required")
	}
	return nil
}

func (o *RegistryOptions) ValidateArgsDelete(cmd *cobra.Command) error {
	if o.Node == "" {
		return fmt.Errorf("--node must be specified, or run 'kcctl registry deploy' first to save registry config")
	}
	if o.Name == "" {
		return utils.UsageErrorf(cmd, "image name must be specified")
	}
	if o.Tag == "" {
		return utils.UsageErrorf(cmd, "image tag must be specified")
	}
	return nil
}

func (o *RegistryOptions) deployRegistry() error {
	data, err := o.GetKcRegistryConfigTemplateContent()
	if err != nil {
		return pkgerr.WithMessage(err, "render kc registry config failed")
	}
	cmdList := []string{
		"mkdir -pv /etc/kubeclipper-registry",
		sshutils.WrapEcho(data, "/etc/kubeclipper-registry/kubeclipper-registry.yaml"),
		sshutils.WrapEcho(config.KcRegistryService, "/usr/lib/systemd/system/kc-registry.service"),
		"systemctl daemon-reload && systemctl enable kc-registry --now",
	}
	for _, cmd := range cmdList {
		ret, err := sshutils.SSHCmdWithSudo(o.SSHConfig, o.Node, cmd)
		if err != nil {
			return pkgerr.WithMessagef(err, "run deploy kc registry cmd[%s] failed", cmd)
		}
		if err = ret.Error(); err != nil {
			return pkgerr.WithMessagef(err, "deploy kc registry cmd[%s] failed", cmd)
		}
	}

	// check registry health
	err = wait.PollImmediate(time.Second, time.Second*15, func() (done bool, err error) {
		ok := o.healthz(false)
		if ok {
			logger.Info("registry is health.")
		} else {
			logger.V(2).Info("waiting for registry health.")
		}
		return ok, nil
	})
	if err != nil {
		return fmt.Errorf("registry health check failed on %s: %w", o.Node, err)
	}
	return nil
}

func (o *RegistryOptions) GetKcRegistryConfigTemplateContent() (string, error) {
	tmpl, err := template.New("text").Parse(config.KcRegistryConfigTmpl)
	if err != nil {
		return "", fmt.Errorf("template parse failed: %s", err.Error())
	}

	var data = make(map[string]any)
	data["RegistryPort"] = o.RegistryPort
	data["DataRoot"] = o.DataRoot
	var buffer bytes.Buffer
	if err = tmpl.Execute(&buffer, data); err != nil {
		return "", fmt.Errorf("template execute failed: %s", err.Error())
	}
	return buffer.String(), nil
}

func (o *RegistryOptions) Install() error {
	if err := o.installRegistryBinary(); err != nil {
		return fmt.Errorf("install registry binary error: %s", err.Error())
	}

	if err := o.deployRegistry(); err != nil {
		return fmt.Errorf("deploy registry error: %s", err.Error())
	}

	// save registry config
	if err := o.saveRegistryConfigAfterDeploy(); err != nil {
		logger.Warnf("save registry config failed: %s", err.Error())
	}

	o.printUsage()
	return nil
}

func (o *RegistryOptions) saveRegistryConfigAfterDeploy() error {
	cfg, err := LoadRegistryConfig()
	if err != nil {
		return err
	}
	AddOrUpdateRegistry(cfg, newEntryFromOptions(o))
	return SaveRegistryConfig(cfg)
}

func (o *RegistryOptions) printUsage() {
	success := "deploy registry successfully.\nthere is some tips to visit your repository: \n"
	usage1 := fmt.Sprintf("\t1. visit http://%s/v2/_catalog\n", o.registry())
	usage2 := ""
	if o.RegistryPort != 5000 {
		usage2 = fmt.Sprintf("\t2. run cmd [kcctl registry list --node %s --registry-port %v]", o.Node, o.RegistryPort)
	} else {
		usage2 = "\t2. run cmd [kcctl registry list]"
	}
	fmt.Printf("\033[1;36;36m%s\033[0m\n", success+usage1+usage2)
}

func (o *RegistryOptions) Uninstall() error {
	// clean registry container
	err := o.stopRegistry()
	if err != nil {
		return err
	}

	// clean registry volume and kc package
	err = o.cleanRegistry()
	if err != nil {
		return err
	}

	// remove from registry config
	if err := o.removeRegistryConfigAfterClean(); err != nil {
		logger.Warnf("remove registry config failed: %s", err.Error())
	}

	logger.Info("registry uninstall successfully")
	return nil
}

func (o *RegistryOptions) removeRegistryConfigAfterClean() error {
	cfg, err := LoadRegistryConfig()
	if err != nil {
		return err
	}
	RemoveRegistry(cfg, o.Node)
	return SaveRegistryConfig(cfg)
}

func (o *RegistryOptions) cleanRegistry() error {
	c, err := o.readConfig()
	if err != nil {
		logger.Warn("read registry conf failed, use default value,err: ", err)
	} else {
		o.DataRoot = c.Storage.Filesystem.Rootdirectory
	}

	// clean registry volume and kc package
	cmdList := []string{
		fmt.Sprintf(`rm -rf %s`, o.DataRoot),
		"rm -rf /etc/kubeclipper-registry",
	}
	for _, cmd := range cmdList {
		ret, err := sshutils.SSHCmdWithSudo(o.SSHConfig, o.Node, cmd)
		if err != nil {
			return err
		}
		if err = ret.Error(); err != nil {
			return err
		}
	}
	return nil
}

type Configuration struct {
	// Version is the version which defines the format of the rest of the configuration
	Version string `yaml:"version"`

	// Storage is the configuration for the registry's storage driver
	Storage struct {
		Filesystem struct {
			Rootdirectory string `yaml:"rootdirectory"`
		} `yaml:"filesystem"`
	} `yaml:"storage"`
}

func (o *RegistryOptions) readConfig() (Configuration, error) {
	var c Configuration
	ret, err := sshutils.SSHCmdWithSudo(o.SSHConfig, o.Node, "cat /etc/kubeclipper-registry/kubeclipper-registry.yaml")
	if err != nil {
		return c, err
	}
	if err = ret.Error(); err != nil {
		return c, err
	}

	if err = yaml.Unmarshal([]byte(ret.Stdout), &c); err != nil {
		return c, err
	}
	return c, nil
}

func (o *RegistryOptions) stopRegistry() error {
	cmdList := []string{
		"systemctl disable kc-registry --now",
		"rm -rf /usr/lib/systemd/system/kc-registry.service",
		"systemctl reset-failed kc-registry || true",
		"rm -rf /usr/local/bin/registry",
	}

	for _, cmd := range cmdList {
		ret, err := sshutils.SSHCmdWithSudo(o.SSHConfig, o.Node, cmd)
		if err != nil {
			return err
		}
		if err = ret.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (o *RegistryOptions) List() error {
	var err error
	switch o.Type {
	case "image":
		err = o.listImages()
	case "repository":
		err = o.listRepositories()
	}
	return err
}

func (o *RegistryOptions) Delete() error {
	if o.Tag == "" {
		return errors.New("missing required arguments: 'tag'")
	}
	return client.Delete(o.registry(), o.Name, o.Tag)
}

func (o *RegistryOptions) listRepositories() error {
	catalog, err := client.Catalog(o.registry())
	if err != nil {
		return err
	}
	repository := &Repositories{
		Repositories: catalog,
	}
	return o.PrintFlags.Print(repository, o.IOStreams.Out)
}

// healthz check is the node or port ok
func (o *RegistryOptions) healthz(log bool) bool {
	url := fmt.Sprintf("http://%s:%d/v2/", o.Node, o.RegistryPort)
	_, code, respErr := httputil.CommonRequest(url, "GET", nil, nil, nil)
	if respErr != nil {
		if !log {
			return false
		}
		logger.Error("health check failed,err:", respErr)
		// if request failed and port is default，print a hit for user to specify registry-port
		if isConnectErr(respErr) && o.RegistryPort == NewRegistryOptions(o.IOStreams).RegistryPort {
			logger.Infof("connect failed, maybe the default registry port(%v) is wrong,if you used custom port when deploy,you can used --registry-port to specify it", o.RegistryPort)
		}
		return false
	}
	logger.V(2).Infof("registry health check,url [%s],http code is:%v", url, code)
	return code == http.StatusOK
}

func isConnectErr(err error) bool {
	return strings.Contains(err.Error(), "connect: connection refused")
}

func (o *RegistryOptions) listImages() error {
	tags, err := client.ListTags(o.registry(), o.Name)
	if err != nil {
		return err
	}
	image := &Image{
		Name: o.Name,
		Tags: tags,
	}
	return o.PrintFlags.Print(image, o.IOStreams.Out)
}

func (o *RegistryOptions) installRegistryBinary() error {
	binaryPath, cleanup, err := o.obtainRegistryBinary()
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		return err
	}

	logger.V(3).Info("send registry binary file")
	chmod := "chmod +x /usr/local/bin/registry"
	if err = utils.SendPackage(o.SSHConfig, binaryPath, []string{o.Node}, "/usr/local/bin", nil, &chmod); err != nil {
		return err
	}
	logger.Info("registry binary installed successfully")
	return nil
}

func (o *RegistryOptions) registry() string {
	return fmt.Sprintf("%s:%v", o.Node, o.RegistryPort)
}

func (o *RegistryOptions) obtainRegistryBinary() (string, func(), error) {
	if o.Arch == "" {
		result, err := sshutils.SSHCmd(o.SSHConfig, o.Node, "uname -m")
		if err != nil {
			return "", nil, err
		}
		if err := result.Error(); err != nil {
			return "", nil, err
		}
		switch strings.TrimSpace(result.Stdout) {
		case "x86_64":
			o.Arch = releasemanifest.ArchAMD64
		case "aarch64", "arm64":
			o.Arch = releasemanifest.ArchARM64
		default:
			return "", nil, fmt.Errorf("unsupported Registry target architecture %q", result.Stdout)
		}
	}
	ctx := context.Background()
	if o.BundlePath != "" {
		bundle, err := releasemanifest.OpenBundle(ctx, o.BundlePath)
		if err != nil {
			return "", nil, err
		}
		defer bundle.Close()
		image, err := bundle.RegistryImage(o.Arch)
		if err != nil {
			return "", nil, err
		}
		return extractRegistryPackageBinary(image, o.Arch)
	}
	registry, err := source.Registry(o.PackageRegistry)
	if err != nil {
		return "", nil, err
	}
	var cfg *deliveryregistry.Config
	if o.PackageRegistryFiles.Specified() {
		cfg, err = o.PackageRegistryFiles.Resolve(registry)
	} else {
		cfg, err = deliveryregistry.Resolve(registry)
	}
	if err != nil {
		return "", nil, err
	}
	return deliverybootstrap.FetchRegistryBinary(ctx, registry, o.Arch, cfg)
}

func extractRegistryPackageBinary(image containerv1.Image, arch string) (binary string, cleanup func(), err error) {
	manifest, err := deliveryindexer.ReadPackageManifest(image)
	if err != nil {
		return "", nil, err
	}
	if manifest.SchemaVersion != 1 || manifest.Kind != "bootstrap" || manifest.Name != "registry" ||
		manifest.Platform.OS != "linux" || manifest.Platform.Arch != arch {
		return "", nil, fmt.Errorf("expected bootstrap/registry package for linux/%s", arch)
	}
	expected := ""
	for _, content := range manifest.Contents {
		if content.Name == "registry" && content.File == "registry" {
			expected = content.Digest
		}
	}
	if expected == "" {
		return "", nil, fmt.Errorf("registry package has no registry binary digest")
	}
	path, cleanup, err := extractRegistryBinaryToTemp(image)
	if err != nil {
		return "", cleanup, err
	}
	file, err := os.Open(path)
	if err != nil {
		return "", cleanup, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", cleanup, err
	}
	if fmt.Sprintf("sha256:%x", hash.Sum(nil)) != expected {
		return "", cleanup, fmt.Errorf("registry binary digest mismatch")
	}
	return path, cleanup, nil
}

func extractRegistryBinaryToTemp(img containerv1.Image) (string, func(), error) {
	tmpDir, err := os.MkdirTemp("", "kc-registry-image-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() {
		_ = os.RemoveAll(tmpDir)
	}
	dst := filepath.Join(tmpDir, "registry")
	if err = extractRegistryBinary(img, dst); err != nil {
		cleanup()
		return "", nil, err
	}
	return dst, cleanup, nil
}

func extractRegistryBinary(img containerv1.Image, dst string) error {
	rc := mutate.Extract(img)
	defer rc.Close()
	tr := tar.NewReader(rc)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("registry binary not found in package image")
		}
		if err != nil {
			return err
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		if strings.TrimPrefix(filepath.ToSlash(filepath.Clean(header.Name)), "/") != "opt/kubeclipper/resource/registry" {
			continue
		}
		mode := os.FileMode(header.Mode)
		if mode == 0 {
			mode = 0755
		}
		return writeReaderToFile(tr, dst, mode|0100)
	}
}

func writeReaderToFile(src io.Reader, dst string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err = io.Copy(out, src); err != nil {
		out.Close()
		return err
	}
	if err = out.Close(); err != nil {
		return err
	}
	return os.Chmod(dst, mode)
}

func (o *RegistryOptions) listTags(toComplete string) []string {
	if o.Name == "" {
		return nil
	}
	tags, err := client.ListTags(o.registry(), o.Name)
	if err != nil {
		logger.V(2).Warnf("list tags error: %s", err.Error())
	}
	set := sets.NewString()
	for _, v := range tags {
		if strings.HasPrefix(v, toComplete) {
			set.Insert(v)
		}
	}
	return set.List()
}

func (o *RegistryOptions) listRepos(toComplete string) []string {
	repositories, err := client.Catalog(o.registry())
	if err != nil {
		logger.V(2).Warnf("list repositories error: %s", err.Error())
		return nil
	}
	set := sets.NewString()
	for _, value := range repositories {
		if strings.HasPrefix(value, toComplete) {
			set.Insert(value)
		}
	}
	return set.List()
}
