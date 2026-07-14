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
	"compress/gzip"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/google/go-containerregistry/pkg/crane"
	containerv1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
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
	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"
)

const (
	defaultRegistryPackageRegistry = "ghcr.io/lixd/kubeclipper"
	defaultRegistryVersion         = "3.1.1"
	packageRegistryBinaryPath      = "/opt/kubeclipper/resource/registry"
	standardRegistryBinaryPath     = "/bin/registry"
	offlineBundleRegistryArchive   = "kubeclipper-offline-registry-bundle/bootstrap/registry-image.tar"
	offlineBundleChecksums         = "kubeclipper-offline-registry-bundle/SHA256SUMS"

	longDescription = `
  Docker registry operation.

  Currently, you can deploy, clean, push, list and delete docker registry.
  Use docker engine API V2, visit the website(https://docs.docker.com/registry/spec/api/) for more information.`
	registryExample = `
  # Deploy docker registry from a package registry
  kcctl registry deploy --pk-file key --node 10.0.0.111
  # Deploy docker registry from an offline image archive
  kcctl registry deploy --pk-file key --node 10.0.0.111 --registry-image-archive registry-v1.8.0-linux-amd64.tar.gz
  # Deploy docker registry directly from a self-bootstrapping offline Registry bundle
  kcctl registry deploy --pk-file key --node 10.0.0.111 --offline-bundle kubeclipper-offline-registry-bundle-v1.8.0-amd64.tar.gz
  # Clean docker registry
  kcctl registry clean --pk-file key --node 10.0.0.111
  # Push docker image to registry
  kcctl registry push --node 10.0.0.111 --image-archive images.tar.gz
  # List repositories in docker registry
  kcctl registry list
  # List repositories with explicit node
  kcctl registry list --node 10.0.0.111
  # List images
  kcctl registry list --type image --name etcd
  # Delete docker image
  kcctl registry delete --node 10.0.0.111 --name etcd --tag 1.5.1-0

  Please read 'kcctl registry -h' get more registry flags.`
	deployLongDescription = `
  Deploy docker registry.`
	deployExample = `
  # Deploy docker registry from a package registry
  kcctl registry deploy --pk-file key --node 10.0.0.111
  # Deploy docker registry specify data directory
  kcctl registry deploy --pk-file key --node 10.0.0.111 --data-root /var/lib/myregistry
  # Deploy docker registry specify port
  # If you used custom port,then must specify it in push、list、delete cmd.
  kcctl registry deploy --pk-file key --node 10.0.0.111 --registry-port 6666
  # Deploy docker registry from an offline image archive
  kcctl registry deploy --pk-file key --node 10.0.0.111 --registry-image-archive registry-v1.8.0-linux-amd64.tar.gz
  # Deploy docker registry from a self-bootstrapping offline Registry bundle
  kcctl registry deploy --pk-file key --node 10.0.0.111 --offline-bundle kubeclipper-offline-registry-bundle-v1.8.0-amd64.tar.gz
  # Deploy docker registry from a local binary
  kcctl registry deploy --pk-file key --node 10.0.0.111 --registry-binary registry-linux-amd64

  Please read 'kcctl registry deploy -h' get more registry deploy flags.`
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
	pushLongDescription = `
  Push docker image by flags.`
	pushExample = `
  # Push docker image to registry
  # You can use [docker save  $images > images.tar] or [docker save  $images > images.tar && gzip -f images.tar]  to generate image archive
  # example: docker save k8s.gcr.io/pause:3.2 k8s.gcr.io/coredns/coredns:1.6.7 > images.tar
  # example: docker save k8s.gcr.io/pause:3.2 k8s.gcr.io/coredns/coredns:1.6.7 > images.tar && gzip -f images.tar
  kcctl registry push --node 10.0.0.111 --image-archive images.tar
  kcctl registry push --node 10.0.0.111 --image-archive images.tar.gz
  # Push using saved config
  kcctl registry push --image-archive images.tar.gz

  Please read 'kcctl registry push -h' get more registry push flags.`
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

	ImageArchive         string
	RegistryImage        string
	RegistryImageArchive string
	RegistryBinary       string
	OfflineBundle        string
	PackageRegistry      string
	Version              string
	Arch                 string

	DataRoot     string
	RegistryPort int

	Type      string
	Name      string
	Tag       string
	Number    int
	TagSuffix string

	// tracks which flags were explicitly set by the user
	nodeChanged            bool
	portChanged            bool
	sshUserChanged         bool
	pkFileChanged          bool
	pkPasswdChanged        bool
	registryImageChanged   bool
	registryArchiveChanged bool
	registryBinaryChanged  bool
	offlineBundleChanged   bool
	packageRegistryChanged bool
	versionChanged         bool
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
	cmd.AddCommand(NewCmdRegistryPush(o))
	cmd.AddCommand(NewCmdRegistryList(o))
	cmd.AddCommand(NewCmdRegistryDelete(o))

	return cmd
}

func NewCmdRegistryDeploy(o *RegistryOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "deploy (--user | -u <user>) (--passwd <passwd>) (--pk-file <pk-file>) (--pk-passwd <pk-passwd>) (--node <node>) [--offline-bundle <bundle> | --registry-image <image> | --registry-image-archive <archive> | --registry-binary <binary> | --package-registry <registry>] (--data-root <data-root>)  (--registry-port <registry-port>) [flags]",
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
	cmd.Flags().StringVar(&o.RegistryImage, "registry-image", o.RegistryImage, "full registry bootstrap package image reference.")
	cmd.Flags().StringVar(&o.RegistryImageArchive, "registry-image-archive", o.RegistryImageArchive, "local docker image archive containing the registry binary.")
	cmd.Flags().StringVar(&o.RegistryBinary, "registry-binary", o.RegistryBinary, "local registry binary path.")
	cmd.Flags().StringVar(&o.OfflineBundle, "offline-bundle", o.OfflineBundle, "self-bootstrapping KubeClipper offline Registry bundle.")
	cmd.Flags().StringVar(&o.PackageRegistry, "package-registry", o.PackageRegistry, "OCI registry prefix containing kubeclipper/packages/bootstrap/registry. Default: ghcr.io/lixd/kubeclipper.")
	cmd.Flags().StringVar(&o.Version, "version", o.Version, "registry bootstrap image version. Default: 3.1.1.")
	cmd.Flags().StringVar(&o.Arch, "arch", o.Arch, "registry bootstrap image architecture. Default: detected from the target node.")
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

func NewCmdRegistryPush(o *RegistryOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "push [--node <node>] (--image-archive <archive>) [--registry-port <registry-port>] [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "registry push image",
		Long:                  pushLongDescription,
		Example:               pushExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			o.trackChangedFlags(cmd)
			utils.CheckErr(o.Complete(cmd))
			utils.CheckErr(o.ValidateArgsPush())
			if !o.healthPreCheck() {
				return
			}
			utils.CheckErr(o.Push())
		},
	}

	cmd.Flags().StringVar(&o.Node, "node", o.Node, "registry node. If not specified, uses the current registry from config.")
	cmd.Flags().StringVar(&o.ImageArchive, "image-archive", o.ImageArchive, "Path to a docker image archive (.tar or .tar.gz) generated by docker save.")
	cmd.Flags().StringVar(&o.TagSuffix, "tag-suffix", o.TagSuffix, "Append a suffix to the final image tag. For example, if the original image is library/busybox:1.36 and you set --tag-suffix=-amd64, the image will be pushed as library/busybox:1.36-amd64. Useful for creating architecture-specific tags in one shot.")
	cmd.Flags().IntVar(&o.RegistryPort, "registry-port", o.RegistryPort, "registry port.")

	utils.CheckErr(cmd.MarkFlagRequired("image-archive"))
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
	o.registryImageChanged = cmd.Flags().Changed("registry-image")
	o.registryArchiveChanged = cmd.Flags().Changed("registry-image-archive")
	o.registryBinaryChanged = cmd.Flags().Changed("registry-binary")
	o.offlineBundleChanged = cmd.Flags().Changed("offline-bundle")
	o.packageRegistryChanged = cmd.Flags().Changed("package-registry")
	o.versionChanged = cmd.Flags().Changed("version")
}

// Complete loads registry config and fills in unset options from the matching registry entry.
// When --node is specified, it looks up that node's entry; otherwise uses the current entry.
func (o *RegistryOptions) Complete(cmd *cobra.Command) error {
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
	if cmd.Name() == "deploy" && o.Arch == "" && o.Node != "" && (o.SSHConfig.PkFile != "" || o.SSHConfig.Password != "") {
		result, err := sshutils.SSHCmd(o.SSHConfig, o.Node, "uname -m")
		if err != nil {
			return fmt.Errorf("detect registry node architecture: %w", err)
		}
		o.Arch, err = normalizeRegistryArchitecture(result.Stdout)
		if err != nil {
			return err
		}
	}
	return nil
}

func normalizeRegistryArchitecture(machine string) (string, error) {
	switch strings.TrimSpace(machine) {
	case "x86_64", "amd64":
		return "amd64", nil
	case "aarch64", "arm64":
		return "arm64", nil
	default:
		return "", fmt.Errorf("unsupported registry node architecture %q", strings.TrimSpace(machine))
	}
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

func (o *RegistryOptions) ValidateArgsPush() error {
	if o.Node == "" {
		return fmt.Errorf("--node must be specified, or run 'kcctl registry deploy' first to save registry config")
	}
	if o.ImageArchive == "" {
		return fmt.Errorf("--image-archive must be specified")
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
	if o.Arch == "" {
		return fmt.Errorf("--arch must be specified")
	}
	sources := o.explicitRegistryDeploySources()
	if len(sources) > 1 {
		return fmt.Errorf("registry deploy source flags are mutually exclusive: %s", strings.Join(sources, ", "))
	}
	if o.Version != "" && (o.RegistryImage != "" || o.RegistryImageArchive != "" || o.RegistryBinary != "" || o.OfflineBundle != "") {
		return fmt.Errorf("--version can only be used with --package-registry or the default package registry")
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
		logger.Warnf("waiting for registry health timeout,"+
			"please ssh to node [%s] and run cmd [journalctl -xu kc-registry] for more information.", o.Node)
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

func (o *RegistryOptions) Push() error {
	logger.Info("process image archive")
	err := utils.SendPackageLocal(o.ImageArchive, config.DefaultPkgPath, nil)
	if err != nil {
		return err
	}
	fullPath := path.Join(config.DefaultPkgPath, path.Base(o.ImageArchive))

	if strings.HasSuffix(fullPath, ".tar.gz") {
		logger.Info("unzip image archive")
		if err = sshutils.Cmd("gzip", "-df", fullPath); err != nil {
			return err
		}
		fullPath = strings.ReplaceAll(fullPath, ".tar.gz", ".tar")
	} else if strings.HasSuffix(fullPath, ".tar") {
		logger.Info(".tar file, skip unzip image archive")
	} else {
		return fmt.Errorf("unknown image archive type: %s, only .tar and .tar.gz are supported", fullPath)
	}

	logger.V(3).Infof("push %s to %s", fullPath, o.registry())
	logger.Info("waiting for push image")
	if err = client.Push(fullPath, o.registry(), o.TagSuffix); err != nil {
		return err
	}
	logger.Info("push image successful")

	return o.removePushImageArchive()
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

func (o *RegistryOptions) explicitRegistryDeploySources() []string {
	var sources []string
	if o.RegistryBinary != "" {
		sources = append(sources, "--registry-binary")
	}
	if o.RegistryImageArchive != "" {
		sources = append(sources, "--registry-image-archive")
	}
	if o.RegistryImage != "" {
		sources = append(sources, "--registry-image")
	}
	if o.OfflineBundle != "" {
		sources = append(sources, "--offline-bundle")
	}
	if o.PackageRegistry != "" {
		sources = append(sources, "--package-registry")
	}
	return sources
}

func (o *RegistryOptions) obtainRegistryBinary() (string, func(), error) {
	if o.OfflineBundle != "" {
		archivePath, archiveCleanup, err := extractRegistryArchiveFromOfflineBundle(o.OfflineBundle)
		if err != nil {
			return "", archiveCleanup, err
		}
		defer archiveCleanup()
		path, cleanup, err := o.obtainRegistryBinaryFromArchive(archivePath, packageRegistryBinaryPath)
		if err != nil {
			return "", cleanup, err
		}
		logger.Infof("extracted registry binary from offline bundle %s", o.OfflineBundle)
		return path, cleanup, nil
	}

	if o.RegistryBinary != "" {
		path, cleanup, err := normalizeRegistryBinary(o.RegistryBinary)
		if err != nil {
			return "", cleanup, err
		}
		logger.Infof("use local registry binary %s", o.RegistryBinary)
		return path, cleanup, nil
	}

	if o.RegistryImageArchive != "" {
		path, cleanup, err := o.obtainRegistryBinaryFromArchive(o.RegistryImageArchive, standardRegistryBinaryPath)
		if err != nil {
			return "", cleanup, err
		}
		logger.Infof("extracted registry binary from archive %s", o.RegistryImageArchive)
		return path, cleanup, nil
	}

	if o.RegistryImage != "" {
		path, cleanup, err := o.obtainRegistryBinaryFromImage(o.RegistryImage, standardRegistryBinaryPath)
		if err != nil {
			return "", cleanup, err
		}
		logger.Infof("extracted registry binary from image %s", o.RegistryImage)
		return path, cleanup, nil
	}

	ref, err := o.resolveRegistryImage()
	if err != nil {
		return "", nil, err
	}
	path, cleanup, err := o.obtainRegistryBinaryFromImage(ref, packageRegistryBinaryPath)
	if err != nil {
		return "", cleanup, err
	}
	logger.Infof("extracted registry binary from image %s", ref)
	return path, cleanup, nil
}

func (o *RegistryOptions) resolveRegistryImage() (string, error) {
	if o.RegistryImage != "" {
		return o.RegistryImage, nil
	}
	registry := strings.TrimRight(o.PackageRegistry, "/")
	if registry == "" {
		registry = defaultRegistryPackageRegistry
	}
	version := o.Version
	if version == "" {
		version = defaultRegistryVersion
	}
	return fmt.Sprintf("%s/%s/bootstrap/registry:%s", registry, "kubeclipper/packages", version), nil
}

func (o *RegistryOptions) obtainRegistryBinaryFromImage(ref, binaryPath string) (string, func(), error) {
	logger.Infof("pull registry image %s", ref)
	img, err := crane.Pull(ref, crane.Insecure, crane.WithPlatform(&containerv1.Platform{
		OS:           "linux",
		Architecture: o.Arch,
	}))
	if err != nil {
		return "", nil, err
	}
	return extractRegistryBinaryToTemp(img, binaryPath)
}

func (o *RegistryOptions) obtainRegistryBinaryFromArchive(archivePath, binaryPath string) (string, func(), error) {
	img, err := tarball.Image(dockerArchiveOpener(archivePath), nil)
	if err != nil {
		return "", nil, err
	}
	return extractRegistryBinaryToTemp(img, binaryPath)
}

func normalizeRegistryBinary(src string) (string, func(), error) {
	tmpDir, err := os.MkdirTemp("", "kc-registry-binary-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() {
		_ = os.RemoveAll(tmpDir)
	}
	dst := filepath.Join(tmpDir, "registry")
	if err = copyFile(src, dst, 0755); err != nil {
		cleanup()
		return "", nil, err
	}
	return dst, cleanup, nil
}

func extractRegistryBinaryToTemp(img containerv1.Image, binaryPath string) (string, func(), error) {
	tmpDir, err := os.MkdirTemp("", "kc-registry-image-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() {
		_ = os.RemoveAll(tmpDir)
	}
	dst := filepath.Join(tmpDir, "registry")
	if err = extractRegistryBinary(img, dst, binaryPath); err != nil {
		cleanup()
		return "", nil, err
	}
	return dst, cleanup, nil
}

func extractRegistryBinary(img containerv1.Image, dst, binaryPath string) error {
	rc := mutate.Extract(img)
	defer rc.Close()
	tr := tar.NewReader(rc)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("registry binary not found in image; expected %s", binaryPath)
		}
		if err != nil {
			return err
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		if !isRegistryBinaryPath(header.Name, binaryPath) {
			continue
		}
		mode := os.FileMode(header.Mode)
		if mode == 0 {
			mode = 0755
		}
		return writeReaderToFile(tr, dst, mode|0100)
	}
}

func isRegistryBinaryPath(name, expected string) bool {
	cleaned := "/" + strings.TrimPrefix(filepath.ToSlash(filepath.Clean(name)), "/")
	return cleaned == expected
}

func dockerArchiveOpener(archivePath string) tarball.Opener {
	return func() (io.ReadCloser, error) {
		file, err := os.Open(archivePath)
		if err != nil {
			return nil, err
		}
		if !strings.HasSuffix(archivePath, ".gz") {
			return file, nil
		}
		gzr, err := gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, err
		}
		return gzipReadCloser{Reader: gzr, closers: []io.Closer{gzr, file}}, nil
	}
}

func extractRegistryArchiveFromOfflineBundle(bundlePath string) (string, func(), error) {
	rc, err := dockerArchiveOpener(bundlePath)()
	if err != nil {
		return "", nil, err
	}
	defer rc.Close()

	tmpDir, err := os.MkdirTemp("", "kc-registry-offline-bundle-")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() { _ = os.RemoveAll(tmpDir) }
	archivePath := filepath.Join(tmpDir, "registry-image.tar")
	var checksumData []byte
	foundArchive := false

	tr := tar.NewReader(rc)
	for {
		header, nextErr := tr.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			cleanup()
			return "", nil, nextErr
		}
		name := strings.TrimPrefix(path.Clean(strings.TrimPrefix(header.Name, "./")), "/")
		switch name {
		case offlineBundleRegistryArchive:
			if header.Typeflag != tar.TypeReg && header.Typeflag != tar.TypeRegA {
				cleanup()
				return "", nil, fmt.Errorf("offline bundle registry archive is not a regular file")
			}
			if err = writeReaderToFile(tr, archivePath, 0600); err != nil {
				cleanup()
				return "", nil, err
			}
			foundArchive = true
		case offlineBundleChecksums:
			checksumData, err = io.ReadAll(tr)
			if err != nil {
				cleanup()
				return "", nil, err
			}
		}
	}
	if !foundArchive {
		cleanup()
		return "", nil, fmt.Errorf("offline bundle does not contain %s; regenerate it from a release manifest with bootstrap packages", offlineBundleRegistryArchive)
	}
	expected := checksumForBundleFile(checksumData, "bootstrap/registry-image.tar")
	if expected == "" {
		cleanup()
		return "", nil, fmt.Errorf("offline bundle checksum is missing for bootstrap/registry-image.tar")
	}
	data, err := os.ReadFile(archivePath)
	if err != nil {
		cleanup()
		return "", nil, err
	}
	actual := fmt.Sprintf("%x", sha256.Sum256(data))
	if actual != expected {
		cleanup()
		return "", nil, fmt.Errorf("offline bundle registry archive checksum mismatch: got %s, want %s", actual, expected)
	}
	return archivePath, cleanup, nil
}

func checksumForBundleFile(data []byte, name string) string {
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if strings.TrimPrefix(fields[1], "./") == name {
			return strings.ToLower(fields[0])
		}
	}
	return ""
}

type gzipReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (r gzipReadCloser) Close() error {
	var closeErr error
	for _, closer := range r.closers {
		if err := closer.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func copyFile(src, dst string, mode os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	return writeReaderToFile(in, dst, mode)
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

func (o *RegistryOptions) removePushImageArchive() error {
	hook := fmt.Sprintf(`rm -rf %s/kc %s %s`,
		config.DefaultPkgPath,
		path.Join(config.DefaultPkgPath, o.ImageArchive),
		strings.Replace(path.Join(config.DefaultPkgPath, o.ImageArchive), ".tar.gz", ".tar", 1))
	ret, err := sshutils.RunCmdAsSSH(hook)
	if err != nil {
		return err
	}
	if err = ret.Error(); err != nil {
		return err
	}
	logger.Info("remove image archive successfully")
	return nil
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
