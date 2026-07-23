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

package join

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"gopkg.in/yaml.v2"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/pkg/cli/deploy"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"

	"github.com/kubeclipper/kubeclipper/pkg/utils/autodetection"

	"github.com/kubeclipper/kubeclipper/pkg/cli/config"
	"github.com/kubeclipper/kubeclipper/pkg/cli/sudo"
	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/logger"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
)

const (
	longDescription = `
  Add Server and Agents nodes on kubeclipper platform.

  At least one Server node must be installed before adding an Agents node.`
	joinExample = `
  # Add agent node.
  kcctl join --agent 192.168.10.123 --package-registry registry.local:5000

  # Add agent node specify region.
  kcctl join --agent us-west-1:192.168.10.123 --package-registry registry.local:5000

  # Add multiple agent nodes.
  kcctl join --agent 192.168.10.123,192.168.10.124 --package-registry registry.local:5000

  # Add multiple agent nodes in same region.
  kcctl join --agent us-west-1:192.168.10.123,192.168.10.124 --package-registry registry.local:5000

  # Add multiple agent nodes node in different region
  kcctl join --agent us-west-1:1.2.3.4 --agent us-west-2:2.3.4.5 --package-registry registry.local:5000

  # add multiple agent nodes which has orderly ip.
  # this will add 10 agent,1.1.1.1, 1.1.1.2, ... 1.1.1.10.
  kcctl join --agent us-west-1:1.1.1.1-1.1.1.10 --package-registry registry.local:5000

  # Add multiple agent nodes and config float ip.
  kcctl join --agent 192.168.10.123,192.168.10.124 --float-ip 192.168.10.123:172.20.149.199 --float-ip 192.168.10.124:172.20.149.200 --package-registry registry.local:5000

  # Add agent nodes use config file. join config example:
ssh:
  user: root
  password: "0000"
  pkFile: ""
  privateKey: ""
  pkPassword: ""
  port: 22
  connectionTimeout: 1m0s
# Optional transport used only to read certificates from the existing server.
# Omit it when server and agent nodes share the deploy SSH settings.
serverSSH:
  user: root
  pkFile: "/root/.ssh/server_id_rsa"
  port: 2202
#	MethodFirst     = "first-found"
#	MethodInterface = "interface="
#	MethodCidr      = "cidr="
#	MethodCanReach  = "can-reach="
ipDetect: first-found
nodeIPDetect: first-found
packageRegistry: registry.local:5000
agents:
  192.168.234.41:
    #region: default
    #floatIP:
  192.168.234.42:
    #region: default2
    #floatIP:
  kcctl join --join-config join-config.yaml
  Please read 'kcctl join -h' get more deploy flags`
)

type JoinOptions struct {
	options.IOStreams
	deployConfig         *options.DeployConfig
	cliOpts              *options.CliOptions
	client               *kc.Client
	serverSourceRevision string

	agents       []string // user input agents,maybe with region,need to parse.
	floatIPs     []string // format: ip:floatIP,e.g. 192.168.10.11:172.20.149.199
	ipDetect     string
	nodeIPDetect string
	parseAgent   options.Agents

	sshConfig       *sshutils.SSH
	serverSSHConfig *sshutils.SSH
	sshRunner       func(*sshutils.SSH, string, string) (sshutils.Result, error)

	joinConfigPath string

	PackageRegistry string `json:"packageRegistry" yaml:"packageRegistry,omitempty"`

	packageRegistryFiles      deliveryregistry.FileOptions
	packageRegistryConfig     *deliveryregistry.Config
	packageRegistryConfigPath string
	rotatePackageRegistry     bool
	deployedPackageRegistry   string
	packageRegistryDownload   func(*sshutils.SSH, string, string, string) error
	packageRegistryCopy       func(*sshutils.SSH, string, string, string) error
	packageRegistryExists     func(*sshutils.SSH, string, string) (bool, error)
	packageRegistryRemove     func(*sshutils.SSH, string, string) error
	certDownload              func(*sshutils.SSH, string, string, string) error
	certCopy                  func(*sshutils.SSH, string, []string, string) error
}

type JoinConfig struct {
	Agents          options.Agents `json:"agents,omitempty" yaml:"agents,omitempty"`
	IPDetect        string         `json:"ipDetect,omitempty" yaml:"ipDetect,omitempty"`
	NodeIPDetect    string         `json:"nodeIPDetect,omitempty" yaml:"nodeIPDetect,omitempty"`
	PackageRegistry string         `json:"packageRegistry,omitempty" yaml:"packageRegistry,omitempty"`
	SSHConfig       *sshutils.SSH  `json:"ssh,omitempty" yaml:"ssh,omitempty"`
	ServerSSHConfig *sshutils.SSH  `json:"serverSSH,omitempty" yaml:"serverSSH,omitempty"`
}

func NewJoinOptions(streams options.IOStreams) *JoinOptions {
	return &JoinOptions{
		cliOpts:         options.NewCliOptions(),
		IOStreams:       streams,
		deployConfig:    options.NewDeployOptions(),
		ipDetect:        autodetection.MethodFirst,
		sshConfig:       sshutils.NewSSH(),
		serverSSHConfig: &sshutils.SSH{},
		sshRunner:       sshutils.SSHCmdWithSudo,
		packageRegistryDownload: func(sshConfig *sshutils.SSH, host, localPath, remotePath string) error {
			return sshConfig.DownloadSudo(host, localPath, remotePath)
		},
		packageRegistryCopy: func(sshConfig *sshutils.SSH, host, localPath, remotePath string) error {
			return sshConfig.CopySudoMode(host, localPath, remotePath, deliveryregistry.PrivateFileMode)
		},
		packageRegistryExists: remotePackageRegistryConfigExists,
		packageRegistryRemove: remotePackageRegistryConfigRemove,
		certDownload: func(sshConfig *sshutils.SSH, host, localPath, remotePath string) error {
			return sshConfig.DownloadSudo(host, localPath, remotePath)
		},
		certCopy: func(sshConfig *sshutils.SSH, localPath string, hosts []string, remoteDir string) error {
			return utils.SendPackageV2(sshConfig, localPath, hosts, remoteDir, nil, nil)
		},
	}
}

func remotePackageRegistryConfigExists(sshConfig *sshutils.SSH, host, remotePath string) (bool, error) {
	result, err := sshutils.SSHCmdWithSudo(sshConfig, host, fmt.Sprintf(
		"if test -e %s; then echo present; else echo absent; fi", remotePath,
	))
	if err != nil {
		return false, err
	}
	if resultErr := result.Error(); resultErr != nil {
		return false, resultErr
	}
	switch strings.TrimSpace(result.Stdout) {
	case "present":
		return true, nil
	case "absent":
		return false, nil
	default:
		return false, fmt.Errorf("unexpected package registry config existence result from %s: %q", host, result.Stdout)
	}
}

func remotePackageRegistryConfigRemove(sshConfig *sshutils.SSH, host, remotePath string) error {
	result, err := sshutils.SSHCmdWithSudo(sshConfig, host, "rm -f "+remotePath)
	if err != nil {
		return err
	}
	return result.Error()
}

func NewCmdJoin(streams options.IOStreams) *cobra.Command {
	o := NewJoinOptions(streams)
	cmd := &cobra.Command{
		Use:                   "join [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "Join kubeclipper agent node",
		Long:                  longDescription,
		Example:               joinExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete())
			utils.CheckErr(o.ValidateArgs(cmd))
			if !o.preCheck() {
				return
			}
			utils.CheckErr(o.RunJoinFunc())
		},
	}
	o.cliOpts.AddFlags(cmd.Flags())
	cmd.Flags().StringVar(&o.ipDetect, "ip-detect", o.ipDetect, fmt.Sprintf("Kc agent node ip detect method. Used to route between nodes. \n%s", options.IPDetectDescription))
	cmd.Flags().StringVar(&o.nodeIPDetect, "node-ip-detect", o.nodeIPDetect, fmt.Sprintf("Kc agent node ip detect method. Used for routing between nodes in the kubernetes cluster. If not specified, ip-detect is inherited. \n%s", options.IPDetectDescription))
	cmd.Flags().StringArrayVar(&o.agents, "agent", o.agents, "join agent node.")
	cmd.Flags().StringArrayVar(&o.floatIPs, "float-ip", o.floatIPs, "Kc agent ip and float ip.")
	cmd.Flags().StringVar(&o.PackageRegistry, "package-registry", o.PackageRegistry, "OCI registry host:port for KubeClipper offline packages. Default is inherited from the deploy config.")
	addPackageRegistryClientFlags(cmd, &o.packageRegistryFiles)
	cmd.Flags().StringVar(&o.joinConfigPath, "join-config", "", "path to the join config file to use for join")

	options.AddFlagsToSSH(o.sshConfig, cmd.Flags())
	cmd.Flags().StringVar(&o.serverSSHConfig.User, "server-ssh-user", "", "SSH user for reading certificates from the server node")
	cmd.Flags().StringVar(&o.serverSSHConfig.Password, "server-ssh-password", "", "SSH password for the server node")
	cmd.Flags().StringVar(&o.serverSSHConfig.PkFile, "server-ssh-pk-file", "", "SSH private key file for the server node")
	cmd.Flags().StringVar(&o.serverSSHConfig.PkPassword, "server-ssh-pk-password", "", "SSH private key password for the server node")
	cmd.Flags().IntVar(&o.serverSSHConfig.Port, "server-ssh-port", 0, "SSH port for the server node (defaults to deploy SSH config)")
	return cmd
}

func addPackageRegistryClientFlags(cmd *cobra.Command, opts *deliveryregistry.FileOptions) {
	cmd.Flags().StringVar(&opts.Scheme, "package-registry-scheme", opts.Scheme,
		"Package Registry transport scheme: https or http (default inherited from the server)")
	cmd.Flags().StringVar(&opts.Username, "package-registry-username", opts.Username, "Package Registry username or robot account")
	cmd.Flags().StringVar(&opts.PasswordFile, "package-registry-password-file", opts.PasswordFile,
		"File containing the Package Registry password or token")
	cmd.Flags().StringVar(&opts.CAFile, "package-registry-ca-file", opts.CAFile, "PEM CA file used to verify the Package Registry")
	cmd.Flags().BoolVar(&opts.SkipTLSVerify, "package-registry-skip-tls-verify", opts.SkipTLSVerify,
		"Skip Package Registry TLS verification (not recommended)")
}

func readJoinConfig(path string) (*JoinConfig, error) {
	joinConfig := &JoinConfig{}
	fData, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(fData, joinConfig)
	return joinConfig, err
}

func (c *JoinOptions) Complete() error {
	var err error
	if c.joinConfigPath != "" {
		joinConfig, err := readJoinConfig(c.joinConfigPath)
		if err != nil {
			return err
		}
		if len(joinConfig.Agents) == 0 {
			return fmt.Errorf("join-config must specify at least one agent node")
		}
		if joinConfig.Agents != nil {
			c.parseAgent = joinConfig.Agents
		}
		if joinConfig.IPDetect != "" {
			c.ipDetect = joinConfig.IPDetect
		}
		if joinConfig.NodeIPDetect != "" {
			c.nodeIPDetect = joinConfig.NodeIPDetect
		}
		if joinConfig.PackageRegistry != "" {
			c.PackageRegistry = joinConfig.PackageRegistry
		}
		if joinConfig.SSHConfig != nil {
			c.sshConfig = joinConfig.SSHConfig
		}
		if joinConfig.ServerSSHConfig != nil {
			c.serverSSHConfig = joinConfig.ServerSSHConfig
		}
	} else {
		if c.parseAgent, err = deploy.BuildAgent(c.agents, c.floatIPs, c.deployConfig.DefaultRegion); err != nil {
			return err
		}
	}

	// config Complete
	if err = c.cliOpts.Complete(); err != nil {
		return err
	}
	c.client, err = kc.FromConfig(c.cliOpts.ToRawConfig())
	if err != nil {
		return err
	}
	// deploy config Complete
	c.deployConfig, err = deploy.GetDeployConfig(context.Background(), c.client, true)
	if err != nil {
		return errors.WithMessage(err, "get online deploy-config failed")
	}
	serverVersion, err := c.client.Version(context.Background())
	if err != nil {
		return errors.WithMessage(err, "get kubeclipper server version failed")
	}
	c.serverSourceRevision = strings.TrimSpace(serverVersion.GitCommit)
	if c.serverSourceRevision == "" {
		return errors.New("kubeclipper server source revision is empty")
	}
	deployedPackageRegistry := strings.TrimRight(strings.TrimSpace(c.deployConfig.PackageRegistry), "/")
	if strings.TrimSpace(c.PackageRegistry) != "" {
		c.deployConfig.PackageRegistry = c.PackageRegistry
	}
	c.deployConfig.PackageRegistry = strings.TrimRight(strings.TrimSpace(c.deployConfig.PackageRegistry), "/")
	c.rotatePackageRegistry = c.packageRegistryFiles.Specified() ||
		c.deployConfig.PackageRegistry != deployedPackageRegistry
	c.deployedPackageRegistry = deployedPackageRegistry
	// overwrite by specify
	if c.ipDetect != "" {
		c.deployConfig.IPDetect = c.ipDetect
	}
	if c.nodeIPDetect == "" {
		logger.Infof("node-ip-detect inherits from ip-detect: %s", c.ipDetect)
	} else {
		c.deployConfig.NodeIPDetect = c.nodeIPDetect
	}
	// When joining agent nodes, the corresponding pk-file or password is not saved,
	// which causes operational complexity but avoids problems caused by different pk-file or password
	// between nodes
	if c.sshConfig != nil {
		if c.sshConfig.Port == 0 {
			c.sshConfig.Port = sshutils.NewSSH().Port
		}
		if c.sshConfig.User == "" {
			c.sshConfig.User = sshutils.NewSSH().User
		}
	} else {
		c.sshConfig = c.deployConfig.SSHConfig
	}
	c.serverSSHConfig = completeServerSSHConfig(c.serverSSHConfig, c.deployConfig.SSHConfig)
	return nil
}

func completeServerSSHConfig(server, fallback *sshutils.SSH) *sshutils.SSH {
	if server == nil {
		server = &sshutils.SSH{}
	}
	if fallback == nil {
		fallback = sshutils.NewSSH()
	}
	completed := *server
	if completed.User == "" {
		completed.User = fallback.User
	}
	if completed.Password == "" {
		completed.Password = fallback.Password
	}
	if completed.Port == 0 {
		completed.Port = fallback.Port
	}
	if completed.PkFile == "" && completed.PrivateKey == "" {
		completed.PkFile, completed.PrivateKey = fallback.PkFile, fallback.PrivateKey
	}
	if completed.PkPassword == "" {
		completed.PkPassword = fallback.PkPassword
	}
	if completed.ConnectionTimeout == nil {
		completed.ConnectionTimeout = fallback.ConnectionTimeout
	}
	return &completed
}

func (c *JoinOptions) preCheck() bool {
	if !sudo.PreCheck("sudo", c.sshConfig, c.IOStreams, c.parseAgent.ListIP()) {
		return false
	}
	if !sudo.PreCheck("server sudo", c.serverSSHConfig, c.IOStreams, c.deployConfig.ServerIPs) {
		return false
	}
	// check if the node is already added
	for _, agent := range c.parseAgent.ListIP() {
		if !c.preCheckKcAgent(agent) {
			return false
		}
	}
	return sudo.MultiNIC("ipDetect", c.sshConfig, c.IOStreams, c.parseAgent.ListIP(), c.ipDetect)
}

func (c *JoinOptions) ValidateArgs(cmd *cobra.Command) error {
	if c.sshConfig.PkFile == "" && c.sshConfig.Password == "" {
		return utils.UsageErrorf(cmd, "one of pkfile or password must be specified, please config it in %s", c.deployConfig.Config)
	}
	if c.ipDetect != "" && !autodetection.CheckMethod(c.ipDetect) {
		return utils.UsageErrorf(cmd, "invalid ip detect method, support [first-found,interface=xxx,cidr=xxx] now")
	}
	if c.nodeIPDetect != "" && !autodetection.CheckMethod(c.nodeIPDetect) {
		return utils.UsageErrorf(cmd, "invalid node ip detect method, support [first-found,interface=xxx,cidr=xxx] now")
	}
	if len(c.parseAgent) == 0 {
		return utils.UsageErrorf(cmd, "must specified at least one agent node")
	}
	if len(c.deployConfig.ServerIPs) == 0 {
		logger.Error("join an agent node requires specifying at least one server node")
		logger.Info("example: kcctl join --agent 172.10.10.20 --server 172.10.10.10")
		return utils.UsageErrorf(cmd, "join an agent node requires specifying at least one server node")
	}
	if strings.TrimSpace(c.deployConfig.PackageRegistry) == "" {
		return utils.UsageErrorf(cmd, "join an agent node requires packageRegistry in deploy-config")
	}
	return nil
}

func (c *JoinOptions) RunJoinFunc() error {
	err := c.RunJoinNode()
	if err != nil {
		return err
	}

	return nil
}

func (c *JoinOptions) RunJoinNode() error {
	cleanup, err := c.preparePackageRegistryConfig()
	if err != nil {
		return err
	}
	defer cleanup()
	if err := c.runJoinServerNode(); err != nil {
		return fmt.Errorf("join server node failed: %s", err.Error())
	}

	if err := c.runJoinAgentNode(); err != nil {
		return fmt.Errorf("join agent node failed: %s", err.Error())
	}
	if c.rotatePackageRegistry {
		if err := c.updateExistingPackageRegistryConfigs(func() error {
			return deploy.UpdateDeployConfig(context.Background(), c.client, c.deployConfig, true)
		}); err != nil {
			return c.failJoinWithRollbackAndConfig(errors.Wrap(err, "update existing package registry config"))
		}
	}

	return nil
}

func (c *JoinOptions) preparePackageRegistryConfig() (func(), error) {
	if c.rotatePackageRegistry {
		registryConfig, err := c.packageRegistryFiles.Resolve(c.deployConfig.PackageRegistry)
		if err != nil {
			return func() {}, err
		}
		path, cleanup, err := writeTemporaryPackageRegistryConfig(registryConfig)
		if err != nil {
			return func() {}, err
		}
		c.packageRegistryConfig, c.packageRegistryConfigPath = registryConfig, path
		return cleanup, nil
	}

	file, err := os.CreateTemp("", "kc-package-registry-*.json")
	if err != nil {
		return func() {}, err
	}
	path := file.Name()
	if err = file.Close(); err != nil {
		_ = os.Remove(path)
		return func() {}, err
	}
	cleanup := func() { _ = os.Remove(path) }
	if err = c.packageRegistryDownload(c.serverSSHConfig, c.deployConfig.ServerIPs[0], path, deliveryregistry.ServerConfigPath); err != nil {
		cleanup()
		return func() {}, errors.Wrap(err,
			"inherit package registry credentials from server; provide package registry TLS/auth flags when joining an older deployment")
	}
	if err = os.Chmod(path, deliveryregistry.PrivateFileMode); err != nil {
		cleanup()
		return func() {}, err
	}
	registryConfig, err := deliveryregistry.Load(path)
	if err != nil {
		cleanup()
		return func() {}, err
	}
	if registryConfig.Registry != strings.TrimRight(strings.TrimSpace(c.deployConfig.PackageRegistry), "/") {
		cleanup()
		return func() {}, fmt.Errorf(
			"server package registry credentials are for %s, not %s",
			registryConfig.Registry, c.deployConfig.PackageRegistry,
		)
	}
	c.packageRegistryConfig, c.packageRegistryConfigPath = registryConfig, path
	return cleanup, nil
}

func writeTemporaryPackageRegistryConfig(registryConfig *deliveryregistry.Config) (path string, cleanup func(), err error) {
	dir, err := os.MkdirTemp("", "kc-package-registry-")
	if err != nil {
		return "", func() {}, err
	}
	cleanup = func() { _ = os.RemoveAll(dir) }
	path = filepath.Join(dir, "package-registry.json")
	if err = deliveryregistry.Write(path, registryConfig); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return path, cleanup, nil
}

func (c *JoinOptions) copyPackageRegistryConfig(sshConfig *sshutils.SSH, hosts []string, remotePath string) error {
	for _, host := range hosts {
		if err := c.packageRegistryCopy(sshConfig, host, c.packageRegistryConfigPath, remotePath); err != nil {
			return fmt.Errorf("copy package registry config to %s: %w", host, err)
		}
	}
	return nil
}

type packageRegistryTarget struct {
	host       string
	remotePath string
	sshConfig  *sshutils.SSH
}

func (c *JoinOptions) existingPackageRegistryTargets() []packageRegistryTarget {
	servers := make(map[string]struct{}, len(c.deployConfig.ServerIPs))
	targets := make([]packageRegistryTarget, 0, len(c.deployConfig.ServerIPs)+len(c.deployConfig.Agents))
	for _, host := range c.deployConfig.ServerIPs {
		servers[host] = struct{}{}
		targets = append(targets, packageRegistryTarget{
			host: host, remotePath: deliveryregistry.ServerConfigPath, sshConfig: c.serverSSHConfig,
		})
	}
	for _, host := range c.deployConfig.Agents.ListIP() {
		if c.parseAgent.Exists(host) {
			continue
		}
		sshConfig := c.deployConfig.AgentSSHConfig
		if _, serverLocal := servers[host]; serverLocal || sshConfig == nil {
			sshConfig = c.serverSSHConfig
		}
		targets = append(targets, packageRegistryTarget{
			host: host, remotePath: deliveryregistry.AgentConfigPath, sshConfig: sshConfig,
		})
	}
	return targets
}

func (c *JoinOptions) updateExistingPackageRegistryConfigs(commit func() error) error {
	backupDir, err := os.MkdirTemp("", "kc-package-registry-backup-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(backupDir)

	targets := c.existingPackageRegistryTargets()
	backups := make([]string, len(targets))
	for index, target := range targets {
		backupPath := filepath.Join(backupDir, fmt.Sprintf("target-%d.json", index))
		if err = c.packageRegistryDownload(target.sshConfig, target.host, backupPath, target.remotePath); err != nil {
			exists, existsErr := c.packageRegistryExists(target.sshConfig, target.host, target.remotePath)
			if existsErr != nil {
				return fmt.Errorf("backup package registry config %s from %s failed (%s); existence check failed: %w",
					target.remotePath, target.host, err.Error(), existsErr)
			}
			if exists {
				return fmt.Errorf("backup package registry config %s from %s: %w", target.remotePath, target.host, err)
			}
			continue
		}
		if chmodErr := os.Chmod(backupPath, deliveryregistry.PrivateFileMode); chmodErr != nil {
			return chmodErr
		}
		backups[index] = backupPath
	}

	updated := 0
	for index, target := range targets {
		if err = c.packageRegistryCopy(target.sshConfig, target.host, c.packageRegistryConfigPath, target.remotePath); err != nil {
			rollbackErr := c.restorePackageRegistryConfigs(targets[:updated], backups[:updated])
			if rollbackErr != nil {
				return fmt.Errorf("update %s on %s failed (%s); rollback failed: %w", target.remotePath, target.host, err.Error(), rollbackErr)
			}
			return fmt.Errorf("update %s on %s: %w; earlier targets were restored", target.remotePath, target.host, err)
		}
		updated = index + 1
	}
	if err = commit(); err != nil {
		rollbackErr := c.restorePackageRegistryConfigs(targets, backups)
		if rollbackErr != nil {
			return fmt.Errorf("persist package registry config failed (%s); rollback failed: %w", err.Error(), rollbackErr)
		}
		return fmt.Errorf("persist package registry config: %w; node configs were restored", err)
	}
	return nil
}

func (c *JoinOptions) restorePackageRegistryConfigs(targets []packageRegistryTarget, backups []string) error {
	var failures []string
	for index := len(targets); index > 0; {
		index--
		target := targets[index]
		var err error
		if backups[index] == "" {
			err = c.packageRegistryRemove(target.sshConfig, target.host, target.remotePath)
		} else {
			err = c.packageRegistryCopy(target.sshConfig, target.host, backups[index], target.remotePath)
		}
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s %s: %v", target.host, target.remotePath, err))
		}
	}
	if len(failures) > 0 {
		return errors.New(strings.Join(failures, "; "))
	}
	return nil
}

func (c *JoinOptions) runJoinAgentNode() error {
	for ip := range c.parseAgent {
		metadata := c.parseAgent[ip]
		metadata.AgentID = uuid.New().String()
		c.parseAgent[ip] = metadata
		if c.deployConfig.Agents == nil {
			c.deployConfig.Agents = make(options.Agents)
		}
		c.deployConfig.Agents.Add(ip, metadata)
	}
	// Persist the cleanup inventory before installing anything. If the kcctl
	// process is interrupted after an agent starts, force-clean still knows
	// both the host and the transport that were used to reach it.
	c.deployConfig.AgentSSHConfig = agentSSHConfigForPersistence(c.sshConfig)
	packageRegistry := c.deployConfig.PackageRegistry
	if c.rotatePackageRegistry {
		c.deployConfig.PackageRegistry = c.deployedPackageRegistry
	}
	if err := deploy.UpdateDeployConfig(context.Background(), c.client, c.deployConfig, true); err != nil {
		c.deployConfig.PackageRegistry = packageRegistry
		return errors.Wrap(err, "persist planned agents in deploy config failed")
	}
	c.deployConfig.PackageRegistry = packageRegistry

	for ip, metadata := range c.parseAgent {
		if err := c.agentNodeFiles(ip, metadata); err != nil {
			return c.failJoinWithRollbackAndConfig(err)
		}
		if err := c.enableAgent(ip, metadata); err != nil {
			return c.failJoinWithRollbackAndConfig(err)
		}
	}
	logger.Info("agent node join completed. show command: 'kcctl get node'")
	return nil
}

func agentSSHConfigForPersistence(source *sshutils.SSH) *sshutils.SSH {
	if source == nil {
		return nil
	}
	persisted := *source
	// SSH.connect caches the contents of PkFile in PrivateKey. Retain the
	// reusable path for cleanup without copying that private key into the
	// deploy ConfigMap and local deploy-config.
	if persisted.PkFile != "" {
		persisted.PrivateKey = ""
	}
	return &persisted
}

func (c *JoinOptions) failJoinWithRollbackAndConfig(joinErr error) error {
	rollbackErr := c.rollbackJoinedAgents()
	for ip := range c.parseAgent {
		delete(c.deployConfig.Agents, ip)
	}
	if c.rotatePackageRegistry {
		c.deployConfig.PackageRegistry = c.deployedPackageRegistry
	}
	persistErr := deploy.UpdateDeployConfig(context.Background(), c.client, c.deployConfig, true)
	if rollbackErr != nil || persistErr != nil {
		return errors.Wrapf(joinErr, "join agent failed; rollback error: %v; cleanup inventory update error: %v", rollbackErr, persistErr)
	}
	return errors.Wrap(joinErr, "join agent failed; partially installed agents were rolled back")
}

func (c *JoinOptions) failJoinWithRollback(joinErr error) error {
	if rollbackErr := c.rollbackJoinedAgents(); rollbackErr != nil {
		return errors.Wrapf(joinErr, "join agent failed; rollback also failed: %v", rollbackErr)
	}
	return errors.Wrap(joinErr, "join agent failed; partially installed agents were rolled back")
}

func (c *JoinOptions) rollbackJoinedAgents() error {
	command := "systemctl disable kc-agent --now || true; rm -rf /usr/lib/systemd/system/kc-agent.service /etc/kubeclipper-agent /usr/local/bin/kubeclipper-agent; systemctl daemon-reload; systemctl reset-failed kc-agent || true"
	var rollbackErrors []string
	for _, ip := range c.parseAgent.ListIP() {
		result, err := c.sshRunner(c.sshConfig, ip, command)
		if err != nil {
			rollbackErrors = append(rollbackErrors, fmt.Sprintf("%s: %v", ip, err))
			continue
		}
		if err := result.Error(); err != nil {
			rollbackErrors = append(rollbackErrors, fmt.Sprintf("%s: %v", ip, err))
		}
	}
	if len(rollbackErrors) > 0 {
		return errors.New(strings.Join(rollbackErrors, "; "))
	}
	return nil
}

func (c *JoinOptions) preCheckKcAgent(ip string) bool {
	// check if the node is already in deploy config
	if c.deployConfig.Agents.Exists(ip) {
		logger.Errorf("node %s is already deployed", ip)
		return false
	}
	// check if kc-agent is running
	ret, err := sshutils.SSHCmdWithSudo(c.sshConfig, ip, "systemctl --all --type service | grep kc-agent | wc -l")
	logger.V(2).Info(ret.String())
	if err != nil {
		logger.Errorf("check node %s failed: %s", ip, err.Error())
		return false
	}
	if ret.StdoutToString("") != "0" {
		logger.Errorf("kc-agent service exist on %s, please clean old environment", ip)
		return false
	}
	return true
}

func (c *JoinOptions) agentNodeFiles(node string, metadata options.Metadata) error {
	if err := deploy.InstallBootstrapAssetsFromRegistry(context.Background(), deploy.BootstrapInstallOptions{
		Registry:                  c.deployConfig.PackageRegistry,
		RegistryConfig:            c.packageRegistryConfig,
		SSH:                       c.sshConfig,
		Hosts:                     []string{node},
		NeedAgent:                 false,
		KubeClipperSourceRevision: c.serverSourceRevision,
	}); err != nil {
		return errors.Wrap(err, "install bootstrap agent from registry")
	}
	if err := c.copyPackageRegistryConfig(c.sshConfig, []string{node}, deliveryregistry.AgentConfigPath); err != nil {
		return err
	}
	err := c.sendCerts(node)
	if err != nil {
		return err
	}
	agentConfig := c.getKcAgentConfigTemplateContent(metadata)
	cmdList := []string{
		sshutils.WrapEcho(config.KcAgentService, "/usr/lib/systemd/system/kc-agent.service"), // write systemd file
		"mkdir -pv /etc/kubeclipper-agent ",
		sshutils.WrapEcho(agentConfig, "/etc/kubeclipper-agent/kubeclipper-agent.yaml"), // write agent.yaml
	}
	for _, cmd := range cmdList {
		ret, err := sshutils.SSHCmdWithSudo(c.sshConfig, node, cmd)
		if err != nil {
			return err
		}
		if err = ret.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (c *JoinOptions) enableAgent(node string, metadata options.Metadata) error {
	// enable agent service
	ret, err := sshutils.SSHCmdWithSudo(c.sshConfig, node, "systemctl daemon-reload && systemctl enable kc-agent --now")
	if err != nil {
		return errors.Wrap(err, "enable kc agent")
	}
	if err = ret.Error(); err != nil {
		return errors.Wrap(err, "enable kc agent")
	}
	// add deploy-config
	if c.deployConfig.Agents == nil {
		c.deployConfig.Agents = make(options.Agents)
	}
	c.deployConfig.Agents.Add(node, metadata)
	return nil
}

func (c *JoinOptions) runJoinServerNode() error {
	for _, node := range c.deployConfig.ServerIPs {
		if err := c.checkServerNode(node); err != nil {
			return err
		}
	}
	return nil
}

func (c *JoinOptions) checkServerNode(node string) error {
	// TODO: implement server node pre-check validation
	return nil
}

func (c *JoinOptions) getKcAgentConfigTemplateContent(metadata options.Metadata) string {
	tmpl, err := template.New("text").Parse(config.KcAgentConfigTmpl)
	if err != nil {
		logger.Fatalf("template parse failed: %s", err.Error())
	}

	var data = make(map[string]interface{})
	data["Region"] = metadata.Region
	data["FloatIP"] = metadata.FloatIP
	data["IPDetect"] = c.deployConfig.IPDetect
	data["NodeIPDetect"] = c.deployConfig.NodeIPDetect
	data["AgentID"] = metadata.AgentID
	if c.deployConfig.Debug {
		data["LogLevel"] = "debug"
	} else {
		data["LogLevel"] = "info"
	}
	var endpoint []string
	for _, v := range c.deployConfig.MQ.IPs {
		endpoint = append(endpoint, fmt.Sprintf("%s:%d", v, c.deployConfig.MQ.Port))
	}
	data["MQServerEndpoints"] = endpoint
	data["MQExternal"] = c.deployConfig.MQ.External
	data["MQUser"] = c.deployConfig.MQ.User
	data["MQAuthToken"] = c.deployConfig.MQ.Secret
	data["MQTLS"] = c.deployConfig.MQ.TLS
	if c.deployConfig.MQ.TLS {
		if c.deployConfig.MQ.External {
			data["MQCaPath"] = c.deployConfig.MQ.CA
			data["MQClientCertPath"] = c.deployConfig.MQ.ClientCert
			data["MQClientKeyPath"] = c.deployConfig.MQ.ClientKey
		} else {
			data["MQCaPath"] = filepath.Join(options.DefaultKcAgentConfigPath, options.DefaultCaPath, filepath.Base(c.deployConfig.MQ.CA))
			data["MQClientCertPath"] = filepath.Join(options.DefaultKcAgentConfigPath, options.DefaultNatsPKIPath, filepath.Base(c.deployConfig.MQ.ClientCert))
			data["MQClientKeyPath"] = filepath.Join(options.DefaultKcAgentConfigPath, options.DefaultNatsPKIPath, filepath.Base(c.deployConfig.MQ.ClientKey))
		}
	}
	data["OpLogDir"] = c.deployConfig.OpLog.Dir
	data["OpLogThreshold"] = c.deployConfig.OpLog.Threshold
	data["KcImageRepoMirror"] = c.deployConfig.ImageProxy.KcImageRepoMirror
	var buffer bytes.Buffer
	if err = tmpl.Execute(&buffer, data); err != nil {
		logger.Fatalf("template execute failed: %s", err.Error())
	}
	return buffer.String()
}

func (c *JoinOptions) sendCerts(ip string) error {
	if !c.deployConfig.MQ.TLS {
		return nil
	}

	dir, err := os.MkdirTemp("", "kc-join-certs-*")
	if err != nil {
		return errors.WithMessage(err, "create temporary certificate directory")
	}
	defer func() { _ = os.RemoveAll(dir) }()

	remoteFiles := []string{
		c.deployConfig.MQ.CA,
		c.deployConfig.MQ.ClientCert,
		c.deployConfig.MQ.ClientKey,
	}
	localFiles := make([]string, 0, len(remoteFiles))
	for i, remotePath := range remoteFiles {
		if strings.TrimSpace(remotePath) == "" {
			return errors.New("MQ TLS certificate path must not be empty")
		}
		fileDir := filepath.Join(dir, fmt.Sprintf("%d", i))
		if err = os.Mkdir(fileDir, deliveryregistry.PrivateDirMode); err != nil {
			return errors.WithMessage(err, "create temporary certificate file directory")
		}
		localPath := filepath.Join(fileDir, filepath.Base(remotePath))
		if err = c.certDownload(c.serverSSHConfig, c.deployConfig.ServerIPs[0], localPath, remotePath); err != nil {
			return errors.WithMessagef(err, "download certificate %s from server", remotePath)
		}
		if err = os.Chmod(localPath, deliveryregistry.PrivateFileMode); err != nil {
			return errors.WithMessagef(err, "secure downloaded certificate %s", remotePath)
		}
		localFiles = append(localFiles, localPath)
	}

	destCa := filepath.Join(options.DefaultKcAgentConfigPath, options.DefaultCaPath)
	destCert := filepath.Join(options.DefaultKcAgentConfigPath, options.DefaultNatsPKIPath)
	destKey := filepath.Join(options.DefaultKcAgentConfigPath, options.DefaultNatsPKIPath)
	if c.deployConfig.MQ.External {
		destCa = filepath.Dir(c.deployConfig.MQ.CA)
		destCert = filepath.Dir(c.deployConfig.MQ.ClientCert)
		destKey = filepath.Dir(c.deployConfig.MQ.ClientKey)
	}

	for i, destDir := range []string{destCa, destCert, destKey} {
		if err := c.certCopy(c.sshConfig, localFiles[i], []string{ip}, destDir); err != nil {
			return err
		}
	}
	return nil
}
