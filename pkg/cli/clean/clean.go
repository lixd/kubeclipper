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

package clean

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/pkg/errors"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubeclipper/kubeclipper/pkg/cli/deploy"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"

	"github.com/kubeclipper/kubeclipper/pkg/cli/sudo"
	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"

	"github.com/kubeclipper/kubeclipper/pkg/cli/logger"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
)

const (
	longDescription = `
  Uninstall kubeclipper Platform .

  Uninstall all kubeclipper plug-ins.`
	cleanExample = `
  # Uninstall the entire kubeclipper platform.
  kcctl clean --all
  kcctl clean -A

  # Mock uninstall,without -A flag will only do preCheck and config check.
  kcctl clean

  # Uninstall the entire kubeclipper platform,use specify the auth config.
  kcctl clean -A --config ~/.kc/config

  # Uninstall the entire kubeclipper platform,use local deploy config when kc-server is not health.
  kcctl clean -A -f

  Please read 'kcctl clean -h' get more clean flags`
)

type CleanOptions struct {
	options.IOStreams
	cliOpts            *options.CliOptions
	deployConfig       *options.DeployConfig
	client             *kc.Client
	cleanAll           bool
	force              bool
	sudoPreCheck       func(string, *sshutils.SSH, options.IOStreams, []string) bool
	remoteCleanup      func(*sshutils.SSH, []string, string, []string) error
	localConfigCleanup func() error

	allNodes []string
}

type sshTransportGroup struct {
	id        string
	sshConfig *sshutils.SSH
	hosts     []string
}

func NewCleanOptions(streams options.IOStreams) *CleanOptions {
	o := &CleanOptions{
		IOStreams:     streams,
		cliOpts:       options.NewCliOptions(),
		deployConfig:  options.NewDeployOptions(),
		sudoPreCheck:  sudo.PreCheck,
		remoteCleanup: runRemoteCleanup,
	}
	o.localConfigCleanup = o.cleanKcConfig
	return o
}

func NewCmdClean(streams options.IOStreams) *cobra.Command {
	o := NewCleanOptions(streams)
	cmd := &cobra.Command{
		Use:                   "clean [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "Uninstall kubeclipper platform",
		Long:                  longDescription,
		Example:               cleanExample,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete())
			if !o.preCheck() {
				return
			}
			utils.CheckErr(o.RunClean())
			fmt.Printf("\033[1;40;36m%s\033[0m\n", options.Contact)
		},
	}
	o.cliOpts.AddFlags(cmd.Flags())
	cmd.Flags().BoolVarP(&o.cleanAll, "all", "A", o.cleanAll, "clean all components for kubeclipper")
	cmd.Flags().BoolVarP(&o.force, "force", "f", o.force, "force use local deploy config to clean kubeclipper when kc-server not health")
	cmd.Flags().StringVar(&o.deployConfig.Config, "deploy-config", options.DefaultDeployConfigPath, "path to the deploy config file to use for clean,just work with force flag.")
	return cmd
}

func (c *CleanOptions) Complete() error {

	if c.force {
		if err := c.deployConfig.Complete(); err != nil {
			return errors.WithMessage(err, "get local deploy-config failed")
		}
	} else {
		// config Complete
		var err error
		if err = c.cliOpts.Complete(); err != nil {
			return err
		}
		c.client, err = kc.FromConfig(c.cliOpts.ToRawConfig())
		if err != nil {
			return err
		}

		// deploy-config Complete
		c.deployConfig, err = deploy.GetDeployConfig(context.Background(), c.client, true)
		if err != nil {
			return errors.WithMessage(err, "get online deploy-config failed")
		}
		nodes, err := c.client.ListNodes(context.Background(), kc.Queries{})
		if err != nil {
			return errors.WithMessage(err, "get online node inventory for clean failed")
		}
		if err := mergeAndPersistOnlineAgents(c.deployConfig, nodes); err != nil {
			return err
		}
	}
	if err := c.deployConfig.NormalizeSSHTransports(); err != nil {
		return errors.WithMessage(err, "normalize clean SSH transports")
	}
	if _, err := c.agentTransportGroups(true); err != nil {
		return err
	}

	c.allNodes = sets.NewString().
		Insert(c.deployConfig.ServerIPs...).
		Insert(c.deployConfig.Agents.ListIP()...).
		List()
	return nil
}

func mergeAndPersistOnlineAgents(deployConfig *options.DeployConfig, nodes *kc.NodesList) error {
	if err := mergeOnlineAgents(deployConfig, nodes); err != nil {
		return err
	}
	if err := deployConfig.Write(); err != nil {
		return errors.WithMessage(err, "persist merged node inventory for force clean")
	}
	return nil
}

func mergeOnlineAgents(deployConfig *options.DeployConfig, nodes *kc.NodesList) error {
	if deployConfig.Agents == nil {
		deployConfig.Agents = make(options.Agents)
	}
	if nodes == nil {
		return nil
	}
	for _, node := range nodes.Items {
		ip := node.Status.Ipv4DefaultIP
		if ip == "" {
			ip = node.Status.NodeIpv4DefaultIP
		}
		if ip == "" {
			return fmt.Errorf("online node %q has no reachable IPv4 address for clean", node.Name)
		}
		if !deployConfig.Agents.Exists(ip) {
			deployConfig.Agents.Add(ip, options.Metadata{AgentID: node.Name})
		}
	}
	return nil
}

func (c *CleanOptions) preCheck() bool {
	ok := true
	if len(c.deployConfig.ServerIPs) > 0 {
		ok = c.sudoPreCheck("server sudo", c.deployConfig.SSHConfig, c.IOStreams, c.deployConfig.ServerIPs) && ok
	}
	groups, err := c.agentTransportGroups(true)
	if err != nil {
		logger.Error(err)
		return false
	}
	for _, group := range groups {
		name := fmt.Sprintf("agent sudo (%s)", group.id)
		ok = c.sudoPreCheck(name, group.sshConfig, c.IOStreams, group.hosts) && ok
	}
	return ok
}

func (c *CleanOptions) agentTransportGroups(includeServerAgents bool) ([]sshTransportGroup, error) {
	if err := c.deployConfig.NormalizeSSHTransports(); err != nil {
		return nil, errors.WithMessage(err, "normalize agent SSH transports")
	}
	servers := sets.NewString(c.deployConfig.ServerIPs...)
	grouped := make(map[string]*sshTransportGroup)
	for _, ip := range c.deployConfig.Agents.ListIP() {
		if !includeServerAgents && servers.Has(ip) {
			continue
		}
		id, sshConfig, err := c.deployConfig.SSHTransportForAgent(ip)
		if err != nil {
			return nil, err
		}
		group := grouped[id]
		if group == nil {
			group = &sshTransportGroup{id: id, sshConfig: sshConfig}
			grouped[id] = group
		}
		group.hosts = append(group.hosts, ip)
	}
	ids := make([]string, 0, len(grouped))
	for id := range grouped {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	groups := make([]sshTransportGroup, 0, len(ids))
	for _, id := range ids {
		group := grouped[id]
		sort.Strings(group.hosts)
		groups = append(groups, *group)
	}
	return groups, nil
}

func (c *CleanOptions) RunClean() error {
	if c.cleanAll {
		if err := utilerrors.NewAggregate([]error{
			c.cleanKcAgent(), c.cleanKcServer(), c.cleanKcConsole(),
			c.cleanBinaries(), c.cleanKcEnv(),
		}); err != nil {
			return errors.Wrap(err, "clean kubeclipper platform failed")
		}
		// Keep the local recovery inventory whenever a remote cleanup fails.
		// It is the only source available to a subsequent force-clean after the
		// server has already been removed.
		if err := c.localConfigCleanup(); err != nil {
			return errors.Wrap(err, "clean kubeclipper local config failed")
		}
	}
	logger.Info("clean successful")
	return nil
}

func (c *CleanOptions) cleanKcAgent() error {
	if len(c.deployConfig.Agents.ListIP()) == 0 {
		logger.V(2).Info("no kubeclipper agent need to be cleaned")
		return nil
	}

	cmdList := agentCleanupCommands(c.deployConfig.OpLog.Dir)
	var errs []error
	groups, err := c.agentTransportGroups(true)
	if err != nil {
		return err
	}
	for _, group := range groups {
		errs = append(errs, c.remoteCleanup(group.sshConfig, group.hosts, "kc agent ("+group.id+")", cmdList))
	}
	return utilerrors.NewAggregate(errs)
}

func agentCleanupCommands(opLogDir string) []string {
	return []string{
		"systemctl disable kc-agent --now || true",
		"rm -rf /usr/lib/systemd/system/kc-agent.service",
		"rm -rf /etc/kubeclipper-agent",
		"rm -rf /opt/kc/manifest && (rmdir /opt/kc 2>/dev/null || true)",
		fmt.Sprintf("rm -rf %s", opLogDir),
		"systemctl reset-failed kc-agent || true",
	}
}

func (c *CleanOptions) cleanKcServer() error {
	if len(c.deployConfig.ServerIPs) == 0 {
		logger.V(2).Info("no kubeclipper server need to be cleaned")
		return nil
	}

	cmdList := serverCleanupCommands(c.deployConfig.EtcdConfig.DataDir)
	return c.remoteCleanup(c.deployConfig.SSHConfig, c.deployConfig.ServerIPs, "kc server", cmdList)
}

func serverCleanupCommands(etcdDataDir string) []string {
	return []string{
		"systemctl disable kc-server --now || true",
		"rm -rf /usr/lib/systemd/system/kc-server.service",
		"systemctl disable kc-etcd --now || true",
		"rm -rf /usr/lib/systemd/system/kc-etcd.service",
		"rm -rf /etc/kubeclipper-server",
		fmt.Sprintf("rm -rf %s", etcdDataDir),
		"systemctl reset-failed kc-etcd || true",
		"systemctl reset-failed kc-server || true",
	}
}

func (c *CleanOptions) cleanKcConsole() error {
	if len(c.deployConfig.ServerIPs) == 0 {
		logger.V(2).Info("no kubeclipper console need to be cleaned")
		return nil
	}

	cmdList := []string{
		"systemctl disable kc-console --now || true",
		"rm -rf /usr/lib/systemd/system/kc-console.service",
		"rm -rf /etc/kc-console",
		"systemctl reset-failed kc-console || true",
	}
	return c.remoteCleanup(c.deployConfig.SSHConfig, c.deployConfig.ServerIPs, "kc console", cmdList)
}

func (c *CleanOptions) cleanBinaries() error {
	if len(c.allNodes) == 0 {
		logger.V(2).Info("no kubeclipper node need to be cleaned")
		return nil
	}

	cmdList := []string{
		"rm -rf /usr/local/bin/kubeclipper* && rm -rf /usr/local/bin/etcd*  && rm -rf /usr/local/bin/caddy",
	}
	var errs []error
	if len(c.deployConfig.ServerIPs) > 0 {
		errs = append(errs, c.remoteCleanup(c.deployConfig.SSHConfig, c.deployConfig.ServerIPs, "server binaries", cmdList))
	}
	groups, err := c.agentTransportGroups(false)
	if err != nil {
		return err
	}
	for _, group := range groups {
		errs = append(errs, c.remoteCleanup(group.sshConfig, group.hosts, "agent binaries ("+group.id+")", cmdList))
	}
	return utilerrors.NewAggregate(errs)
}

func (c *CleanOptions) cleanKcEnv() error {
	if len(c.deployConfig.ServerIPs) == 0 {
		logger.V(2).Info("no kubeclipper console need to be cleaned")
		return nil
	}

	cmdList := []string{
		"rm -rf /etc/kc/kc.env",
	}

	return c.remoteCleanup(c.deployConfig.SSHConfig, c.deployConfig.ServerIPs, "kc environment", cmdList)
}

func (c *CleanOptions) cleanKcConfig() error {
	if err := sshutils.Cmd("rm", "-rf", filepath.Dir(options.DefaultDeployConfigPath)); err != nil {
		return errors.Wrap(err, "clean kc config")
	}
	return nil
}

func runRemoteCleanup(sshConfig *sshutils.SSH, hosts []string, component string, commands []string) error {
	var errs []error
	for _, command := range commands {
		if err := sshutils.CmdBatchWithSudo(sshConfig, hosts, command, sshutils.DefaultWalk); err != nil {
			errs = append(errs, errors.Wrapf(err, "clean %s", component))
		}
	}
	return utilerrors.NewAggregate(errs)
}
