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
	cliOpts      *options.CliOptions
	deployConfig *options.DeployConfig
	client       *kc.Client
	cleanAll     bool
	force        bool

	allNodes []string
}

func NewCleanOptions(streams options.IOStreams) *CleanOptions {
	return &CleanOptions{
		IOStreams:    streams,
		cliOpts:      options.NewCliOptions(),
		deployConfig: options.NewDeployOptions(),
	}
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
		c.deployConfig, err = deploy.GetDeployConfig(context.Background(), c.client, false)
		if err != nil {
			return errors.WithMessage(err, "get online deploy-config failed")
		}
		nodes, err := c.client.ListNodes(context.Background(), kc.Queries{})
		if err != nil {
			return errors.WithMessage(err, "get online node inventory for clean failed")
		}
		if err := mergeOnlineAgents(c.deployConfig, nodes); err != nil {
			return err
		}
	}

	c.allNodes = sets.NewString().
		Insert(c.deployConfig.ServerIPs...).
		Insert(c.deployConfig.Agents.ListIP()...).
		List()
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
	serverOK := sudo.PreCheck("server sudo", c.deployConfig.SSHConfig, c.IOStreams, c.deployConfig.ServerIPs)
	agentOK := sudo.PreCheck("agent sudo", c.agentSSHConfig(), c.IOStreams, c.deployConfig.Agents.ListIP())
	return serverOK && agentOK
}

func (c *CleanOptions) agentSSHConfig() *sshutils.SSH {
	if c.deployConfig.AgentSSHConfig != nil {
		return c.deployConfig.AgentSSHConfig
	}
	return c.deployConfig.SSHConfig
}

func (c *CleanOptions) RunClean() error {
	if c.cleanAll {
		if err := utilerrors.NewAggregate([]error{
			c.cleanKcAgent(), c.cleanKcServer(), c.cleanKcConsole(),
			c.cleanBinaries(), c.cleanKcEnv(), c.cleanKcConfig(),
		}); err != nil {
			return errors.Wrap(err, "clean kubeclipper platform failed")
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

	cmdList := []string{
		"systemctl disable kc-agent --now || true",
		"rm -rf /usr/lib/systemd/system/kc-agent.service",
		"rm -rf /etc/kubeclipper-agent",
		fmt.Sprintf("rm -rf %s", c.deployConfig.OpLog.Dir),
		"systemctl reset-failed kc-agent || true",
	}
	return runRemoteCleanup(c.agentSSHConfig(), c.deployConfig.Agents.ListIP(), "kc agent", cmdList)
}

func (c *CleanOptions) cleanKcServer() error {
	if len(c.deployConfig.ServerIPs) == 0 {
		logger.V(2).Info("no kubeclipper server need to be cleaned")
		return nil
	}

	cmdList := []string{
		"systemctl disable kc-server --now || true",
		"rm -rf /usr/lib/systemd/system/kc-server.service",
		"systemctl disable kc-etcd --now || true",
		"rm -rf /usr/lib/systemd/system/kc-etcd.service",
		"rm -rf /etc/kubeclipper-server",
		fmt.Sprintf("rm -rf %s", c.deployConfig.EtcdConfig.DataDir),
		"systemctl reset-failed kc-etcd || true",
		"systemctl reset-failed kc-server || true",
	}
	return runRemoteCleanup(c.deployConfig.SSHConfig, c.deployConfig.ServerIPs, "kc server", cmdList)
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
	return runRemoteCleanup(c.deployConfig.SSHConfig, c.deployConfig.ServerIPs, "kc console", cmdList)
}

func (c *CleanOptions) cleanBinaries() error {
	if len(c.allNodes) == 0 {
		logger.V(2).Info("no kubeclipper node need to be cleaned")
		return nil
	}

	cmdList := []string{
		"rm -rf /usr/local/bin/kubeclipper* && rm -rf /usr/local/bin/etcd*  && rm -rf /usr/local/bin/caddy",
	}

	return utilerrors.NewAggregate([]error{
		runRemoteCleanup(c.deployConfig.SSHConfig, c.deployConfig.ServerIPs, "server binaries", cmdList),
		runRemoteCleanup(c.agentSSHConfig(), c.deployConfig.Agents.ListIP(), "agent binaries", cmdList),
	})
}

func (c *CleanOptions) cleanKcEnv() error {
	if len(c.deployConfig.ServerIPs) == 0 {
		logger.V(2).Info("no kubeclipper console need to be cleaned")
		return nil
	}

	cmdList := []string{
		"rm -rf /etc/kc/kc.env",
	}

	return runRemoteCleanup(c.deployConfig.SSHConfig, c.deployConfig.ServerIPs, "kc environment", cmdList)
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
