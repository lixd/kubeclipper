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

package cluster

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	v1 "github.com/kubeclipper/kubeclipper/pkg/apis/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/cli/printer"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	corevalidation "github.com/kubeclipper/kubeclipper/pkg/scheme/core/validation"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

const (
	upgradeLongDescription = `
	Upgrade cluster version
`
	clusterUpgradeExample = `
	# Upgrade cluster using its saved image registry
	kcctl cluster upgrade --cluster-name clu-1 --version v1.21.3
`
)

const (
	versionRule = "^[v]([1-9]\\d|[1-9])(.([1-9]\\d|\\d)){2}$"
)

type ClusterUpgradeOpts struct {
	BaseOptions
	ClusterName string
	Version     string
}

func NewClusterUpgradeOpts(streams options.IOStreams) *ClusterUpgradeOpts {
	return &ClusterUpgradeOpts{
		BaseOptions: BaseOptions{
			PrintFlags: printer.NewPrintFlags(),
			CliOpts:    options.NewCliOptions(),
			IOStreams:  streams,
		},
	}
}

func NewCmdClusterUpgrade(streams options.IOStreams) *cobra.Command {
	c := NewClusterUpgradeOpts(streams)
	cmd := &cobra.Command{
		Use:     "upgrade (--cluster-name) (--version) [flags]",
		Short:   "upgrade kubernetes cluster version",
		Long:    upgradeLongDescription,
		Example: clusterUpgradeExample,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(c.Complete())
			utils.CheckErr(c.Validates())
			utils.CheckErr(c.Run())
		},
	}

	cmd.Flags().StringVarP(&c.ClusterName, "cluster-name", "c", c.ClusterName, "cluster name")
	cmd.Flags().StringVarP(&c.Version, "version", "v", c.Version, "target version")

	utils.CheckErr(cmd.MarkFlagRequired("cluster-name"))
	utils.CheckErr(cmd.MarkFlagRequired("version"))

	return cmd
}

func (c *ClusterUpgradeOpts) Complete() error {
	if err := c.CliOpts.Complete(); err != nil {
		return err
	}
	client, err := kc.FromConfig(c.CliOpts.ToRawConfig())
	if err != nil {
		return err
	}
	c.Client = client
	return nil
}

func (c *ClusterUpgradeOpts) Validates() error {
	if err := c.checkVersionFormat(); err != nil {
		return err
	}

	if err := c.checkVersionExist(); err != nil {
		return err
	}

	clu, err := c.checkClusterExist()
	if err != nil {
		return err
	}
	return corevalidation.ValidateKubernetesUpgradeVersion(clu.Name, clu.KubernetesVersion, c.Version)
}

func (c *ClusterUpgradeOpts) Run() error {
	clusterUpgrade := &v1.ClusterUpgrade{
		Version: c.Version,
	}

	return c.Client.UpgradeCluster(context.TODO(), c.ClusterName, clusterUpgrade)
}

func (c *ClusterUpgradeOpts) checkClusterExist() (corev1.Cluster, error) {
	var err error
	var clusterList *kc.ClustersList
	clusterList, err = c.Client.DescribeCluster(context.TODO(), c.ClusterName)
	if err != nil {
		return corev1.Cluster{}, err
	}
	return clusterList.Items[0], nil
}

func (c *ClusterUpgradeOpts) checkVersionFormat() error {
	m, err := regexp.Compile(versionRule)
	if err != nil {
		return err
	}
	if !m.MatchString(c.Version) {
		return errors.New("wrong version format")
	}
	return nil
}

func (c *ClusterUpgradeOpts) checkVersionExist() error {
	componentMeta, err := c.Client.GetComponentMeta(context.TODO(), nil)
	if err != nil {
		return err
	}
	versions := sets.NewString()
	for _, rule := range componentMeta.Rules {
		if version, ok := rule["version"].(string); ok {
			versions.Insert(version)
		}
	}
	if !versions.Has(c.Version) {
		return fmt.Errorf("version [%s] you specify is not avalible", c.Version)
	}
	return nil
}
