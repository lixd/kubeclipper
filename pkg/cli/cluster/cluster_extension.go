/*
 *
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package cluster

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/printer"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

var (
	extensionLongDescription = `
  Re-install the k8s-extension package on a running cluster. The operation is
  built from the cluster's own packagePlan snapshot, so it cannot drift to a
  different artifact, and other clusters' package plans are untouched.`

	extensionExample = `
  # re-install the k8s-extension package on a running cluster
  kcctl cluster extension --cluster-name demo`
)

type ClusterExtensionOpts struct {
	BaseOptions
	ClusterName string
}

func NewClusterExtensionOpts(streams options.IOStreams) *ClusterExtensionOpts {
	return &ClusterExtensionOpts{
		BaseOptions: BaseOptions{
			PrintFlags: printer.NewPrintFlags(),
			CliOpts:    options.NewCliOptions(),
			IOStreams:  streams,
		},
	}
}

func NewCmdClusterExtension(streams options.IOStreams) *cobra.Command {
	o := NewClusterExtensionOpts(streams)
	cmd := &cobra.Command{
		Use:     "extension (--cluster-name) [flags]",
		Short:   "re-install the k8s-extension package on a running cluster",
		Long:    extensionLongDescription,
		Example: extensionExample,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete(o.CliOpts))
			utils.CheckErr(o.Validates(cmd))
			utils.CheckErr(o.Run())
		},
	}
	cmd.Flags().StringVar(&o.ClusterName, "cluster-name", o.ClusterName, "cluster name")
	utils.CheckErr(cmd.MarkFlagRequired("cluster-name"))
	return cmd
}

func (o *ClusterExtensionOpts) Complete(opts *options.CliOptions) error {
	if err := o.CliOpts.Complete(); err != nil {
		return err
	}
	client, err := kc.FromConfig(o.CliOpts.ToRawConfig())
	if err != nil {
		return err
	}
	o.Client = client
	return nil
}

func (o *ClusterExtensionOpts) Validates(cmd *cobra.Command) error {
	if o.ClusterName == "" {
		return utils.UsageErrorf(cmd, "cluster name must be specified")
	}
	return nil
}

func (o *ClusterExtensionOpts) Run() error {
	if err := o.Client.UpgradeClusterExtension(context.TODO(), o.ClusterName); err != nil {
		return err
	}
	_, err := fmt.Fprintf(o.IOStreams.Out, "cluster %s extension upgrade submitted\n", o.ClusterName)
	return err
}
