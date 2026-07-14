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

package cluster

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/printer"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	"github.com/kubeclipper/kubeclipper/pkg/clusteroperation"
	"github.com/kubeclipper/kubeclipper/pkg/query"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

type NodeOptions struct {
	BaseOptions
	Operation   clusteroperation.NodesPatchOperation
	ClusterName string
	Workers     []string
}

func NewNodeOptions(streams options.IOStreams, operation clusteroperation.NodesPatchOperation) *NodeOptions {
	return &NodeOptions{
		BaseOptions: BaseOptions{
			PrintFlags: printer.NewPrintFlags(),
			CliOpts:    options.NewCliOptions(),
			IOStreams:  streams,
		},
		Operation: operation,
	}
}

func NewCmdAddNode(streams options.IOStreams) *cobra.Command {
	return newCmdNode(streams, clusteroperation.NodesOperationAdd)
}

func NewCmdRemoveNode(streams options.IOStreams) *cobra.Command {
	return newCmdNode(streams, clusteroperation.NodesOperationRemove)
}

func newCmdNode(streams options.IOStreams, operation clusteroperation.NodesPatchOperation) *cobra.Command {
	o := NewNodeOptions(streams, operation)
	verb := string(operation)
	cmd := &cobra.Command{
		Use:                   verb + "-node --cluster-name <cluster> --worker <node-id-or-ip> [flags]",
		DisableFlagsInUseLine: true,
		Short:                 verb + " worker nodes in a Kubernetes cluster",
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete())
			utils.CheckErr(o.Validate(cmd))
			utils.CheckErr(o.Run())
		},
	}
	cmd.Flags().StringVar(&o.ClusterName, "cluster-name", "", "Kubernetes cluster name")
	cmd.Flags().StringSliceVar(&o.Workers, "worker", nil, "worker node ID or IP; may be specified more than once")
	o.CliOpts.AddFlags(cmd.Flags())
	o.PrintFlags.AddFlags(cmd)

	utils.CheckErr(cmd.RegisterFlagCompletionFunc("cluster-name", o.completeClusterNames))
	utils.CheckErr(cmd.RegisterFlagCompletionFunc("worker", o.completeWorkers))
	utils.CheckErr(cmd.MarkFlagRequired("cluster-name"))
	utils.CheckErr(cmd.MarkFlagRequired("worker"))
	return cmd
}

func (o *NodeOptions) Complete() error {
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

func (o *NodeOptions) Validate(cmd *cobra.Command) error {
	if o.Operation != clusteroperation.NodesOperationAdd && o.Operation != clusteroperation.NodesOperationRemove {
		return utils.UsageErrorf(cmd, "unsupported node operation %q", o.Operation)
	}
	if strings.TrimSpace(o.ClusterName) == "" {
		return utils.UsageErrorf(cmd, "--cluster-name must be specified")
	}
	if len(o.Workers) == 0 {
		return utils.UsageErrorf(cmd, "at least one --worker must be specified")
	}
	return nil
}

func (o *NodeOptions) Run() error {
	nodes, err := o.resolveWorkers(context.Background())
	if err != nil {
		return err
	}
	result, err := o.Client.AddOrRemoveNode(context.Background(), &clusteroperation.PatchNodes{
		Operation: o.Operation,
		Role:      common.NodeRoleWorker,
		Nodes:     nodes,
	}, o.ClusterName)
	if err != nil {
		return err
	}
	return o.PrintFlags.Print(result, o.Out)
}

func (o *NodeOptions) resolveWorkers(ctx context.Context) (corev1.WorkerNodeList, error) {
	listed, err := o.Client.ListNodes(ctx, kc.Queries{})
	if err != nil {
		return nil, err
	}
	byIdentity := make(map[string]string, len(listed.Items)*2)
	for i := range listed.Items {
		node := &listed.Items[i]
		byIdentity[node.Name] = node.Name
		if node.Status.Ipv4DefaultIP != "" {
			byIdentity[node.Status.Ipv4DefaultIP] = node.Name
		}
	}
	workers := make(corev1.WorkerNodeList, 0, len(o.Workers))
	seen := make(map[string]struct{}, len(o.Workers))
	for _, identity := range o.Workers {
		id, ok := byIdentity[strings.TrimSpace(identity)]
		if !ok {
			return nil, fmt.Errorf("node %s does not exist", identity)
		}
		if _, ok = seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		workers = append(workers, corev1.WorkerNode{ID: id})
	}
	return workers, nil
}

func (o *NodeOptions) completeClusterNames(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if err := o.Complete(); err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	clusters, err := o.Client.ListClusters(context.Background(), kc.Queries{})
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	var result []string
	for i := range clusters.Items {
		if strings.HasPrefix(clusters.Items[i].Name, toComplete) {
			result = append(result, clusters.Items[i].Name)
		}
	}
	return result, cobra.ShellCompDirectiveNoFileComp
}

func (o *NodeOptions) completeWorkers(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if err := o.Complete(); err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	q := query.New()
	q.Limit = 1000
	nodes, err := o.Client.ListNodes(context.Background(), kc.Queries(*q))
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	var target *corev1.Cluster
	if o.ClusterName != "" {
		clusters, err := o.Client.DescribeCluster(context.Background(), o.ClusterName)
		if err != nil || len(clusters.Items) == 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		target = &clusters.Items[0]
	}
	selected := make(map[string]struct{}, len(o.Workers))
	for _, worker := range o.Workers {
		selected[worker] = struct{}{}
	}
	var result []string
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if target != nil && !workerEligible(o.Operation, node.Name, target) {
			continue
		}
		for _, identity := range []string{node.Name, node.Status.Ipv4DefaultIP} {
			if identity == "" || !strings.HasPrefix(identity, toComplete) {
				continue
			}
			if _, ok := selected[identity]; ok {
				continue
			}
			result = append(result, identity)
		}
	}
	return result, cobra.ShellCompDirectiveNoFileComp
}

func workerEligible(operation clusteroperation.NodesPatchOperation, nodeID string, cluster *corev1.Cluster) bool {
	if cluster == nil {
		return true
	}
	workers := cluster.Workers.GetNodeIDs()
	switch operation {
	case clusteroperation.NodesOperationAdd:
		return !cluster.GetAllNodes().Has(nodeID)
	case clusteroperation.NodesOperationRemove:
		for _, worker := range workers {
			if worker == nodeID {
				return true
			}
		}
	}
	return false
}
