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
	"strings"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/printer"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	"github.com/kubeclipper/kubeclipper/pkg/clusteroperation"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

const (
	convertLongDescription = `
	Convert existing cluster nodes between the master and worker roles.

	Converting to master joins a worker into the control plane (etcd member +
	apiserver). Converting to worker demotes a master: the node is drained,
	reset and rejoined as a worker. The converted nodes stay in the cluster.
`
	convertExample = `
	# promote a worker node to master
	cluster convert --cluster-name demo --role master --nodes 172.20.151.110

	# demote a master node back to worker (node ID from 'kcctl get node')
	cluster convert --cluster-name demo --role worker --nodes 7fe23459-9b74-4f11-9377-98b1f49d4e75
`
)

type ClusterConvertOpts struct {
	BaseOptions
	ClusterName string
	Role        string
	Nodes       []string
}

func NewClusterConvertOpts(streams options.IOStreams) *ClusterConvertOpts {
	return &ClusterConvertOpts{
		BaseOptions: BaseOptions{
			PrintFlags: printer.NewPrintFlags(),
			CliOpts:    options.NewCliOptions(),
			IOStreams:  streams,
		},
	}
}

func NewCmdClusterConvert(streams options.IOStreams) *cobra.Command {
	o := NewClusterConvertOpts(streams)
	cmd := &cobra.Command{
		Use:     "convert (--cluster-name) (--role master|worker) (--nodes) [flags]",
		Short:   "convert cluster nodes between master and worker roles",
		Long:    convertLongDescription,
		Example: convertExample,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete())
			utils.CheckErr(o.Validate())
			if !o.confirm() {
				return
			}
			utils.CheckErr(o.Run())
		},
	}

	cmd.Flags().StringVarP(&o.ClusterName, "cluster-name", "c", o.ClusterName, "cluster name")
	cmd.Flags().StringVarP(&o.Role, "role", "r", o.Role, "target node role: master or worker")
	cmd.Flags().StringSliceVar(&o.Nodes, "nodes", o.Nodes, "comma-separated node IDs or IPv4 addresses of the nodes to convert")
	utils.CheckErr(cmd.MarkFlagRequired("cluster-name"))
	utils.CheckErr(cmd.MarkFlagRequired("role"))
	utils.CheckErr(cmd.MarkFlagRequired("nodes"))

	return cmd
}

func (o *ClusterConvertOpts) Complete() error {
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

func (o *ClusterConvertOpts) Validate() error {
	if o.ClusterName == "" {
		return errors.New("--cluster-name is required")
	}
	switch common.NodeRole(o.Role) {
	case common.NodeRoleMaster, common.NodeRoleWorker:
	default:
		return fmt.Errorf("--role must be master or worker, got %q", o.Role)
	}
	if len(o.Nodes) == 0 {
		return errors.New("--nodes is required")
	}
	clu, err := o.getCluster()
	if err != nil {
		return err
	}
	// resolve ID or IPv4 to node IDs and verify the nodes belong to the
	// opposite role before touching the server
	target := common.NodeRole(o.Role)
	source := common.NodeRoleWorker
	if target == common.NodeRoleWorker {
		source = common.NodeRoleMaster
	}
	inCluster, err := o.roleNodeIDs(clu, source)
	if err != nil {
		return err
	}
	resolved := make([]string, 0, len(o.Nodes))
	for _, entry := range o.Nodes {
		if _, ok := inCluster[entry]; ok {
			resolved = append(resolved, entry)
			continue
		}
		if id, ok := o.ipToNodeID(clu, entry); ok {
			if _, member := inCluster[id]; member {
				resolved = append(resolved, id)
				continue
			}
		}
		return fmt.Errorf("node %q is not a %s of cluster %s", entry, source, o.ClusterName)
	}
	o.Nodes = resolved
	return nil
}

func (o *ClusterConvertOpts) confirm() bool {
	if options.AssumeYes {
		return true
	}
	_, _ = o.IOStreams.Out.Write([]byte(fmt.Sprintf(
		"Convert node(s) %s of cluster %s to %s. This changes etcd membership and cannot be undone by the platform. Please input (yes/no) ",
		strings.Join(o.Nodes, ","), o.ClusterName, o.Role)))
	return utils.AskForConfirmation()
}

func (o *ClusterConvertOpts) Run() error {
	pcn := &clusteroperation.PatchConvertNodes{
		Role:  common.NodeRole(o.Role),
		Nodes: toWorkerNodeList(o.Nodes),
	}
	_, err := o.Client.ConvertClusterNodes(context.TODO(), o.ClusterName, pcn)
	if err != nil {
		return err
	}
	_, _ = o.IOStreams.Out.Write([]byte(fmt.Sprintf("cluster %s node conversion to %s submitted for node(s): %s\n",
		o.ClusterName, o.Role, strings.Join(o.Nodes, ","))))
	return nil
}

func (o *ClusterConvertOpts) getCluster() (corev1.Cluster, error) {
	clusterList, err := o.Client.DescribeCluster(context.TODO(), o.ClusterName)
	if err != nil {
		return corev1.Cluster{}, err
	}
	if len(clusterList.Items) == 0 {
		return corev1.Cluster{}, fmt.Errorf("cluster %s not found", o.ClusterName)
	}
	return clusterList.Items[0], nil
}

// roleNodeIDs returns the ID set of the given role in the cluster.
func (o *ClusterConvertOpts) roleNodeIDs(clu corev1.Cluster, role common.NodeRole) (map[string]struct{}, error) {
	list := clu.Workers
	if role == common.NodeRoleMaster {
		list = clu.Masters
	}
	set := make(map[string]struct{}, len(list))
	for _, id := range list.GetNodeIDs() {
		set[id] = struct{}{}
	}
	return set, nil
}

// ipToNodeID maps an agent IPv4 to the KC node ID via the node registry.
func (o *ClusterConvertOpts) ipToNodeID(clu corev1.Cluster, ip string) (string, bool) {
	if nodeList, err := o.Client.ListNodes(context.TODO(), kc.Queries{}); err == nil {
		for _, n := range nodeList.Items {
			if n.Status.Ipv4DefaultIP == ip {
				return n.Name, true
			}
		}
	}
	return "", false
}

func toWorkerNodeList(ids []string) corev1.WorkerNodeList {
	list := make(corev1.WorkerNodeList, 0, len(ids))
	for _, id := range ids {
		list = append(list, corev1.WorkerNode{ID: id})
	}
	return list
}
