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

package clusteroperation

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1/k8s"
	"github.com/kubeclipper/kubeclipper/pkg/utils/strutil"
)

// PatchConvertNodes flips existing cluster nodes between the master and worker
// roles. Role is the TARGET role: converting to master promotes workers into
// the control plane; converting to worker demotes masters back into workers.
// ConvertNodes carries the resolved node info so the builder can relocate the
// nodes inside the extra metadata regardless of which cluster revision the
// controller reads (the storage Get ignores historical resource versions).
type PatchConvertNodes struct {
	Role         common.NodeRole       `json:"role"`
	Nodes        corev1.WorkerNodeList `json:"nodes"`
	ConvertNodes []component.Node      `json:"convertNodes"`
}

func (p *PatchConvertNodes) Validate() error {
	switch p.Role {
	case common.NodeRoleMaster, common.NodeRoleWorker:
	default:
		return fmt.Errorf("%w: %q, must be master or worker", ErrInvalidNodesRole, p.Role)
	}
	if len(p.Nodes) == 0 {
		return ErrZeroNode
	}
	return nil
}

// MakeCompare moves the given nodes between the cluster's master and worker
// lists. Nodes must already belong to the opposite role; the handler rejects
// the request when nothing remains to convert.
func (p *PatchConvertNodes) MakeCompare(cluster *corev1.Cluster) error {
	if err := p.Validate(); err != nil {
		return err
	}
	switch p.Role {
	case common.NodeRoleMaster:
		// promote: nodes must currently be workers
		p.Nodes = cluster.Workers.Intersect(p.Nodes...)
		if len(p.Nodes) == 0 {
			return fmt.Errorf("%w: no worker node to convert to master", ErrZeroNode)
		}
		cluster.Workers = cluster.Workers.Complement(p.Nodes...)
		cluster.Masters = append(cluster.Masters, p.Nodes...)
	case common.NodeRoleWorker:
		// demote: nodes must currently be masters
		p.Nodes = cluster.Masters.Intersect(p.Nodes...)
		if len(p.Nodes) == 0 {
			return fmt.Errorf("%w: no master node to convert to worker", ErrZeroNode)
		}
		// keep at least one master: demoting all control-plane members
		// leaves the cluster without an apiserver/etcd quorum
		if len(cluster.Masters)-len(p.Nodes) < 1 {
			return fmt.Errorf("%w: converting all %d master nodes would leave the cluster without a control plane, at least one master must remain",
				ErrInvalidNodesTopology, len(cluster.Masters))
		}
		cluster.Masters = cluster.Masters.Complement(p.Nodes...)
		cluster.Workers = append(cluster.Workers, p.Nodes...)
	}
	return nil
}

// moveExtra relocates the converted nodes inside the extra metadata so that
// agent-side step targets resolve their IP/hostname from the TARGET role.
// The extra metadata may come from either the pre- or post-operation cluster
// revision, so the nodes are removed from BOTH role lists first and then
// appended to the target role exactly once, using the resolved node info
// carried in ConvertNodes.
func (p *PatchConvertNodes) moveExtra(extra *component.ExtraMetadata) error {
	if len(p.ConvertNodes) == 0 {
		return fmt.Errorf("convert nodes carry no resolved node info")
	}
	convertIDs := make(map[string]struct{}, len(p.ConvertNodes))
	for _, n := range p.ConvertNodes {
		convertIDs[n.ID] = struct{}{}
	}
	extra.Masters = dropNodes(extra.Masters, convertIDs)
	extra.Workers = dropNodes(extra.Workers, convertIDs)
	switch p.Role {
	case common.NodeRoleMaster:
		extra.Masters = append(extra.Masters, p.ConvertNodes...)
	case common.NodeRoleWorker:
		extra.Workers = append(extra.Workers, p.ConvertNodes...)
	default:
		return ErrInvalidNodesRole
	}
	return nil
}

func dropNodes(from []component.Node, ids map[string]struct{}) []component.Node {
	remain := make([]component.Node, 0, len(from))
	for _, n := range from {
		if _, ok := ids[n.ID]; ok {
			continue
		}
		remain = append(remain, n)
	}
	return remain
}

func setsNew(ids []string) map[string]struct{} {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

// ConvertNodeOperation builds the ConvertNodes operation.
var _ Interface = (*ConvertNodeOperation)(nil)

type ConvertNodeOperation struct {
	Options
}

func NewConvertNodeOperation(options Options) *ConvertNodeOperation {
	return &ConvertNodeOperation{options}
}

func (n *ConvertNodeOperation) Builder() (*corev1.Operation, error) {
	var pcn PatchConvertNodes
	if err := json.Unmarshal(n.pendingOperation.ExtraData, &pcn); err != nil {
		return nil, err
	}
	if err := pcn.Validate(); err != nil {
		return nil, err
	}
	// move the nodes between the extra role lists before any step generation:
	// promote resolves IPs from the master maps, demote from the worker maps,
	// and both directions must exclude the converted node from its source role
	if err := pcn.moveExtra(n.extra); err != nil {
		return nil, err
	}

	op, err := pcn.MakeOperation(*n.extra, n.cluster)
	if err != nil {
		return nil, err
	}
	op.Labels[common.LabelTimeoutSeconds] = n.pendingOperation.Timeout
	op.Labels[common.LabelOperationSponsor] = n.pendingOperation.OperationSponsor
	return op, nil
}

// MakeOperation must be called after MakeCompare moved the nodes in the
// cluster object and moveExtra moved them in the extra metadata.
func (p *PatchConvertNodes) MakeOperation(extra component.ExtraMetadata, cluster *corev1.Cluster) (*corev1.Operation, error) {
	op := &corev1.Operation{}
	op.Name = extra.OperationID
	op.Labels = map[string]string{
		common.LabelClusterName:     cluster.Name,
		common.LabelOperationAction: corev1.OperationConvertNodes,
	}
	ctx := component.WithExtraMetadata(context.TODO(), extra)
	// the converted nodes already run the k8s packages from the cluster plan;
	// the plan is only consumed by offline addon/CNI image-load steps which
	// stay idempotent on already-provisioned nodes
	if plan, err := deliveryapis.DecodeResolvedArtifactPlan(cluster.Status.PackagePlan); err != nil {
		return nil, fmt.Errorf("decode cluster package plan: %w", err)
	} else if plan != nil {
		ctx = component.WithResolvedArtifactPlan(ctx, plan)
	}

	switch p.Role {
	case common.NodeRoleMaster:
		return p.makePromoteOperation(ctx, extra, cluster, op)
	case common.NodeRoleWorker:
		return p.makeDemoteOperation(ctx, extra, cluster, op)
	default:
		return nil, ErrInvalidNodesRole
	}
}

// makePromoteOperation joins an existing worker into the control plane. The
// node already runs CRI, packages and extensions, so only the kubeadm
// control-plane join chain is needed.
func (p *PatchConvertNodes) makePromoteOperation(ctx context.Context, extra component.ExtraMetadata, cluster *corev1.Cluster, op *corev1.Operation) (*corev1.Operation, error) {
	stepNodes, err := p.targetStepNodes(extra)
	if err != nil {
		return nil, err
	}

	gen := k8s.GenNode{}
	if err = gen.InitStepper(&extra, cluster, common.NodeRoleMaster.String()).MakeInstallSteps(&extra, stepNodes, common.NodeRoleMaster.String()); err != nil {
		return nil, err
	}
	op.Steps = append(op.Steps, gen.GetSteps(corev1.ActionInstall)...)
	return op, nil
}

// makeDemoteOperation removes the control-plane role from a master and rejoins
// the node as a worker. kubeadm has no demotion, so the node is reset and
// rejoined: etcd member remove → drain → kubeadm reset → data dirs → join.
func (p *PatchConvertNodes) makeDemoteOperation(ctx context.Context, extra component.ExtraMetadata, cluster *corev1.Cluster, op *corev1.Operation) (*corev1.Operation, error) {
	stepNodes, err := p.targetStepNodes(extra)
	if err != nil {
		return nil, err
	}

	// teardown phase, mirrors the master branch of GenNode.MakeUninstallSteps
	// minus package/CNI/extension cleanup which the rejoin re-provisions
	avaMasters, err := extra.Masters.AvailableKubeMasters()
	if err != nil {
		return nil, err
	}
	remaining := k8s.UnwrapAvailableMasters(avaMasters, stepNodes)
	if len(remaining) == 0 {
		return nil, fmt.Errorf("no remaining control-plane node to serve the etcd and drain steps")
	}
	steps, err := k8s.EtcdMemberRemoveSteps(remaining[0], stepNodes)
	if err != nil {
		return nil, err
	}
	op.Steps = append(op.Steps, steps...)

	for _, node := range stepNodes {
		d := &k8s.Drain{}
		steps, err = d.InitStepper(node.Hostname, []string{"--ignore-daemonsets", "--delete-emptydir-data", "--force", "--timeout=5m"}).UninstallSteps([]corev1.StepNode{remaining[0]})
		if err != nil {
			return nil, err
		}
		op.Steps = append(op.Steps, steps...)
	}

	steps, err = k8s.KubeadmReset(stepNodes)
	if err != nil {
		return nil, err
	}
	op.Steps = append(op.Steps, steps...)
	op.Steps = append(op.Steps,
		k8s.DoCommandRemoveStep("removeEtcdDataDir", stepNodes, strutil.StringDefaultIfEmpty(k8s.EtcdDefaultDataDir, cluster.Etcd.DataDir)),
		k8s.DoCommandRemoveStep("removeKubeletDataDir", stepNodes, k8s.KubeletDefaultDataDir),
		k8s.DoCommandRemoveStep("removeDockershimDataDir", stepNodes, k8s.DockershimDefaultDataDir),
	)
	op.Steps = append(op.Steps, k8s.ClearVIPDomainSteps(*cluster, stepNodes)...)

	// rejoin phase as a worker: env setup, image loads, kubeadm join --token
	gen := k8s.GenNode{}
	if err = gen.InitStepper(&extra, cluster, common.NodeRoleWorker.String()).MakeInstallSteps(&extra, stepNodes, common.NodeRoleWorker.String()); err != nil {
		return nil, err
	}
	op.Steps = append(op.Steps, gen.GetSteps(corev1.ActionInstall)...)

	// reconcile the lvs care config on all workers: the demoted node becomes a
	// new backend consumer and the remaining workers must drop it as a master
	if cluster.Networking.WorkerNodeVip != "" && len(extra.Workers) > 0 {
		steps, err = k8s.LvsCareRefreshSteps(cluster, &extra, unwrapWorkerStepNodes(extra), nil)
		if err != nil {
			return nil, err
		}
		op.Steps = append(op.Steps, steps...)
	}
	return op, nil
}

func (p *PatchConvertNodes) targetStepNodes(extra component.ExtraMetadata) ([]corev1.StepNode, error) {
	var stepNodes []corev1.StepNode
	switch p.Role {
	case common.NodeRoleMaster:
		ips := extra.GetMasterNodeIP()
		clusterIPs := extra.GetMasterNodeClusterIP()
		for _, nodeID := range p.Nodes.GetNodeIDs() {
			stepNodes = append(stepNodes, corev1.StepNode{
				ID:       nodeID,
				IPv4:     ips[nodeID],
				NodeIPv4: clusterIPs[nodeID],
				Hostname: extra.GetMasterHostname(nodeID),
			})
		}
	case common.NodeRoleWorker:
		ips := extra.GetWorkerNodeIP()
		clusterIPs := extra.GetWorkerNodeClusterIP()
		for _, nodeID := range p.Nodes.GetNodeIDs() {
			stepNodes = append(stepNodes, corev1.StepNode{
				ID:       nodeID,
				IPv4:     ips[nodeID],
				NodeIPv4: clusterIPs[nodeID],
				Hostname: extra.GetWorkerHostname(nodeID),
			})
		}
	default:
		return nil, ErrInvalidNodesRole
	}
	for _, sn := range stepNodes {
		if sn.IPv4 == "" || sn.Hostname == "" {
			return nil, fmt.Errorf("convert node %q is missing agent IP or hostname metadata", sn.ID)
		}
	}
	return stepNodes, nil
}

func unwrapWorkerStepNodes(extra component.ExtraMetadata) []corev1.StepNode {
	stepNodes := make([]corev1.StepNode, 0, len(extra.Workers))
	workerIPs := extra.GetWorkerNodeIP()
	workerClusterIPs := extra.GetWorkerNodeClusterIP()
	for _, node := range extra.Workers {
		stepNodes = append(stepNodes, corev1.StepNode{
			ID:       node.ID,
			IPv4:     workerIPs[node.ID],
			NodeIPv4: workerClusterIPs[node.ID],
			Hostname: extra.GetWorkerHostname(node.ID),
		})
	}
	return stepNodes
}
