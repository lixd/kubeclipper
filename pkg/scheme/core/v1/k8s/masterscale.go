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

package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	"github.com/kubeclipper/kubeclipper/pkg/logger"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/utils/cmdutil"
	"github.com/kubeclipper/kubeclipper/pkg/utils/fileutil"
	"github.com/kubeclipper/kubeclipper/pkg/utils/ipvsutil"
	"github.com/kubeclipper/kubeclipper/pkg/utils/strutil"
)

var (
	_ component.StepRunnable = (*EtcdMemberRemove)(nil)
	_ component.StepRunnable = (*LvsCare)(nil)
)

const (
	etcdMemberRemove = "etcdMemberRemove"
	lvsCareRefresh   = "lvsCareRefresh"

	etcdCACertPath     = "/etc/kubernetes/pki/etcd/ca.crt"
	etcdServerCertPath = "/etc/kubernetes/pki/etcd/server.crt"
	etcdServerKeyPath  = "/etc/kubernetes/pki/etcd/server.key"
	etcdClientPort     = "2379"
	etcdPeerPort       = "2380"

	lvscareManifestFile = "/etc/kubernetes/manifests/kube-lvscare.yaml"
)

func init() {
	if err := component.RegisterAgentStep(fmt.Sprintf(component.RegisterStepKeyFormat, etcdMemberRemove, version, component.TypeStep), &EtcdMemberRemove{}); err != nil {
		panic(err)
	}
	if err := component.RegisterAgentStep(fmt.Sprintf(component.RegisterStepKeyFormat, lvsCareRefresh, version, component.TypeStep), &LvsCare{}); err != nil {
		panic(err)
	}
}

// EtcdMemberRemove removes the etcd members whose peer addresses match the
// given node IPs. It runs on a surviving control-plane node; kubeadm reset on
// the leaving node alone never shrinks the etcd membership.
type EtcdMemberRemove struct {
	// MemberIPs holds candidate addresses (access and cluster plane) of the
	// leaving nodes; etcd advertises the agent-detected address, so both are
	// matched against the peer URL.
	MemberIPs []string `json:"memberIPs"`
	// LocalIPs holds candidate addresses of the executing (surviving) node
	// used to build etcdctl client endpoints.
	LocalIPs []string `json:"localIPs"`
}

func (stepper *EtcdMemberRemove) NewInstance() component.ObjectMeta {
	return &EtcdMemberRemove{}
}

func (stepper *EtcdMemberRemove) Install(ctx context.Context, opts component.Options) ([]byte, error) {
	return nil, fmt.Errorf("EtcdMemberRemove does not support install")
}

func (stepper *EtcdMemberRemove) Uninstall(ctx context.Context, opts component.Options) ([]byte, error) {
	if opts.DryRun {
		return nil, nil
	}
	endpoints := make([]string, 0, len(stepper.LocalIPs))
	for _, ip := range stepper.LocalIPs {
		if ip != "" {
			endpoints = append(endpoints, fmt.Sprintf("https://%s:%s", ip, etcdClientPort))
		}
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no etcd client endpoint available to remove member")
	}
	base := fmt.Sprintf("ETCDCTL_API=3 etcdctl --endpoints=%s --cacert=%s --cert=%s --key=%s",
		strings.Join(endpoints, ","), etcdCACertPath, etcdServerCertPath, etcdServerKeyPath)

	ec, err := cmdutil.RunCmdWithContext(ctx, opts.DryRun, "bash", "-c", base+" member list")
	if err != nil {
		logger.Error("etcdctl member list error", zap.Error(err), zap.String("stderr", ec.StdErr()))
		return nil, err
	}
	for _, ip := range stepper.MemberIPs {
		if ip == "" {
			continue
		}
		id := findEtcdMemberID(ec.StdOut(), ip)
		if id == "" {
			// Retry after a partial failure may hit an already-removed member;
			// treat absence as converged instead of failing the operation.
			logger.Warnf("etcd member with peer address https://%s:%s not found, assume already removed", ip, etcdPeerPort)
			continue
		}
		if _, err := cmdutil.RunCmdWithContext(ctx, opts.DryRun, "bash", "-c",
			fmt.Sprintf("%s member remove %s", base, id)); err != nil {
			logger.Error("etcdctl member remove error", zap.Error(err), zap.String("memberID", id))
			return nil, err
		}
		logger.Infof("etcd member %s (peer %s) removed", id, ip)
	}
	return nil, nil
}

// findEtcdMemberID returns the etcd member ID whose peer URL points at the
// given node IP, or an empty string when no member matches. The member list
// table is comma-separated: memberID,status,name,peerURL,clientURL,isLearner.
func findEtcdMemberID(memberListStdout, peerIP string) string {
	prefix := fmt.Sprintf("https://%s:%s", peerIP, etcdPeerPort)
	for _, line := range strings.Split(memberListStdout, "\n") {
		if !strings.Contains(line, prefix) {
			continue
		}
		fields := strings.Split(strings.TrimSpace(line), ",")
		if len(fields) > 0 && strings.TrimSpace(fields[0]) != "" {
			return strings.TrimSpace(fields[0])
		}
	}
	return ""
}

// LvsCare reconciles the API server virtual service on worker nodes after the
// master set changed: it re-programs the IPVS real servers and re-renders the
// kube-lvscare static pod with the new master list. When only one master
// remains the care pod and the virtual service are removed, mirroring the
// creation path which never installs them for single-master clusters.
type LvsCare struct {
	WorkerNodeVIP string            `json:"workerNodeVip"`
	Masters       map[string]string `json:"masters"`
	ImageRegistry string            `json:"imageRegistry,omitempty"`
}

func (stepper *LvsCare) NewInstance() component.ObjectMeta {
	return &LvsCare{}
}

func (stepper *LvsCare) Install(ctx context.Context, opts component.Options) ([]byte, error) {
	return nil, stepper.reconcile(ctx, opts)
}

func (stepper *LvsCare) Uninstall(ctx context.Context, opts component.Options) ([]byte, error) {
	return nil, stepper.reconcile(ctx, opts)
}

func (stepper *LvsCare) reconcile(ctx context.Context, opts component.Options) error {
	if stepper.WorkerNodeVIP == "" {
		return nil
	}
	if len(stepper.Masters) > 1 {
		var rsList []ipvsutil.RealServer
		for _, ip := range stepper.Masters {
			rsList = append(rsList, ipvsutil.RealServer{Address: ip, Port: 6443})
		}
		vs := ipvsutil.VirtualServer{
			Address:     stepper.WorkerNodeVIP,
			Port:        6443,
			RealServers: rsList,
		}
		if err := ipvsutil.Clear(opts.DryRun); err != nil {
			logger.Warnf("ipvs clear service error info: %v", err)
		}
		if err := ipvsutil.CreateIPVS(&vs, opts.DryRun); err != nil {
			return err
		}
		care := &ClusterNode{
			WorkerNodeVIP: stepper.WorkerNodeVIP,
			Masters:       stepper.Masters,
			ImageRegistry: stepper.ImageRegistry,
		}
		if err := os.MkdirAll(filepath.Dir(lvscareManifestFile), 0755); err != nil {
			return err
		}
		return fileutil.WriteFileWithContext(ctx, lvscareManifestFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644,
			care.renderIPVSCarePod, opts.DryRun)
	}
	if err := ipvsutil.Clear(opts.DryRun); err != nil {
		logger.Warnf("ipvs clear service error info: %v", err)
	}
	_, err := cmdutil.RunCmdWithContext(ctx, opts.DryRun, "bash", "-c", fmt.Sprintf("rm -f %s", lvscareManifestFile))
	return err
}

// EtcdMemberRemoveSteps builds the step that removes the leaving control-plane
// nodes from the etcd membership; the step runs on a surviving master.
func EtcdMemberRemoveSteps(local v1.StepNode, leaving []v1.StepNode) ([]v1.Step, error) {
	payload := EtcdMemberRemove{
		MemberIPs: nodeCandidateIPs(leaving),
		LocalIPs:  []string{local.IPv4, local.NodeIPv4},
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return []v1.Step{
		{
			ID:         strutil.GetUUID(),
			Name:       "removeEtcdMember",
			Timeout:    metav1.Duration{Duration: 5 * time.Minute},
			ErrIgnore:  false,
			RetryTimes: 2,
			Nodes:      []v1.StepNode{local},
			Action:     v1.ActionUninstall,
			Commands: []v1.Command{
				{
					Type:          v1.CommandCustom,
					Identity:      fmt.Sprintf(component.RegisterStepKeyFormat, etcdMemberRemove, version, component.TypeStep),
					CustomCommand: bytes,
				},
			},
		},
	}, nil
}

// LvsCareRefreshSteps builds the step that re-programs the API server virtual
// service on the given worker nodes after the master set changed. leaving
// holds the nodes being removed: they are still part of the extra metadata
// (appended back for IP resolution), so their addresses must be filtered out
// of the real server list.
func LvsCareRefreshSteps(cluster *v1.Cluster, metadata *component.ExtraMetadata, nodes, leaving []v1.StepNode) ([]v1.Step, error) {
	masters := metadata.GetMasterNodeIP()
	if len(leaving) > 0 {
		exclude := make(map[string]struct{}, len(leaving))
		for _, n := range leaving {
			exclude[n.ID] = struct{}{}
		}
		filtered := make(map[string]string, len(masters))
		for id, ip := range masters {
			if _, ok := exclude[id]; ok {
				continue
			}
			filtered[id] = ip
		}
		masters = filtered
	}
	payload := LvsCare{
		WorkerNodeVIP: cluster.Networking.WorkerNodeVip,
		Masters:       masters,
		ImageRegistry: cluster.ResolvedImageRegistry,
	}
	bytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return []v1.Step{
		{
			ID:         strutil.GetUUID(),
			Name:       "refreshLvsCare",
			Timeout:    metav1.Duration{Duration: 2 * time.Minute},
			ErrIgnore:  false,
			RetryTimes: 1,
			Nodes:      nodes,
			// both Install and Uninstall reconcile to the desired master set,
			// so the same action serves the add and the remove operation
			Action: v1.ActionInstall,
			Commands: []v1.Command{
				{
					Type:          v1.CommandCustom,
					Identity:      fmt.Sprintf(component.RegisterStepKeyFormat, lvsCareRefresh, version, component.TypeStep),
					CustomCommand: bytes,
				},
			},
		},
	}, nil
}

// nodeCandidateIPs collects the deduplicated candidate addresses of the given
// step nodes.
func nodeCandidateIPs(nodes []v1.StepNode) []string {
	ips := make([]string, 0, 2*len(nodes))
	seen := make(map[string]struct{}, 2*len(nodes))
	for _, n := range nodes {
		for _, ip := range []string{n.IPv4, n.NodeIPv4} {
			if ip == "" {
				continue
			}
			if _, ok := seen[ip]; ok {
				continue
			}
			seen[ip] = struct{}{}
			ips = append(ips, ip)
		}
	}
	return ips
}
