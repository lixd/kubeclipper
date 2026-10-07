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
	"encoding/json"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func convertTestCluster() *corev1.Cluster {
	return &corev1.Cluster{
		Masters: corev1.WorkerNodeList{
			{ID: "master-1"},
		},
		Workers: corev1.WorkerNodeList{
			{ID: "worker-1"},
			{ID: "worker-2"},
		},
	}
}

func TestPatchConvertNodesMakeCompare(t *testing.T) {
	t.Run("promote moves worker to masters", func(t *testing.T) {
		clu := convertTestCluster()
		pcn := &PatchConvertNodes{Role: common.NodeRoleMaster, Nodes: corev1.WorkerNodeList{{ID: "worker-1"}}}
		if err := pcn.MakeCompare(clu); err != nil {
			t.Fatalf("MakeCompare() error = %v", err)
		}
		if len(clu.Masters) != 2 || clu.Masters.GetNodeIDs()[1] != "worker-1" {
			t.Fatalf("masters after promote = %v", clu.Masters.GetNodeIDs())
		}
		if len(clu.Workers) != 1 || clu.Workers.GetNodeIDs()[0] != "worker-2" {
			t.Fatalf("workers after promote = %v", clu.Workers.GetNodeIDs())
		}
	})

	t.Run("demote moves master back to workers", func(t *testing.T) {
		clu := convertTestCluster()
		clu.Masters = append(clu.Masters, corev1.WorkerNode{ID: "master-2"})
		pcn := &PatchConvertNodes{Role: common.NodeRoleWorker, Nodes: corev1.WorkerNodeList{{ID: "master-2"}}}
		if err := pcn.MakeCompare(clu); err != nil {
			t.Fatalf("MakeCompare() error = %v", err)
		}
		if len(clu.Masters) != 1 || clu.Masters.GetNodeIDs()[0] != "master-1" {
			t.Fatalf("masters after demote = %v", clu.Masters.GetNodeIDs())
		}
		if len(clu.Workers) != 3 {
			t.Fatalf("workers after demote = %v", clu.Workers.GetNodeIDs())
		}
	})

	t.Run("demoting the last master is refused", func(t *testing.T) {
		clu := convertTestCluster()
		pcn := &PatchConvertNodes{Role: common.NodeRoleWorker, Nodes: corev1.WorkerNodeList{{ID: "master-1"}}}
		err := pcn.MakeCompare(clu)
		if err == nil || !strings.Contains(err.Error(), "control plane") {
			t.Fatalf("MakeCompare() error = %v, want control plane guard", err)
		}
		// the refused conversion must not mutate the cluster
		if len(clu.Masters) != 1 || len(clu.Workers) != 2 {
			t.Fatalf("cluster mutated by refused conversion: masters=%v workers=%v", clu.Masters.GetNodeIDs(), clu.Workers.GetNodeIDs())
		}
	})

	t.Run("converting a node already in the target role is refused", func(t *testing.T) {
		clu := convertTestCluster()
		pcn := &PatchConvertNodes{Role: common.NodeRoleMaster, Nodes: corev1.WorkerNodeList{{ID: "master-1"}}}
		if err := pcn.MakeCompare(clu); err == nil {
			t.Fatal("MakeCompare() expected error when no worker matches")
		}
	})

	t.Run("invalid role is refused", func(t *testing.T) {
		pcn := &PatchConvertNodes{Role: "bird", Nodes: corev1.WorkerNodeList{{ID: "worker-1"}}}
		if err := pcn.Validate(); err == nil {
			t.Fatal("Validate() expected error for invalid role")
		}
	})

	t.Run("empty node list is refused", func(t *testing.T) {
		pcn := &PatchConvertNodes{Role: common.NodeRoleMaster}
		if err := pcn.Validate(); err != ErrZeroNode {
			t.Fatalf("Validate() error = %v, want ErrZeroNode", err)
		}
	})
}

func TestPatchConvertNodesMoveExtra(t *testing.T) {
	resolved := []component.Node{
		{ID: "worker-1", IPv4: "10.0.0.2", NodeIPv4: "192.168.0.2", Hostname: "w1"},
	}
	t.Run("promote relocates from pre-operation revision", func(t *testing.T) {
		extra := &component.ExtraMetadata{
			Masters: []component.Node{
				{ID: "master-1", IPv4: "10.0.0.1", NodeIPv4: "192.168.0.1", Hostname: "m1"},
			},
			Workers: []component.Node{
				{ID: "worker-1", IPv4: "10.0.0.2", NodeIPv4: "192.168.0.2", Hostname: "w1"},
				{ID: "worker-2", IPv4: "10.0.0.3", NodeIPv4: "192.168.0.3", Hostname: "w2"},
			},
		}
		pcn := &PatchConvertNodes{Role: common.NodeRoleMaster,
			Nodes:        corev1.WorkerNodeList{{ID: "worker-1"}},
			ConvertNodes: resolved}
		if err := pcn.moveExtra(extra); err != nil {
			t.Fatalf("moveExtra() error = %v", err)
		}
		if len(extra.Masters) != 2 || extra.Masters[1].ID != "worker-1" {
			t.Fatalf("masters after move = %v", extra.Masters)
		}
		if len(extra.Workers) != 1 || extra.Workers[0].ID != "worker-2" {
			t.Fatalf("workers after move = %v", extra.Workers)
		}
		// the promote step builder resolves the joining node's IP from the master maps
		ips := extra.GetMasterNodeIP()
		if ips["worker-1"] != "10.0.0.2" {
			t.Fatalf("GetMasterNodeIP()[worker-1] = %q, want 10.0.0.2", ips["worker-1"])
		}
	})

	t.Run("promote works from post-operation revision too", func(t *testing.T) {
		// the storage Get ignores historical resource versions, so the extra
		// may already carry the node on the target side; moveExtra must not
		// duplicate it
		extra := &component.ExtraMetadata{
			Masters: []component.Node{
				{ID: "master-1", IPv4: "10.0.0.1", Hostname: "m1"},
				{ID: "worker-1", IPv4: "10.0.0.2", Hostname: "w1"},
			},
		}
		pcn := &PatchConvertNodes{Role: common.NodeRoleMaster,
			Nodes:        corev1.WorkerNodeList{{ID: "worker-1"}},
			ConvertNodes: resolved}
		if err := pcn.moveExtra(extra); err != nil {
			t.Fatalf("moveExtra() error = %v", err)
		}
		count := 0
		for _, m := range extra.Masters {
			if m.ID == "worker-1" {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("worker-1 appears %d times in masters, want exactly 1", count)
		}
	})

	t.Run("demote moves master back to workers", func(t *testing.T) {
		extra := &component.ExtraMetadata{
			Masters: []component.Node{
				{ID: "master-1", IPv4: "10.0.0.1", Hostname: "m1"},
				{ID: "master-2", IPv4: "10.0.0.2", Hostname: "m2"},
			},
			Workers: []component.Node{},
		}
		pcn := &PatchConvertNodes{Role: common.NodeRoleWorker,
			Nodes:        corev1.WorkerNodeList{{ID: "master-2"}},
			ConvertNodes: []component.Node{{ID: "master-2", IPv4: "10.0.0.2", Hostname: "m2"}}}
		if err := pcn.moveExtra(extra); err != nil {
			t.Fatalf("moveExtra() error = %v", err)
		}
		if len(extra.Masters) != 1 || extra.Masters[0].ID != "master-1" {
			t.Fatalf("masters after demote move = %v", extra.Masters)
		}
		if len(extra.Workers) != 1 || extra.Workers[0].ID != "master-2" {
			t.Fatalf("workers after demote move = %v", extra.Workers)
		}
	})

	t.Run("missing resolved info is refused", func(t *testing.T) {
		pcn := &PatchConvertNodes{Role: common.NodeRoleMaster,
			Nodes: corev1.WorkerNodeList{{ID: "worker-1"}}}
		if err := pcn.moveExtra(&component.ExtraMetadata{}); err == nil {
			t.Fatal("moveExtra() expected error when ConvertNodes is empty")
		}
	})
}

func TestConvertOperationRoundTrip(t *testing.T) {
	// the pending operation ExtraData must round-trip through JSON the same
	// way the handler stores it and the builder reads it back
	pcn := &PatchConvertNodes{Role: common.NodeRoleWorker, Nodes: corev1.WorkerNodeList{{ID: "master-2"}}}
	raw, err := json.Marshal(pcn)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back PatchConvertNodes
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.Role != common.NodeRoleWorker || len(back.Nodes) != 1 || back.Nodes.GetNodeIDs()[0] != "master-2" {
		t.Fatalf("round trip mismatch: %+v", back)
	}
}
