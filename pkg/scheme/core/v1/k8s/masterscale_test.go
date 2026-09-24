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
	"encoding/json"
	"net"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func Test_extractCertificateKey(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		want    string
		wantErr bool
	}{
		{
			name: "key on the same line",
			output: `[upload-certs] Storing the certificates in Secret "kubeadm-certs" in the "kube-system" Namespace
[upload-certs] Using certificate key: 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef`,
			want: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
		{
			name: "key on the following line",
			output: `[upload-certs] Using certificate key:
46518267766fc19772ecc334c13190f8131f1bf48a213538879f6427f74fe8e2`,
			want: "46518267766fc19772ecc334c13190f8131f1bf48a213538879f6427f74fe8e2",
		},
		{
			name:    "no certificate key",
			output:  `[upload-certs] failed to upload certs: stage runners failed`,
			wantErr: true,
		},
		{
			name:    "empty output",
			output:  "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractCertificateKey(tt.output)
			if (err != nil) != tt.wantErr {
				t.Fatalf("extractCertificateKey() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("extractCertificateKey() = %q, want %q", got, tt.want)
			}
		})
	}
}

func Test_findEtcdMemberID(t *testing.T) {
	memberList := `4b9a7f19,started,kc-1,https://172.16.131.146:2380,https://172.16.131.146:2379,false
a1b2c3d4,started,kc-2,https://172.16.131.230:2380,https://172.16.131.230:2379,false`
	if got := findEtcdMemberID(memberList, "172.16.131.146"); got != "4b9a7f19" {
		t.Fatalf("findEtcdMemberID() = %q, want 4b9a7f19", got)
	}
	if got := findEtcdMemberID(memberList, "172.16.131.230"); got != "a1b2c3d4" {
		t.Fatalf("findEtcdMemberID() = %q, want a1b2c3d4", got)
	}
	if got := findEtcdMemberID(memberList, "172.16.131.208"); got != "" {
		t.Fatalf("findEtcdMemberID() = %q, want empty for absent member", got)
	}
	if got := findEtcdMemberID("", "172.16.131.146"); got != "" {
		t.Fatalf("findEtcdMemberID() = %q, want empty on empty list", got)
	}
}

// masterStepFixtureCluster builds the minimal cluster fixture for step-plan
// construction; nothing here talks to a real cluster.
func masterStepFixtureCluster() *v1.Cluster {
	c := &v1.Cluster{}
	c.Name = "master-scale"
	c.KubernetesVersion = "v1.33.0"
	c.ContainerRuntime.Type = "containerd"
	c.Etcd.DataDir = "/var/lib/etcd"
	c.CNI.Type = "calico"
	c.CNI.Version = "v3.21.2"
	c.CNI.Calico = &v1.Calico{Mode: "Overlay-Vxlan-All"}
	c.Networking.IPFamily = v1.IPFamilyIPv4
	c.Networking.Services.CIDRBlocks = []string{"10.96.0.0/16"}
	c.Networking.Pods.CIDRBlocks = []string{"172.25.0.0/24"}
	c.Networking.DNSDomain = "cluster.local"
	c.Networking.ProxyMode = "ipvs"
	c.Networking.WorkerNodeVip = "169.254.169.100"
	c.ResolvedImageRegistry = "kc-package-registry"
	return c
}

func masterStepFixtureMetadata() *component.ExtraMetadata {
	return &component.ExtraMetadata{
		ClusterName: "master-scale",
		KubeVersion: "v1.33.0",
		Masters: []component.Node{
			{ID: "joining", IPv4: "127.0.0.1", NodeIPv4: "127.0.0.1", Hostname: "joining"},
			{ID: "surviving", IPv4: "127.0.0.1", NodeIPv4: "127.0.0.1", Hostname: "surviving"},
		},
		Workers: []component.Node{
			{ID: "worker-1", IPv4: "192.0.2.10", NodeIPv4: "192.0.2.10", Hostname: "worker-1"},
		},
	}
}

// availableMasterListener satisfies the AvailableKubeMasters TCP probe on the
// loopback address used by the fixtures.
func availableMasterListener(t *testing.T) *net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:6443")
	if err != nil {
		t.Fatalf("cannot bind the fake apiserver port: %v", err)
	}
	return &l
}

func stepNames(steps []v1.Step) []string {
	names := make([]string, 0, len(steps))
	for _, s := range steps {
		names = append(names, s.Name)
	}
	return names
}

func assertContainsStep(t *testing.T, steps []v1.Step, name string) {
	t.Helper()
	for _, s := range steps {
		if s.Name == name {
			return
		}
	}
	t.Fatalf("step %q not found in plan: %v", name, stepNames(steps))
}

func TestGenNodeMasterInstallSteps(t *testing.T) {
	l := availableMasterListener(t)
	defer func() { _ = (*l).Close() }()

	cluster := masterStepFixtureCluster()
	metadata := masterStepFixtureMetadata()
	patchNodes := []v1.StepNode{{ID: "joining", IPv4: "127.0.0.1", NodeIPv4: "127.0.0.1", Hostname: "joining"}}

	gen := &GenNode{}
	if err := gen.InitStepper(metadata, cluster, NodeRoleMaster).MakeInstallSteps(metadata, patchNodes, NodeRoleMaster); err != nil {
		t.Fatal(err)
	}
	steps := gen.GetSteps(v1.ActionInstall)
	assertContainsStep(t, steps, "getJoinCommand")
	assertContainsStep(t, steps, "renderMasterJoinConfig")
	assertContainsStep(t, steps, "joinNode")
	assertContainsStep(t, steps, "waitForAddedNodesReady")
	assertContainsStep(t, steps, "refreshLvsCare")

	// the join command must be generated on an existing control-plane member,
	// never on the node being joined
	getJoin := findStep(t, steps, "getJoinCommand")
	if len(getJoin.Nodes) != 1 || getJoin.Nodes[0].ID != "surviving" {
		t.Fatalf("getJoinCommand must run on the surviving master, got %v", getJoin.Nodes)
	}

	// the join config must be the control-plane variant carrying a certificate key
	joinConfig := findStep(t, steps, "renderMasterJoinConfig")
	var payload KubeadmConfig
	if err := json.Unmarshal(joinConfig.Commands[0].CustomCommand, &payload); err != nil {
		t.Fatal(err)
	}
	if !payload.IsControlPlane {
		t.Fatal("master scale-up must render the control-plane join config")
	}

	lvs := findStep(t, steps, "refreshLvsCare")
	if lvs.Action != v1.ActionInstall {
		t.Fatalf("refreshLvsCare action = %q, want %q", lvs.Action, v1.ActionInstall)
	}
	if len(lvs.Nodes) != 1 || lvs.Nodes[0].ID != "worker-1" {
		t.Fatalf("refreshLvsCare must run on the workers, got %v", lvs.Nodes)
	}
	var lvsPayload LvsCare
	if err := json.Unmarshal(lvs.Commands[0].CustomCommand, &lvsPayload); err != nil {
		t.Fatal(err)
	}
	if lvsPayload.WorkerNodeVIP != "169.254.169.100" {
		t.Fatalf("refreshLvsCare vip = %q, want 169.254.169.100", lvsPayload.WorkerNodeVIP)
	}
	if _, ok := lvsPayload.Masters["joining"]; !ok {
		t.Fatalf("refreshLvsCare must include the joined master: %v", lvsPayload.Masters)
	}
}

func TestGenNodeMasterUninstallSteps(t *testing.T) {
	l := availableMasterListener(t)
	defer func() { _ = (*l).Close() }()

	cluster := masterStepFixtureCluster()
	metadata := masterStepFixtureMetadata()
	patchNodes := []v1.StepNode{{ID: "joining", IPv4: "127.0.0.1", NodeIPv4: "127.0.0.1", Hostname: "joining"}}

	gen := &GenNode{}
	if err := gen.InitStepper(metadata, cluster, NodeRoleMaster).MakeUninstallSteps(metadata, patchNodes); err != nil {
		t.Fatal(err)
	}
	steps := gen.GetSteps(v1.ActionUninstall)
	assertContainsStep(t, steps, "removeEtcdMember")
	assertContainsStep(t, steps, "drainNode")
	assertContainsStep(t, steps, "kubeadmReset")
	assertContainsStep(t, steps, "removeEtcdDataDir")
	assertContainsStep(t, steps, "clearVIPDomain")
	assertContainsStep(t, steps, "refreshLvsCare")

	// etcd membership must be shrunk from a surviving control-plane member
	// while the leaving node is still alive
	etcd := findStep(t, steps, "removeEtcdMember")
	if etcd.Action != v1.ActionUninstall {
		t.Fatalf("removeEtcdMember action = %q, want %q", etcd.Action, v1.ActionUninstall)
	}
	if len(etcd.Nodes) != 1 || etcd.Nodes[0].ID != "surviving" {
		t.Fatalf("removeEtcdMember must run on the surviving master, got %v", etcd.Nodes)
	}
	var etcdPayload EtcdMemberRemove
	if err := json.Unmarshal(etcd.Commands[0].CustomCommand, &etcdPayload); err != nil {
		t.Fatal(err)
	}
	if len(etcdPayload.MemberIPs) != 1 || etcdPayload.MemberIPs[0] != "127.0.0.1" {
		t.Fatalf("removeEtcdMember member IPs = %v, want the leaving node address", etcdPayload.MemberIPs)
	}

	reset := findStep(t, steps, "removeEtcdDataDir")
	if len(reset.Nodes) != 1 || reset.Nodes[0].ID != "joining" {
		t.Fatalf("removeEtcdDataDir must run on the leaving node, got %v", reset.Nodes)
	}
	var shell v1.Command
	if reset.Commands[0].Type != v1.CommandShell {
		t.Fatalf("removeEtcdDataDir command type = %q, want shell", reset.Commands[0].Type)
	}
	shell = reset.Commands[0]
	if !strings.Contains(shell.ShellCommand[2], "/var/lib/etcd") {
		t.Fatalf("removeEtcdDataDir must clean the etcd data dir: %q", shell.ShellCommand[2])
	}

	lvs := findStep(t, steps, "refreshLvsCare")
	if len(lvs.Nodes) != 1 || lvs.Nodes[0].ID != "worker-1" {
		t.Fatalf("refreshLvsCare must run on the workers, got %v", lvs.Nodes)
	}
	var lvsPayload LvsCare
	if err := json.Unmarshal(lvs.Commands[0].CustomCommand, &lvsPayload); err != nil {
		t.Fatal(err)
	}
	if _, ok := lvsPayload.Masters["joining"]; ok {
		t.Fatalf("refreshLvsCare must not keep the leaving master: %v", lvsPayload.Masters)
	}
}

func TestGenNodeMasterUninstallRequiresExistingControlPlane(t *testing.T) {
	cluster := masterStepFixtureCluster()
	metadata := masterStepFixtureMetadata()
	// the only master is the node being removed: no 6443 in the test sandbox
	// anyway, so the plan must refuse instead of targeting the leaving node
	metadata.Masters = []component.Node{
		{ID: "joining", IPv4: "192.0.2.1", NodeIPv4: "192.0.2.1", Hostname: "joining"},
	}
	patchNodes := []v1.StepNode{{ID: "joining", IPv4: "192.0.2.1", NodeIPv4: "192.0.2.1", Hostname: "joining"}}

	gen := &GenNode{}
	if err := gen.InitStepper(metadata, cluster, NodeRoleMaster).MakeUninstallSteps(metadata, patchNodes); err == nil {
		t.Fatal("uninstall plan must fail when no control-plane member remains to serve the steps")
	}
}

func findStep(t *testing.T, steps []v1.Step, name string) v1.Step {
	t.Helper()
	for _, s := range steps {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("step %q not found in plan: %v", name, stepNames(steps))
	return v1.Step{}
}
