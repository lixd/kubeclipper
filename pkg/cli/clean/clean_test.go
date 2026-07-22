package clean

import (
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"
)

func TestMergeOnlineAgentsIncludesJoinedNodes(t *testing.T) {
	config := options.NewDeployOptions()
	config.Agents.Add("10.0.0.1", options.Metadata{AgentID: "deployed"})
	nodes := &kc.NodesList{Items: []v1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "deployed"}, Status: v1.NodeStatus{Ipv4DefaultIP: "10.0.0.1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "joined"}, Status: v1.NodeStatus{NodeIpv4DefaultIP: "10.0.0.2"}},
	}}

	if err := mergeOnlineAgents(config, nodes); err != nil {
		t.Fatalf("mergeOnlineAgents() error: %v", err)
	}
	if got, want := config.Agents.ListIP(), []string{"10.0.0.1", "10.0.0.2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("agents = %v, want %v", got, want)
	}
}

func TestAgentSSHConfigUsesJoinTransport(t *testing.T) {
	deploySSH := &sshutils.SSH{User: "server", PkFile: "/server/key"}
	agentSSH := &sshutils.SSH{User: "agent", PkFile: "/agent/key"}
	o := NewCleanOptions(options.IOStreams{})
	o.deployConfig.SSHConfig = deploySSH
	o.deployConfig.AgentSSHConfig = agentSSH

	if got := o.agentSSHConfig(); got != agentSSH {
		t.Fatalf("agentSSHConfig() = %+v, want join transport %+v", got, agentSSH)
	}
	o.deployConfig.AgentSSHConfig = nil
	if got := o.agentSSHConfig(); got != deploySSH {
		t.Fatalf("agentSSHConfig() fallback = %+v, want deploy transport %+v", got, deploySSH)
	}
}

func TestAgentHostsByTransportKeepsAIOServerOnServerSSH(t *testing.T) {
	o := NewCleanOptions(options.IOStreams{})
	o.deployConfig.ServerIPs = []string{"10.0.0.1"}
	o.deployConfig.Agents = options.Agents{
		"10.0.0.1": {AgentID: "aio"},
		"10.0.0.2": {AgentID: "joined"},
	}

	serverAgents, joinedAgents := o.agentHostsByTransport()
	if !reflect.DeepEqual(serverAgents, []string{"10.0.0.1"}) || !reflect.DeepEqual(joinedAgents, []string{"10.0.0.2"}) {
		t.Fatalf("server agents = %v, joined agents = %v", serverAgents, joinedAgents)
	}
}

func TestMergeOnlineAgentsRejectsNodeWithoutAddress(t *testing.T) {
	config := options.NewDeployOptions()
	if err := mergeOnlineAgents(config, &kc.NodesList{Items: []v1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "unknown"}}}}); err == nil {
		t.Fatal("mergeOnlineAgents() unexpectedly accepted node without an address")
	}
}

func TestCleanupCommandsRemovePackageRegistryCredentials(t *testing.T) {
	agentCommands := strings.Join(agentCleanupCommands("/var/log/kc-agent"), "\n")
	if !strings.Contains(agentCommands, "rm -rf /etc/kubeclipper-agent") {
		t.Fatal("agent cleanup does not remove package Registry credential directory")
	}
	serverCommands := strings.Join(serverCleanupCommands("/var/lib/kc-etcd"), "\n")
	if !strings.Contains(serverCommands, "rm -rf /etc/kubeclipper-server") {
		t.Fatal("server cleanup does not remove package Registry credential directory")
	}
}
