package clean

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v2"
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

func cleanTransportFixture() *options.DeployConfig {
	deploySSH := &sshutils.SSH{User: "deploy", PkFile: "/keys/deploy"}
	config := options.NewDeployOptions()
	config.SSHConfig = deploySSH
	config.ServerIPs = []string{"10.0.0.1"}
	config.Agents = options.Agents{
		"10.0.0.1": {AgentID: "aio", SSHTransportID: options.SSHTransportIDDeploy},
		"10.0.0.2": {AgentID: "initial", SSHTransportID: options.SSHTransportIDDeploy},
		"10.0.0.3": {AgentID: "joined-a", SSHTransportID: "join-a"},
		"10.0.0.4": {AgentID: "joined-b", SSHTransportID: "join-b"},
	}
	config.SSHTransports = options.SSHTransports{
		options.SSHTransportIDDeploy: deploySSH,
		"join-a":                     {User: "agent-a", PkFile: "/keys/a"},
		"join-b":                     {User: "agent-b", PkFile: "/keys/b"},
	}
	return config
}

func TestAgentTransportGroupsKeepInitialAndJoinedCredentials(t *testing.T) {
	o := NewCleanOptions(options.IOStreams{})
	o.deployConfig = cleanTransportFixture()

	groups, err := o.agentTransportGroups(true)
	if err != nil {
		t.Fatalf("agentTransportGroups() error = %v", err)
	}
	if got, want := len(groups), 3; got != want {
		t.Fatalf("transport group count = %d, want %d: %+v", got, want, groups)
	}
	want := map[string]struct {
		key   string
		hosts []string
	}{
		options.SSHTransportIDDeploy: {key: "/keys/deploy", hosts: []string{"10.0.0.1", "10.0.0.2"}},
		"join-a":                     {key: "/keys/a", hosts: []string{"10.0.0.3"}},
		"join-b":                     {key: "/keys/b", hosts: []string{"10.0.0.4"}},
	}
	for _, group := range groups {
		expected := want[group.id]
		if group.sshConfig.PkFile != expected.key || !reflect.DeepEqual(group.hosts, expected.hosts) {
			t.Fatalf("group %q = key %q hosts %v, want %+v", group.id, group.sshConfig.PkFile, group.hosts, expected)
		}
	}
}

func TestAgentTransportGroupsHonorTransportUsedToJoinServerNode(t *testing.T) {
	o := NewCleanOptions(options.IOStreams{})
	o.deployConfig = cleanTransportFixture()
	metadata := o.deployConfig.Agents["10.0.0.1"]
	metadata.SSHTransportID = "join-a"
	o.deployConfig.Agents["10.0.0.1"] = metadata

	groups, err := o.agentTransportGroups(true)
	if err != nil {
		t.Fatalf("agentTransportGroups() error = %v", err)
	}
	for _, group := range groups {
		if group.id == "join-a" {
			if !reflect.DeepEqual(group.hosts, []string{"10.0.0.1", "10.0.0.3"}) || group.sshConfig.PkFile != "/keys/a" {
				t.Fatalf("joined server transport group = %+v", group)
			}
			return
		}
	}
	t.Fatal("join-a transport group not found")
}

func TestCleanPrecheckAgentsAndBinariesRunPerTransport(t *testing.T) {
	o := NewCleanOptions(options.IOStreams{})
	o.deployConfig = cleanTransportFixture()
	o.allNodes = []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"}

	prechecks := make(map[string][]string)
	o.sudoPreCheck = func(name string, config *sshutils.SSH, _ options.IOStreams, hosts []string) bool {
		prechecks[name+":"+config.PkFile] = append([]string(nil), hosts...)
		return true
	}
	if !o.preCheck() {
		t.Fatal("preCheck() = false")
	}
	for key, hosts := range map[string][]string{
		"server sudo:/keys/deploy":         {"10.0.0.1"},
		"agent sudo (deploy):/keys/deploy": {"10.0.0.1", "10.0.0.2"},
		"agent sudo (join-a):/keys/a":      {"10.0.0.3"},
		"agent sudo (join-b):/keys/b":      {"10.0.0.4"},
	} {
		if !reflect.DeepEqual(prechecks[key], hosts) {
			t.Fatalf("precheck %q hosts = %v, want %v", key, prechecks[key], hosts)
		}
	}

	cleanups := make(map[string][]string)
	o.remoteCleanup = func(config *sshutils.SSH, hosts []string, component string, _ []string) error {
		cleanups[component+":"+config.PkFile] = append([]string(nil), hosts...)
		return nil
	}
	if err := o.cleanKcAgent(); err != nil {
		t.Fatalf("cleanKcAgent() error = %v", err)
	}
	if err := o.cleanBinaries(); err != nil {
		t.Fatalf("cleanBinaries() error = %v", err)
	}
	for key, hosts := range map[string][]string{
		"kc agent (deploy):/keys/deploy":       {"10.0.0.1", "10.0.0.2"},
		"kc agent (join-a):/keys/a":            {"10.0.0.3"},
		"kc agent (join-b):/keys/b":            {"10.0.0.4"},
		"server binaries:/keys/deploy":         {"10.0.0.1"},
		"agent binaries (deploy):/keys/deploy": {"10.0.0.2"},
		"agent binaries (join-a):/keys/a":      {"10.0.0.3"},
		"agent binaries (join-b):/keys/b":      {"10.0.0.4"},
	} {
		if !reflect.DeepEqual(cleanups[key], hosts) {
			t.Fatalf("cleanup %q hosts = %v, want %v", key, cleanups[key], hosts)
		}
	}
}

func TestMergeAndPersistOnlineAgentsWritesPrivateRecoveryInventory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deploy-config.yaml")
	if err := os.WriteFile(path, []byte("old"), 0644); err != nil { //nolint:gosec // Legacy mode is repaired by the write under test.
		t.Fatal(err)
	}
	config := options.NewDeployOptions()
	config.Config = path
	config.SSHConfig = &sshutils.SSH{User: "deploy", PkFile: "/keys/deploy"}
	config.AgentSSHConfig = &sshutils.SSH{User: "joined", PkFile: "/keys/joined"}
	config.Agents.Add("10.0.0.1", options.Metadata{AgentID: "deployed"})
	nodes := &kc.NodesList{Items: []v1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "deployed"}, Status: v1.NodeStatus{Ipv4DefaultIP: "10.0.0.1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "recovered"}, Status: v1.NodeStatus{Ipv4DefaultIP: "10.0.0.2"}},
	}}
	if err := mergeAndPersistOnlineAgents(config, nodes); err != nil {
		t.Fatalf("mergeAndPersistOnlineAgents() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("persisted inventory mode = %#o, want 0600", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var persisted options.DeployConfig
	if err = yaml.Unmarshal(data, &persisted); err != nil {
		t.Fatalf("unmarshal persisted inventory: %v", err)
	}
	metadata, ok := persisted.Agents["10.0.0.2"]
	if !ok || metadata.AgentID != "recovered" || metadata.SSHTransportID != options.SSHTransportIDLegacyAgent {
		t.Fatalf("persisted recovered agent = %+v, exists %t", metadata, ok)
	}
}

func TestRunCleanKeepsRecoveryInventoryUntilRemoteCleanupSucceeds(t *testing.T) {
	o := NewCleanOptions(options.IOStreams{})
	o.deployConfig = cleanTransportFixture()
	o.allNodes = []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.4"}
	o.cleanAll = true
	localRemoved := false
	o.localConfigCleanup = func() error {
		localRemoved = true
		return nil
	}
	o.remoteCleanup = func(_ *sshutils.SSH, _ []string, component string, _ []string) error {
		if strings.Contains(component, "join-a") {
			return errors.New("injected cleanup failure")
		}
		return nil
	}
	if err := o.RunClean(); err == nil {
		t.Fatal("RunClean() error = nil")
	}
	if localRemoved {
		t.Fatal("local recovery inventory was removed after remote cleanup failed")
	}

	o.remoteCleanup = func(*sshutils.SSH, []string, string, []string) error { return nil }
	if err := o.RunClean(); err != nil {
		t.Fatalf("RunClean() retry error = %v", err)
	}
	if !localRemoved {
		t.Fatal("local recovery inventory was retained after every cleanup succeeded")
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
	if !strings.Contains(agentCommands, "rm -rf /opt/kc/manifest") || !strings.Contains(agentCommands, "rmdir /opt/kc") {
		t.Fatal("agent cleanup does not remove package manifest data and its empty parent")
	}
	serverCommands := strings.Join(serverCleanupCommands("/var/lib/kc-etcd"), "\n")
	if !strings.Contains(serverCommands, "rm -rf /etc/kubeclipper-server") {
		t.Fatal("server cleanup does not remove package Registry credential directory")
	}
}
