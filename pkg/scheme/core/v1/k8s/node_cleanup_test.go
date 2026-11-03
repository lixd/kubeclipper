package k8s

import (
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"reflect"
	"testing"
)

func TestNodeRemovalCleansOnlyLeavingNodeCNIState(t *testing.T) {
	listener := availableMasterListener(t)
	defer func() { _ = (*listener).Close() }()
	for _, role := range []string{NodeRoleMaster, NodeRoleWorker} {
		t.Run(role, func(t *testing.T) {
			metadata := masterStepFixtureMetadata()
			cluster := masterStepFixtureCluster()
			leaving := v1.StepNode{ID: "joining", IPv4: "127.0.0.1", NodeIPv4: "127.0.0.1", Hostname: "joining"}
			if role == NodeRoleWorker {
				leaving = v1.StepNode{ID: "worker-1", IPv4: "192.0.2.10", NodeIPv4: "192.0.2.10", Hostname: "worker-1"}
			}
			generator := (&GenNode{}).InitStepper(metadata, cluster, role)
			if err := generator.MakeUninstallSteps(metadata, []v1.StepNode{leaving}); err != nil {
				t.Fatal(err)
			}
			steps := generator.GetSteps(v1.ActionUninstall)
			for name, paths := range map[string][]string{"cleanCNIConfig": {CniDefaultConfigDir}, "removeCNIData": {"/var/lib/cni", "/var/lib/calico"}} {
				step := findStep(t, steps, name)
				if len(step.Nodes) != 1 || step.Nodes[0].ID != leaving.ID {
					t.Fatalf("%s targets surviving nodes: %v", name, step.Nodes)
				}
				if step.ErrIgnore {
					t.Fatalf("%s must expose a cleanup failure before node reuse", name)
				}
				expected := append([]string{"rm", "-rf"}, paths...)
				if len(step.Commands) != 1 || !reflect.DeepEqual(step.Commands[0].ShellCommand, expected) {
					t.Fatalf("%s must remove stale CNI configuration and identity state", name)
				}
			}
		})
	}
}

func TestClusterAndNodeCleanupNeverRecursivelyRemoveMountedCalicoRuntime(t *testing.T) {
	listener := availableMasterListener(t)
	defer func() { _ = (*listener).Close() }()
	metadata := masterStepFixtureMetadata()
	cluster := masterStepFixtureCluster()
	node := (&GenNode{}).InitStepper(metadata, cluster, NodeRoleWorker)
	if err := node.MakeUninstallSteps(metadata, []v1.StepNode{{ID: "worker-1", IPv4: "192.0.2.10", Hostname: "worker-1"}}); err != nil {
		t.Fatal(err)
	}
	full, err := Clear(cluster, metadata)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range append(node.GetSteps(v1.ActionUninstall), full...) {
		for _, cmd := range step.Commands {
			for _, arg := range cmd.ShellCommand {
				if arg == "/var/run/calico" {
					t.Fatalf("%s recursively removes a runtime directory containing host cgroup mounts", step.Name)
				}
			}
		}
	}
}
