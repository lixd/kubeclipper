package k8s

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestKubeadmUpgradeConfigurationRenderedWhenRequested(t *testing.T) {
	config := &KubeadmConfig{ClusterConfigAPIVersion: "v1beta4"}
	output := &bytes.Buffer{}
	if err := config.renderTo(output); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "kind: UpgradeConfiguration") {
		t.Fatal("non-upgrade kubeadm config unexpectedly includes UpgradeConfiguration")
	}

	config.IncludeUpgradeConfiguration = true
	output.Reset()
	if err := config.renderTo(output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "apiVersion: kubeadm.k8s.io/v1beta4\nkind: UpgradeConfiguration\napply: {}") {
		t.Fatalf("upgrade config lacks a valid UpgradeConfiguration document:\n%s", output.String())
	}
}

func TestUpgradeStepsPropagateCommandFailures(t *testing.T) {
	metadata := component.ExtraMetadata{
		Masters:       component.NodeList{{ID: "master-id", Hostname: "master"}},
		Workers:       component.NodeList{{ID: "worker-id", Hostname: "worker"}},
		Offline:       true,
		ImageRegistry: "registry.example:5000",
		ClusterName:   "test-cluster",
		KubeVersion:   "v1.37.0",
	}
	cluster := &v1.Cluster{}
	cluster.ContainerRuntime.Type = "containerd"
	stepper := &Upgrade{Version: "v1.37.0", ImageRegistry: metadata.ImageRegistry}
	stepper.InitStepper(&metadata, cluster)
	if err := stepper.InitSteps(component.WithExtraMetadata(context.Background(), metadata)); err != nil {
		t.Fatal(err)
	}
	if !stepper.Kubeadm.IncludeUpgradeConfiguration {
		t.Fatal("upgrade plan does not request the kubeadm UpgradeConfiguration document")
	}
	kubeadmSteps, err := stepper.Kubeadm.InstallSteps([]v1.StepNode{{ID: "master-id"}})
	if err != nil {
		t.Fatal(err)
	}
	var agentConfig KubeadmConfig
	if err := json.Unmarshal(kubeadmSteps[0].Commands[0].Template.Data, &agentConfig); err != nil {
		t.Fatal(err)
	}
	if !agentConfig.IncludeUpgradeConfiguration {
		t.Fatal("serialized Agent template command lost the UpgradeConfiguration flag")
	}

	var controlPlane, drain, workerUpgrade, uncordon *v1.Step
	steps := stepper.GetInstallSteps()
	for index := range steps {
		step := &steps[index]
		switch step.Name {
		case "UpgradeControlPlane-master":
			controlPlane = step
		case "DrainNode-worker":
			drain = step
		case "UpgradeWorker-worker":
			workerUpgrade = step
		case "UncordonNode-worker":
			uncordon = step
		}
	}
	if controlPlane == nil || drain == nil || workerUpgrade == nil || uncordon == nil {
		t.Fatalf("upgrade plan is missing an expected step: %#v", steps)
	}

	controlCommand := strings.Join(controlPlane.Commands[0].ShellCommand, " ")
	for _, expected := range []string{
		"set -euo pipefail",
		"kubeadm upgrade apply v1.37.0 -y --ignore-preflight-errors all --config /tmp/.k8s/kubeadm.yaml",
		"trap",
		"kubectl get --raw=/readyz --request-timeout=3s",
		"kubectl wait --for=condition=Ready \"node/$node\" --timeout=3s",
		"kubectl wait --for=condition=Ready \"pod/kube-apiserver-$node\" -n kube-system --timeout=3s",
		"ready_deadline=$((SECONDS + 240))",
		"ready_samples < 7",
		"ready_samples=0",
		"control plane API did not remain ready for 30 seconds",
	} {
		if !strings.Contains(controlCommand, expected) {
			t.Errorf("control-plane command %q does not contain %q", controlCommand, expected)
		}
	}
	if strings.Contains(controlCommand, "upgrade apply v1.37.0 -f") {
		t.Errorf("control-plane command still combines --force with --config: %q", controlCommand)
	}
	if controlPlane.Timeout.Duration < 15*time.Minute {
		t.Errorf("control-plane step timeout = %s, want at least 15m for the stable readiness gate", controlPlane.Timeout.Duration)
	}
	if drain.ErrIgnore {
		t.Error("worker drain failure must fail the upgrade operation")
	}
	if drain.Timeout.Duration < 5*time.Minute {
		t.Errorf("worker drain timeout = %s, want at least 5m to honor pod disruption budgets", drain.Timeout.Duration)
	}
	if !strings.Contains(strings.Join(drain.Commands[0].ShellCommand, " "), "--timeout=5m") {
		t.Error("worker drain command lacks an explicit PDB wait timeout")
	}
	if uncordon.ErrIgnore {
		t.Error("worker uncordon failure must fail the upgrade operation")
	}
	if uncordon.Timeout.Duration < 5*time.Minute {
		t.Errorf("worker readiness timeout = %s, want at least 5m", uncordon.Timeout.Duration)
	}
	uncordonCommand := strings.Join(uncordon.Commands[0].ShellCommand, " ")
	if !strings.Contains(uncordonCommand, "kubectl wait --for=condition=Ready") || !strings.Contains(uncordonCommand, "kubectl uncordon") {
		t.Errorf("worker uncordon step must wait for the node to become Ready: %q", uncordonCommand)
	}
	workerCommand := strings.Join(workerUpgrade.Commands[0].ShellCommand, " ")
	if !strings.Contains(workerCommand, "-euo pipefail") {
		t.Errorf("worker upgrade command does not fail fast: %q", workerCommand)
	}
}
