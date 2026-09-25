package create

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

func TestCreateClusterRegistryFlags(t *testing.T) {
	out := &bytes.Buffer{}
	streams := options.IOStreams{In: strings.NewReader(""), Out: out, ErrOut: out}
	cmd := NewCmdCreateCluster(streams)
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetArgs([]string{"--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	help := out.String()
	if !strings.Contains(help, "--image-registry string") {
		t.Fatalf("image registry flag missing from help:\n%s", help)
	}
	if strings.Contains(help, "--local-registry") || strings.Contains(help, "--insecure-registry") {
		t.Fatalf("removed registry flags remain in help:\n%s", help)
	}
}

func TestNewClusterLeavesOnlineImageRegistryEmpty(t *testing.T) {
	opts := NewCreateClusterOptions(options.IOStreams{})
	opts.Name = "demo"
	opts.Masters = []string{"node-1"}
	cluster := opts.newCluster()
	if cluster.ImageRegistry != "" {
		t.Fatalf("imageRegistry = %q, want empty", cluster.ImageRegistry)
	}
}

// The Docker CRI entry was removed; --cri docker must fail validation with a
// message that names the removal instead of a generic unsupported-cri error.
// ValidateArgs is called directly: the full Run path connects to the server in
// Complete before validation ever runs.
func TestCreateClusterRejectsDockerCRI(t *testing.T) {
	streams := options.IOStreams{In: strings.NewReader(""), Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}}
	cmd := NewCmdCreateCluster(streams)
	o := NewCreateClusterOptions(streams)

	o.CRI = "docker"
	err := o.ValidateArgs(cmd)
	if err == nil || !strings.Contains(err.Error(), "Docker CRI is not supported") {
		t.Fatalf("expected removal-specific error, got: %v", err)
	}

	o.CRI = "cri-o"
	err = o.ValidateArgs(cmd)
	if err == nil || !strings.Contains(err.Error(), "unsupported cri") {
		t.Fatalf("expected unknown CRI rejection, got: %v", err)
	}

	// the help output must no longer offer docker
	out := &bytes.Buffer{}
	cmd2 := NewCmdCreateCluster(streams)
	cmd2.SetOut(out)
	cmd2.SetArgs([]string{"--help"})
	if err := cmd2.Execute(); err != nil {
		t.Fatal(err)
	}
	// the --cri flag help must no longer offer docker (other "docker" mentions
	// in the help, like calico's docker0 bridge note, are unrelated)
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.Contains(line, "--cri string") {
			if strings.Contains(line, "docker") {
				t.Fatalf("--cri flag help still offers docker: %s", line)
			}
			if !strings.Contains(line, "containerd") {
				t.Fatalf("--cri flag help should name containerd: %s", line)
			}
			return
		}
	}
	t.Fatalf("--cri flag not found in help:\n%s", out.String())
}

func TestPolicySlotVersions(t *testing.T) {
	metas := &kc.ComponentMeta{Rules: []map[string]interface{}{
		{
			"name": "k8s", "version": "v1.35.8", "type": "k8s", "arch": "amd64",
			"version_control": map[string]interface{}{
				"cri": []interface{}{
					map[string]interface{}{"name": "containerd", "version": "1.7.29", "type": "cri", "default": true},
				},
				"cni": []interface{}{
					map[string]interface{}{"name": "calico", "version": "v3.29.6", "type": "cni", "default": true},
				},
			},
		},
		{
			"name": "k8s", "version": "v1.37.0", "type": "k8s", "arch": "amd64",
			"version_control": map[string]interface{}{
				"cri": []interface{}{
					map[string]interface{}{"name": "containerd", "version": "2.2.4", "type": "cri", "default": true},
					map[string]interface{}{"name": "containerd", "version": "1.7.29", "type": "cri"},
				},
				"cni": []interface{}{
					map[string]interface{}{"name": "calico", "version": "v3.31.5", "type": "cni", "default": true},
					map[string]interface{}{"name": "calico", "version": "v3.29.6", "type": "cni"},
				},
			},
		},
	}}

	// The default must win even when an older, non-default version sorts first.
	got := policySlotVersions(metas, "v1.37.0", "cri", "containerd")
	want := []string{"2.2.4", "1.7.29"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("policySlotVersions(v1.37.0, cri, containerd) = %v, want %v", got, want)
	}
	got = policySlotVersions(metas, "v1.35.8", "cni", "calico")
	if !reflect.DeepEqual(got, []string{"v3.29.6"}) {
		t.Fatalf("policySlotVersions(v1.35.8, cni, calico) = %v, want [v3.29.6]", got)
	}
	// A Kubernetes version without a matching rule offers no defaults, so the
	// caller falls back to the published-version list.
	if got := policySlotVersions(metas, "v1.99.0", "cri", "containerd"); got != nil {
		t.Fatalf("policySlotVersions(v1.99.0, ...) = %v, want nil", got)
	}
	if got := policySlotVersions(nil, "v1.37.0", "cri", "containerd"); got != nil {
		t.Fatalf("policySlotVersions(nil, ...) = %v, want nil", got)
	}
}

// The legacy node-role.kubernetes.io/master taint must not be applied: modern
// kubeadm-rendered addons (coredns) tolerate only the control-plane taint, so
// a single-master cluster with both taints deadlocked at the health check
// (R21). The default taint is the control-plane one kubeadm applies anyway.
func TestNewClusterDefaultTaintIsControlPlane(t *testing.T) {
	opts := NewCreateClusterOptions(options.IOStreams{})
	opts.Name = "demo"
	opts.Masters = []string{"node-1"}
	cluster := opts.newCluster()
	if len(cluster.Masters) != 1 || len(cluster.Masters[0].Taints) != 1 {
		t.Fatalf("default master taints = %+v, want exactly the control-plane taint", cluster.Masters)
	}
	taint := cluster.Masters[0].Taints[0]
	if taint.Key != "node-role.kubernetes.io/control-plane" || taint.Effect != v1.TaintEffectNoSchedule {
		t.Fatalf("default taint = %+v, want node-role.kubernetes.io/control-plane:NoSchedule", taint)
	}

	untainted := NewCreateClusterOptions(options.IOStreams{})
	untainted.Name = "demo"
	untainted.Masters = []string{"node-1"}
	untainted.UntaintMaster = true
	cluster = untainted.newCluster()
	if len(cluster.Masters[0].Taints) != 0 {
		t.Fatalf("--untaint-master taints = %+v, want none", cluster.Masters[0].Taints)
	}
}
