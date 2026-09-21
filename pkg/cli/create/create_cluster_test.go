package create

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
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
