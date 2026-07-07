package deliverypolicy

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

type fakePolicyClient struct {
	policy        *deliveryapis.SupportPolicy
	updatedPolicy *deliveryapis.SupportPolicy
	err           error
}

func (f *fakePolicyClient) GetDeliveryPolicy(ctx context.Context) (*deliveryapis.SupportPolicy, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.policy, nil
}

func (f *fakePolicyClient) UpdateDeliveryPolicy(ctx context.Context, policy *deliveryapis.SupportPolicy) (*deliveryapis.SupportPolicy, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.updatedPolicy = policy
	return policy, nil
}

func TestRunGet(t *testing.T) {
	var out bytes.Buffer
	o := NewDeliveryPolicyOptions(options.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}})
	o.client = &fakePolicyClient{policy: samplePolicy()}
	if err := o.RunGet(); err != nil {
		t.Fatalf("RunGet() error: %v", err)
	}
	if !strings.Contains(out.String(), "default") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestRunApply(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	data := []byte(`
apiVersion: delivery.kubeclipper.io/v1alpha1
kind: SupportPolicy
metadata:
  name: custom
spec:
  policies:
  - name: k8s-v1.36
    match:
      kubernetesVersion: v1.36.*
    componentSlots:
    - slot: cri
      selection: oneOf
      required: true
      default:
        name: containerd
        version: 2.1.0
      options:
      - kind: cri
        name: containerd
        allowedVersions:
        - 2.1.0
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}
	client := &fakePolicyClient{}
	o := NewDeliveryPolicyOptions(options.IOStreams{Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}})
	o.client = client
	o.File = path
	if err := o.RunApply(); err != nil {
		t.Fatalf("RunApply() error: %v", err)
	}
	if client.updatedPolicy == nil {
		t.Fatalf("updatedPolicy is nil")
	}
	if client.updatedPolicy.Metadata.Name != "custom" {
		t.Fatalf("updatedPolicy = %+v", client.updatedPolicy)
	}
}

func TestRunValidate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	data := []byte(`
apiVersion: delivery.kubeclipper.io/v1alpha1
kind: SupportPolicy
metadata:
  name: custom
spec:
  policies:
  - name: k8s-v1.36
    match:
      kubernetesVersion: v1.36.*
    componentSlots:
    - slot: cri
      selection: oneOf
      required: true
      default:
        name: containerd
        version: 2.1.0
      options:
      - kind: cri
        name: containerd
        allowedVersions:
        - 2.1.0
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}
	var out bytes.Buffer
	o := NewDeliveryPolicyOptions(options.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}})
	o.File = path
	if err := o.RunValidate(); err != nil {
		t.Fatalf("RunValidate() error: %v", err)
	}
	if !strings.Contains(out.String(), "valid") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestRunValidateRejectsResourceFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	data := []byte(`
apiVersion: delivery.kubeclipper.io/v1alpha1
kind: SupportPolicy
metadata:
  name: custom
spec:
  policies:
  - name: k8s-v1.36
    match:
      kubernetesVersion: v1.36.*
    componentSlots:
    - slot: cri
      selection: oneOf
      required: true
      default:
        name: containerd
        version: 2.1.0
        digest: sha256:1111111111111111111111111111111111111111111111111111111111111111
      options:
      - kind: cri
        name: containerd
        allowedVersions:
        - 2.1.0
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}
	o := NewDeliveryPolicyOptions(options.IOStreams{Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}})
	o.File = path
	err := o.RunValidate()
	if err == nil {
		t.Fatalf("RunValidate() error = nil, want unknown field error")
	}
	if !strings.Contains(err.Error(), `unknown field "digest"`) {
		t.Fatalf("RunValidate() error = %v", err)
	}
}

func TestRunTemplate(t *testing.T) {
	var out bytes.Buffer
	o := NewDeliveryPolicyOptions(options.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}})
	if err := o.RunTemplate(); err != nil {
		t.Fatalf("RunTemplate() error: %v", err)
	}
	if !strings.Contains(out.String(), "default") {
		t.Fatalf("output = %q", out.String())
	}
	policy := defaultPolicyTemplate()
	if err := policy.Validate(); err != nil {
		t.Fatalf("defaultPolicyTemplate() invalid: %v", err)
	}
	data, err := policyPrinter{policy: policy}.YAMLPrint()
	if err != nil {
		t.Fatalf("YAMLPrint() error: %v", err)
	}
	if !strings.Contains(string(data), "containerd") || !strings.Contains(string(data), "calico") {
		t.Fatalf("template yaml = %q", string(data))
	}
	assertTemplateSlot(t, policy, "cri", "cri", "containerd", "2.2.4")
	assertTemplateSlot(t, policy, "cni", "cni", "calico", "v3.31.5")
	assertTemplateSlot(t, policy, "bootstrap-kubeclipper-agent", "binary", "kubeclipper-agent", "v1.8.0")
	assertTemplateSlot(t, policy, "bootstrap-etcdctl", "binary", "etcdctl", "v3.5.15")
	assertTemplateSlot(t, policy, "extension", "extension", "kubectl-terminal", "v1.0.0")
}

func TestRunDiff(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "policy.yaml")
	data := []byte(`
apiVersion: delivery.kubeclipper.io/v1alpha1
kind: SupportPolicy
metadata:
  name: custom
spec:
  policies:
  - name: k8s-v1.36
    match:
      kubernetesVersion: v1.36.*
    componentSlots:
    - slot: cri
      selection: oneOf
      required: true
      default:
        name: docker
        version: 24.0.0
      options:
      - kind: cri
        name: docker
        allowedVersions:
        - 24.0.0
`)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}
	var out bytes.Buffer
	o := NewDeliveryPolicyOptions(options.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}})
	o.client = &fakePolicyClient{policy: samplePolicy()}
	o.File = path
	if err := o.RunDiff(); err != nil {
		t.Fatalf("RunDiff() error: %v", err)
	}
	if !strings.Contains(out.String(), "--- current") || !strings.Contains(out.String(), "docker") {
		t.Fatalf("diff output = %q", out.String())
	}
}

func samplePolicy() *deliveryapis.SupportPolicy {
	policy := deliveryapis.NewSupportPolicy("default")
	policy.Spec.Policies = []deliveryapis.KubernetesSupportPolicy{{
		Name:  "k8s-v1.36",
		Match: deliveryapis.PolicyMatch{KubernetesVersion: "v1.36.*"},
		ComponentSlots: []deliveryapis.ComponentSlotRule{{
			Slot:      "cri",
			Selection: deliveryapis.SelectionOneOf,
			Required:  true,
			Default:   deliveryapis.ComponentChoice{Name: "containerd", Version: "2.1.0"},
			Options:   []deliveryapis.ComponentOption{{Kind: "cri", Name: "containerd", AllowedVersions: []string{"2.1.0"}}},
		}},
	}}
	return policy
}

func assertTemplateSlot(t *testing.T, policy *deliveryapis.SupportPolicy, slotName, kind, name, version string) {
	t.Helper()
	if policy == nil || len(policy.Spec.Policies) == 0 {
		t.Fatalf("policy has no rules")
	}
	for _, slot := range policy.Spec.Policies[0].ComponentSlots {
		if slot.Slot != slotName {
			continue
		}
		if slot.Default.Name != name || slot.Default.Version != version {
			t.Fatalf("slot %s default = %+v, want %s:%s", slotName, slot.Default, name, version)
		}
		for _, option := range slot.Options {
			if option.Kind == kind && option.Name == name {
				if len(option.AllowedVersions) != 1 || option.AllowedVersions[0] != version {
					t.Fatalf("slot %s versions = %+v, want [%s]", slotName, option.AllowedVersions, version)
				}
				return
			}
		}
		t.Fatalf("slot %s options = %+v, want %s/%s", slotName, slot.Options, kind, name)
	}
	t.Fatalf("policy template missing slot %s", slotName)
}
