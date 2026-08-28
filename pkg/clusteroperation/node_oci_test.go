package clusteroperation

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestGetPackageStepsUsesResolvedArtifact(t *testing.T) {
	plan := &deliveryapis.ResolvedArtifactPlan{Components: []deliveryapis.ResolvedComponent{{
		Kind: "k8s", Name: "k8s", Version: "v1.36.1",
		Transport: deliveryapis.TransportRef{Type: "oci", Ref: "registry/pkg", Digest: "sha256:test"},
	}}}
	cluster := &corev1.Cluster{KubernetesVersion: "v1.36.1"}
	ctx := component.WithResolvedArtifactPlan(context.Background(), plan)
	steps, err := getPackageSteps(ctx, cluster, corev1.ActionInstall, []corev1.StepNode{{ID: "node"}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(steps[0].Commands[0].CustomCommand, &decoded); err != nil {
		t.Fatal(err)
	}
	transport, ok := decoded["transport"].(map[string]any)
	if !ok {
		t.Fatalf("runtime payload has no transport: %#v", decoded)
	}
	if transport["type"] != "oci" {
		t.Fatalf("resolved transport type = %v, want oci", transport["type"])
	}
}

func TestPatchNodesPreservesResolvedArtifactPlan(t *testing.T) {
	plan := &deliveryapis.ResolvedArtifactPlan{
		KubernetesVersion: "v1.36.1",
		Arch:              "amd64",
		Components: []deliveryapis.ResolvedComponent{{
			Kind: "k8s", Name: "k8s", Version: "v1.36.1",
			Transport: deliveryapis.TransportRef{Type: deliveryapis.TransportOCI, Ref: "registry/pkg", Digest: "sha256:test"},
		}},
	}
	payload, err := json.Marshal(&PatchNodes{Operation: NodesOperationAdd, ResolvedArtifactPlan: plan})
	if err != nil {
		t.Fatal(err)
	}
	var decoded PatchNodes
	if err = json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.ResolvedArtifactPlan == nil || decoded.ResolvedArtifactPlan.Components[0].Transport.Digest != "sha256:test" {
		t.Fatalf("decoded package plan = %#v", decoded.ResolvedArtifactPlan)
	}
}
