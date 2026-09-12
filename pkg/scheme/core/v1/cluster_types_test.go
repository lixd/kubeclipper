package v1

import (
	"bytes"
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestLegacyRegistryFieldsAreIgnored(t *testing.T) {
	var cluster Cluster
	legacy := `{"localRegistry":"legacy.example.com/k8s","containerRuntime":{"type":"containerd","insecureRegistry":["legacy.example.com"],"registries":[{"insecureRegistry":"other.example.com"}]}}`
	if err := json.Unmarshal([]byte(legacy), &cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.ImageRegistry != "" {
		t.Fatalf("legacy localRegistry unexpectedly populated imageRegistry: %q", cluster.ImageRegistry)
	}
	if len(cluster.ContainerRuntime.Registries) != 1 || cluster.ContainerRuntime.Registries[0].RegistryRef != nil {
		t.Fatalf("legacy insecure registry unexpectedly populated the new registry model: %#v", cluster.ContainerRuntime)
	}
}

func TestClusterStatusDeepCopyPackagePlan(t *testing.T) {
	cluster := &Cluster{Status: ClusterStatus{PackagePlan: &runtime.RawExtension{Raw: []byte(`{"arch":"amd64"}`)}}}
	clusterCopy := cluster.DeepCopy()
	if clusterCopy.Status.PackagePlan == cluster.Status.PackagePlan {
		t.Fatal("PackagePlan pointer was not copied")
	}
	clusterCopy.Status.PackagePlan.Raw[0] = '['
	if string(cluster.Status.PackagePlan.Raw) != `{"arch":"amd64"}` {
		t.Fatalf("mutating the copy changed the original package plan: %s", cluster.Status.PackagePlan.Raw)
	}
}

func TestClusterStatusPackagePlanJSONRoundTrip(t *testing.T) {
	original := &Cluster{Status: ClusterStatus{PackagePlan: &runtime.RawExtension{Raw: []byte(`{"kubernetesVersion":"v1.36.1","arch":"amd64"}`)}}}
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Cluster
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Status.PackagePlan == nil || !bytes.Equal(decoded.Status.PackagePlan.Raw, original.Status.PackagePlan.Raw) {
		t.Fatalf("package plan JSON round trip = %s, want %s", decoded.Status.PackagePlan.Raw, original.Status.PackagePlan.Raw)
	}
}
