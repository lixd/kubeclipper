package main

import (
	"strings"
	"testing"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

func supportedManifest() buildManifest {
	var manifest buildManifest
	manifest.Bootstrap.KubeClipperVersion = "v2.0.0"
	manifest.Bootstrap.ConsoleVersion = "v1.6.0"
	manifest.Bootstrap.RegistryVersion = "3.1.1"
	manifest.Bootstrap.EtcdVersion = "3.5.21"
	manifest.Resources.K8s.Versions = []string{"v1.36.1", "v1.35.0", "v1.34.2"}
	manifest.Resources.K8sExtension.Versions = []string{"v1"}
	manifest.Resources.CRI.Containerd.Versions = []string{"2.2.4", "1.7.29"}
	manifest.Resources.CNI.Calico.Versions = []string{"v3.31.5", "v3.29.6"}
	manifest.Resources.KCRuntime.Versions = []string{"v2.0.0"}
	manifest.Resources.RuntimeImageSets = map[string]struct {
		Versions []string `json:"versions"`
	}{
		"nfs":     {Versions: []string{"v4.0.2", "v4.1.0"}},
		"metallb": {Versions: []string{"v0.13.7"}},
	}
	return manifest
}

func TestBuildPublishMatrixIncludesEveryReleaseComponent(t *testing.T) {
	manifest := supportedManifest()
	matrix := buildPublishMatrix(&manifest)
	if len(matrix) != 16 {
		t.Fatalf("buildPublishMatrix() produced %d entries, want 16: %+v", len(matrix), matrix)
	}
	wanted := map[publishMatrixEntry]bool{
		{Component: "bootstrap-kubeclipper", Version: "v2.0.0"}: false,
		{Component: "resource-kc-runtime", Version: "v2.0.0"}:   false,
		{Component: "resource-nfs", Version: "v4.1.0"}:          false,
	}
	for _, entry := range matrix {
		if _, ok := wanted[entry]; ok {
			wanted[entry] = true
		}
	}
	for entry, found := range wanted {
		if !found {
			t.Errorf("publish matrix missing %+v", entry)
		}
	}
}

func TestVerifyPolicyCoverageAcceptsReleaseManifest(t *testing.T) {
	if err := verifyPolicyCoverage(supportedManifest(), deliveryapis.DefaultSupportPolicy()); err != nil {
		t.Fatalf("verifyPolicyCoverage() error: %v", err)
	}
}

func TestVerifyPolicyCoverageRejectsUnreferencedArtifact(t *testing.T) {
	manifest := supportedManifest()
	manifest.Resources.CNI.Calico.Versions = append(manifest.Resources.CNI.Calico.Versions, "v9.9.9")
	err := verifyPolicyCoverage(manifest, deliveryapis.DefaultSupportPolicy())
	if err == nil || !strings.Contains(err.Error(), "v9.9.9 is not referenced") {
		t.Fatalf("verifyPolicyCoverage() error = %v", err)
	}
}

func TestVerifyPolicyCoverageRejectsMissingPolicyDefault(t *testing.T) {
	manifest := supportedManifest()
	manifest.Resources.CRI.Containerd.Versions = []string{"2.2.4"}
	err := verifyPolicyCoverage(manifest, deliveryapis.DefaultSupportPolicy())
	if err == nil || !strings.Contains(err.Error(), "1.7.29 is not advertised") {
		t.Fatalf("verifyPolicyCoverage() error = %v", err)
	}
}
