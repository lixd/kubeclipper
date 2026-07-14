package main

import (
	"strings"
	"testing"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

func supportedManifest() buildManifest {
	var manifest buildManifest
	manifest.Bootstrap.KubeClipperVersion = "v1.8.0"
	manifest.Bootstrap.EtcdVersion = "3.5.21"
	manifest.Resources.K8s.Versions = []string{"v1.36.1", "v1.35.0", "v1.34.2"}
	manifest.Resources.K8sExtension.Versions = []string{"v1"}
	manifest.Resources.CRI.Containerd.Versions = []string{"2.2.4", "1.7.29"}
	manifest.Resources.CNI.Calico.Versions = []string{"v3.31.5", "v3.29.6"}
	return manifest
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
