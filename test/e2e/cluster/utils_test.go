/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package cluster

import (
	"encoding/json"
	"errors"
	"path"
	"strings"
	"testing"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/delivery/releasemanifest"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	corevalidation "github.com/kubeclipper/kubeclipper/pkg/scheme/core/validation"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

func TestClusterFixtureUsesCurrentDeliveryPolicy(t *testing.T) {
	manifest, err := releasemanifest.Load("")
	if err != nil {
		t.Fatal(err)
	}
	inventory := deliveryapis.NewPackageInventory("unit-test")
	for _, artifact := range manifest.Artifacts {
		if artifact.Type != releasemanifest.ArtifactTypePackage {
			continue
		}
		inventory.Spec.Packages = append(inventory.Spec.Packages, deliveryapis.PackageEntry{
			Kind: artifact.Component.Kind, Name: artifact.Component.Name, Version: artifact.Component.Version,
			OS: "linux", Arch: "amd64",
			Transport: deliveryapis.TransportRef{
				Type: deliveryapis.TransportOCI, Ref: artifact.Source, Digest: "sha256:" + strings.Repeat("0", 64),
			},
		})
	}
	policy := deliveryapis.DefaultSupportPolicy()
	projection, err := deliveryapis.ProjectComponentMeta(inventory, policy, deliveryapis.ProjectOptions{Archs: []string{"amd64"}})
	if err != nil {
		t.Fatal(err)
	}
	meta := &kc.ComponentMeta{Rules: projection.Rules}
	for _, rule := range projection.Rules {
		fixture := initCluster()
		fixture.KubernetesVersion = rule["version"].(string)
		if err := setSupportedClusterVersion(fixture, meta); err != nil {
			t.Fatal(err)
		}
		for _, support := range policy.Spec.Policies {
			matches, err := path.Match(support.Match.KubernetesVersion, fixture.KubernetesVersion)
			if err != nil {
				t.Fatal(err)
			}
			if !matches {
				continue
			}
			for _, slot := range support.ComponentSlots {
				switch slot.Slot {
				case "cri":
					if fixture.ContainerRuntime.Version != slot.Default.Version {
						t.Fatalf("%s containerd = %s, policy default = %s", fixture.KubernetesVersion, fixture.ContainerRuntime.Version, slot.Default.Version)
					}
				case "cni":
					if fixture.CNI.Version != slot.Default.Version {
						t.Fatalf("%s Calico = %s, policy default = %s", fixture.KubernetesVersion, fixture.CNI.Version, slot.Default.Version)
					}
				}
			}
		}
	}
	current, target, err := supportedUpgradeVersions(meta)
	if err != nil {
		t.Fatal(err)
	}
	if err := corevalidation.ValidateKubernetesUpgradeVersion("unit-test", current, target); err != nil {
		t.Fatal(err)
	}
}

func TestClusterFixtureSelectsVersionsAndDefaults(t *testing.T) {
	meta := clusterVersionMetadata(t, []clusterVersion{
		fixtureVersionRule("v1.9.5", "1.7.0", "v3.28.0"),
		fixtureVersionRule("v1.10.2", "2.0.0", "v3.29.0"),
	})
	fixture := initCluster()
	if err := setSupportedClusterVersion(fixture, meta); err != nil {
		t.Fatal(err)
	}
	if fixture.KubernetesVersion != "v1.10.2" || fixture.ContainerRuntime.Version != "2.0.0" || fixture.CNI.Version != "v3.29.0" {
		t.Fatalf("selected unexpected version combination: %+v", fixture)
	}
	fixture.KubernetesVersion = "v1.9.5"
	if err := setSupportedClusterVersion(fixture, meta); err != nil {
		t.Fatal(err)
	}
	if fixture.ContainerRuntime.Version != "1.7.0" || fixture.CNI.Version != "v3.28.0" {
		t.Fatal("components did not follow the selected Kubernetes rule")
	}
	fixture.KubernetesVersion = "v1.8.0"
	if err := setSupportedClusterVersion(fixture, meta); err == nil {
		t.Fatal("unsupported Kubernetes version was accepted")
	}
	choices := []componentVersion{
		{Name: "containerd", Version: "1.7.0"},
		{Name: "containerd", Version: "2.0.0", Default: true},
	}
	if got := defaultComponentVersion(choices, "containerd"); got != "2.0.0" {
		t.Fatalf("selected %q, want the policy default", got)
	}
}

func TestSupportedUpgradeVersionsRejectsUnavailablePairs(t *testing.T) {
	for _, test := range []struct {
		name            string
		rules           []clusterVersion
		pairUnavailable bool
	}{
		{name: "empty inventory"},
		{name: "invalid version", rules: []clusterVersion{fixtureVersionRule("invalid", "2.0.0", "v3.29.0")}},
		{name: "single version", rules: []clusterVersion{fixtureVersionRule("v1.10.0", "2.0.0", "v3.29.0")}, pairUnavailable: true},
		{name: "minor version gap", rules: []clusterVersion{fixtureVersionRule("v1.10.0", "2.0.0", "v3.29.0"), fixtureVersionRule("v1.12.0", "2.0.0", "v3.29.0")}, pairUnavailable: true},
		{name: "component version change", rules: []clusterVersion{fixtureVersionRule("v1.10.0", "1.7.0", "v3.29.0"), fixtureVersionRule("v1.11.0", "2.0.0", "v3.29.0")}, pairUnavailable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := supportedUpgradeVersions(clusterVersionMetadata(t, test.rules))
			if err == nil {
				t.Fatal("expected an explicit unavailable-upgrade error")
			}
			if errors.Is(err, errNoSupportedUpgradePair) != test.pairUnavailable {
				t.Fatalf("error = %v, want pair-unavailable classification = %t", err, test.pairUnavailable)
			}
		})
	}
}

func TestSupportedUpgradeVersionsRejectsMalformedMetadata(t *testing.T) {
	meta := &kc.ComponentMeta{Rules: []map[string]interface{}{
		{"version": "v1.10.0", "version_control": "invalid"},
	}}
	_, _, err := supportedUpgradeVersions(meta)
	if err == nil || errors.Is(err, errNoSupportedUpgradePair) {
		t.Fatalf("malformed metadata must fail without being classified as an unavailable upgrade pair: %v", err)
	}
}

func TestClusterFixturesAreIndependent(t *testing.T) {
	first := initCluster()
	SetClusterBackupPoint("unit-backup-point")(first)
	SetClusterNodes([]string{"master"}, []string{"worker"})(first)
	first.CNI.Calico.Mode = "BGP"
	first.Networking.Pods.CIDRBlocks[0] = "10.1.0.0/16"
	second := initCluster()
	if second.Labels[common.LabelBackupPoint] != "" || len(second.Masters) != 0 || len(second.Workers) != 0 ||
		second.CNI.Calico.Mode == "BGP" || second.Networking.Pods.CIDRBlocks[0] == "10.1.0.0/16" {
		t.Fatal("one test's cluster changes leaked into the next fixture")
	}
}

func fixtureVersionRule(version, criVersion, cniVersion string) clusterVersion {
	return clusterVersion{Version: version, VersionControl: map[string][]componentVersion{
		"cri": {{Name: corev1.CRIContainerd, Version: criVersion, Default: true}},
		"cni": {{Name: "calico", Version: cniVersion, Default: true}},
	}}
}

func clusterVersionMetadata(t *testing.T, rules []clusterVersion) *kc.ComponentMeta {
	t.Helper()
	data, err := json.Marshal(rules)
	if err != nil {
		t.Fatal(err)
	}
	meta := &kc.ComponentMeta{}
	if err := json.Unmarshal(data, &meta.Rules); err != nil {
		t.Fatal(err)
	}
	return meta
}
