/*
 *
 *  * Copyright 2024 KubeClipper Authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package k8s

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
)

func TestPackageInstallStepsPreserveResolvedTransport(t *testing.T) {
	stepper := &Package{
		Offline: true,
		Version: "v1.36.0",
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    "registry.local:5000/kubeclipper/packages/k8s/k8s:v1.36.0",
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
		Contents: []deliveryapis.ArtifactContent{{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"}},
	}
	steps, err := stepper.InstallSteps([]v1.StepNode{{ID: "master-1"}})
	if err != nil {
		t.Fatalf("InstallSteps() error: %+v", err)
	}
	if len(steps) != 1 || len(steps[0].Commands) != 1 {
		t.Fatalf("steps = %+v", steps)
	}
	var decoded Package
	if err = json.Unmarshal(steps[0].Commands[0].CustomCommand, &decoded); err != nil {
		t.Fatalf("unmarshal package command: %+v", err)
	}
	if decoded.Transport.Ref != stepper.Transport.Ref {
		t.Fatalf("transport = %+v", decoded.Transport)
	}
	if len(decoded.Contents) != 1 || decoded.Contents[0].Name != deliveryapis.ContentConfigs {
		t.Fatalf("contents = %+v", decoded.Contents)
	}
}

func TestPackageInstallRequiresResolvedTransport(t *testing.T) {
	stepper := &Package{
		Offline: true,
		Version: "v1.36.0",
		CriType: v1.CRIDocker,
	}
	if _, err := stepper.Install(context.Background(), component.Options{DryRun: true}); err == nil {
		t.Fatalf("Install() expected missing resolved transport error")
	}
}

func TestPackageInstallRequiresResolvedArtifactSource(t *testing.T) {
	stepper := &Package{
		Offline: true,
		Version: "v1.36.0",
		CriType: v1.CRIDocker,
	}
	if _, err := stepper.Install(context.Background(), component.Options{DryRun: true}); err == nil {
		t.Fatalf("Install() expected resolved artifact error")
	}
}

func TestUpgradeInitStepsPreserveResolvedTransport(t *testing.T) {
	upgrade := &Upgrade{
		Kubeadm: &KubeadmConfig{
			ContainerRuntime:  v1.CRIDocker,
			KubernetesVersion: "v1.34.0",
		},
		Offline: true,
		Version: "v1.36.0",
	}
	extra := component.ExtraMetadata{
		Offline: true,
		Masters: component.NodeList{{ID: "master-1"}},
	}
	plan := &deliveryapis.ResolvedArtifactPlan{Components: []deliveryapis.ResolvedComponent{
		{
			Kind:    K8s,
			Name:    K8s,
			Version: "v1.36.0",
			Transport: deliveryapis.TransportRef{
				Type:   deliveryapis.TransportOCI,
				Ref:    "registry.local:5000/kubeclipper/packages/k8s/k8s:v1.36.0",
				Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			},
			Contents: []deliveryapis.ArtifactContent{
				{Name: deliveryapis.ContentConfigs, File: downloader.ConfigFilename},
			},
		},
	}}
	ctx := component.WithResolvedArtifactPlan(component.WithExtraMetadata(context.Background(), extra), plan)
	if err := upgrade.InitSteps(ctx); err != nil {
		t.Fatalf("InitSteps() error: %+v", err)
	}
	steps := upgrade.GetInstallSteps()
	if len(steps) == 0 || len(steps[0].Commands) == 0 {
		t.Fatalf("steps = %+v", steps)
	}
	var decoded UpgradePackage
	if err := json.Unmarshal(steps[0].Commands[0].CustomCommand, &decoded); err != nil {
		t.Fatalf("unmarshal upgrade package command: %+v", err)
	}
	if decoded.Transport.Ref != "registry.local:5000/kubeclipper/packages/k8s/k8s:v1.36.0" {
		t.Fatalf("transport = %+v", decoded.Transport)
	}
	if len(decoded.Contents) != 1 || decoded.Contents[0].Name != deliveryapis.ContentConfigs {
		t.Fatalf("contents = %+v", decoded.Contents)
	}
}

func TestUpgradePackageInstallRequiresResolvedTransport(t *testing.T) {
	stepper := &UpgradePackage{
		Package: &Package{
			Offline: true,
			Version: "v1.36.0",
			CriType: v1.CRIDocker,
		},
		DownloadImage: true,
	}
	if _, err := stepper.Install(context.Background(), component.Options{DryRun: true}); err == nil {
		t.Fatalf("Install() expected missing resolved transport error")
	}
}
