/*
 *
 *  * Copyright 2026 KubeClipper Authors.
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
)

func TestExtensionInstallStepsWithContextPreserveResolvedTransport(t *testing.T) {
	stepper := &Extension{
		Offline: true,
		Version: extensionVersion,
	}
	plan := &deliveryapis.ResolvedArtifactPlan{Components: []deliveryapis.ResolvedComponent{
		{
			Kind:    k8sExtension,
			Name:    k8sExtension,
			Version: extensionVersion,
			Transport: deliveryapis.TransportRef{
				Type:   deliveryapis.TransportOCI,
				Ref:    "registry.local:5000/kubeclipper/packages/k8s-extension/k8s-extension:v1",
				Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			},
			Contents: []deliveryapis.ArtifactContent{
				{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"},
				{Name: deliveryapis.ContentImages, File: "images.tar.gz"},
			},
		},
	}}
	ctx := component.WithResolvedArtifactPlan(context.Background(), plan)

	steps, err := stepper.InstallStepsWithContext(ctx, []v1.StepNode{{ID: "master-1"}})
	if err != nil {
		t.Fatalf("InstallStepsWithContext() error: %+v", err)
	}
	if len(steps) != 1 || len(steps[0].Commands) != 1 {
		t.Fatalf("steps = %+v", steps)
	}
	var decoded Extension
	if err = json.Unmarshal(steps[0].Commands[0].CustomCommand, &decoded); err != nil {
		t.Fatalf("unmarshal extension command: %+v", err)
	}
	if decoded.Transport.Ref != "registry.local:5000/kubeclipper/packages/k8s-extension/k8s-extension:v1" {
		t.Fatalf("transport = %+v", decoded.Transport)
	}
	if len(decoded.Contents) != 2 {
		t.Fatalf("contents = %+v", decoded.Contents)
	}
}

func TestExtensionInstallStepsWithContextSkipsWithoutResolvedTransport(t *testing.T) {
	stepper := &Extension{
		Offline: true,
		Version: extensionVersion,
	}
	steps, err := stepper.InstallStepsWithContext(context.Background(), []v1.StepNode{{ID: "master-1"}})
	if err != nil {
		t.Fatalf("InstallStepsWithContext() error: %+v", err)
	}
	if len(steps) != 0 {
		t.Fatalf("steps = %+v, want no legacy extension step without resolved transport", steps)
	}
}

func TestExtensionInstallRequiresResolvedTransport(t *testing.T) {
	stepper := &Extension{
		Offline: true,
		Version: extensionVersion,
		CriType: v1.CRIDocker,
	}
	if _, err := stepper.Install(context.Background(), component.Options{DryRun: true}); err == nil {
		t.Fatalf("Install() expected missing resolved transport error")
	}
}

func TestExtensionInstallRequiresResolvedArtifactSource(t *testing.T) {
	stepper := &Extension{
		Offline: true,
		Version: extensionVersion,
		CriType: v1.CRIDocker,
	}
	if _, err := stepper.Install(context.Background(), component.Options{DryRun: true}); err == nil {
		t.Fatalf("Install() expected resolved artifact error")
	}
}
