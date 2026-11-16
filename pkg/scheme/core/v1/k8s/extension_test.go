/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package k8s

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestExtensionInstallStepsPropagateFailures(t *testing.T) {
	resolved := deliveryapis.ResolvedComponent{
		Kind: k8sExtension, Name: k8sExtension, Version: extensionVersion, Arch: "amd64",
		Transport: deliveryapis.TransportRef{Type: deliveryapis.TransportOCI, Ref: "registry.example/package-k8s-extension:v1"},
		Contents:  []deliveryapis.ArtifactContent{{Name: deliveryapis.ContentConfigs, File: "configs.tar.gz"}},
	}
	ctx := component.WithResolvedArtifactPlan(context.Background(), &deliveryapis.ResolvedArtifactPlan{
		Components: []deliveryapis.ResolvedComponent{resolved},
	})
	nodes := []v1.StepNode{{ID: "master"}, {ID: "worker"}}
	stepper := (&Extension{}).InitStepper(&v1.Cluster{})
	steps, err := stepper.InstallStepsWithContext(ctx, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 {
		t.Fatalf("install steps = %d, want 1", len(steps))
	}
	step := steps[0]
	if step.ErrIgnore {
		t.Fatal("extension installation failures must fail the operation after retries are exhausted")
	}
	if step.RetryTimes != 1 || step.Action != v1.ActionInstall || !reflect.DeepEqual(step.Nodes, nodes) {
		t.Fatalf("unexpected extension install step: %#v", step)
	}
	if len(step.Commands) != 1 || step.Commands[0].Type != v1.CommandCustom {
		t.Fatalf("unexpected extension commands: %#v", step.Commands)
	}
	var payload Extension
	if err := json.Unmarshal(step.Commands[0].CustomCommand, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Arch != resolved.Arch || payload.Transport != resolved.Transport || !reflect.DeepEqual(payload.Contents, resolved.Contents) {
		t.Fatalf("extension retry payload lost the resolved artifact: %#v", payload)
	}
}
