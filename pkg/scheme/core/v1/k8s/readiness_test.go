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
	"github.com/kubeclipper/kubeclipper/pkg/agent/operationv2"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
	"io"
	"k8s.io/apimachinery/pkg/runtime"
	"os"
	"os/exec"
	"strings"
	"testing"

	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestNodeReadinessStep(t *testing.T) {
	c := &v1.Cluster{}
	c.Etcd.DataDir = "/var/tmp/r45/etcd custom"
	c.Kubelet.RootDir = "/var/tmp/r45/kubelet"
	c.ContainerRuntime.DataRootDir = "/var/tmp/r45/runtime"
	step := nodeReadinessStep([]v1.StepNode{{ID: "test-node"}}, c)
	if step.ErrIgnore || step.RetryTimes != 0 || len(step.Nodes) != 1 {
		t.Fatalf("preflight must stop install: %#v", step)
	}
	command := step.Commands[0].ShellCommand
	for _, text := range []string{"insufficient disk space", c.Etcd.DataDir, c.Kubelet.RootDir, c.ContainerRuntime.DataRootDir, "chronyc waitsync", "invalid Kubernetes node hostname"} {
		if !strings.Contains(command[2], text) {
			t.Fatalf("missing %q in generated command", text)
		}
	}
	c.Kubelet.IPAsName = true
	if strings.Contains(nodeReadinessStep(nil, c).Commands[0].ShellCommand[2], "invalid Kubernetes node hostname") {
		t.Fatal("hostname check must not reject nodes configured to use their IP as name")
	}
	// When explicitly enabled, execute the exact generated production step on a
	// real Linux host. Namespace fault injection is managed by the acceptance runner.
	if os.Getenv("KC_READINESS_LIVE") == "1" {
		output, err := exec.Command(command[0], command[1:]...).CombinedOutput()
		want := os.Getenv("KC_READINESS_WANT")
		if want == "" {
			if err != nil {
				t.Fatalf("healthy host rejected: %v: %s", err, output)
			}
		} else if err == nil || !strings.Contains(string(output), want) {
			t.Fatalf("want real failure %q, got %v: %s", want, err, output)
		}
		t.Logf("real generated readiness command: err=%v output=%s", err, output)
		raw, marshalErr := json.Marshal(operationv2.CommandStepPayload{Step: step})
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		task := &operations.OperationTask{Spec: operations.OperationTaskSpec{Payload: runtime.RawExtension{Raw: raw}}}
		_, execErr := (operationv2.CommandStepExecutor{}).Reconcile(context.Background(), task, io.Discard)
		if (execErr != nil) != (want != "") {
			t.Fatalf("real Agent executor failed to propagate readiness result: %v", execErr)
		}
		t.Logf("real CommandStep/v1 executor result: %v", execErr)

	}
}
