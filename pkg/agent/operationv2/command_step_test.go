/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package operationv2

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

// blockingStep waits until its context is done, the way the component retry
// loops (utils.RetryFunc) do, and returns the context error.
type blockingStep struct{}

func (blockingStep) Install(ctx context.Context, _ component.Options) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (blockingStep) Uninstall(context.Context, component.Options) ([]byte, error) { return nil, nil }

func (blockingStep) NewInstance() component.ObjectMeta { return &blockingStep{} }

func execTaskFor(t *testing.T, stepTimeout time.Duration) *operations.OperationTask {
	t.Helper()
	const identity = "test-blocking/v1/Blocking"
	if err := component.RegisterAgentStep(identity, &blockingStep{}); err != nil {
		// already registered by an earlier test in this package
		t.Logf("register agent step: %v", err)
	}
	step := corev1.Step{
		ID:     "step-1",
		Name:   "blockingStep",
		Action: corev1.ActionInstall,
		// Timeout is what the agent must enforce: the task context only carries
		// the operation deadline (R25).
		Timeout: metav1.Duration{Duration: stepTimeout},
		Commands: []corev1.Command{{
			Type:          corev1.CommandCustom,
			Identity:      identity,
			CustomCommand: json.RawMessage(`{}`),
		}},
	}
	payload, err := json.Marshal(CommandStepPayload{Step: step})
	if err != nil {
		t.Fatal(err)
	}
	return &operations.OperationTask{
		ObjectMeta: metav1.ObjectMeta{Name: "task-1"},
		Spec: operations.OperationTaskSpec{
			Payload: runtime.RawExtension{Raw: payload},
		},
	}
}

// A step that declares a timeout must stop at that timeout even though the
// task context (the operation deadline) is much further away: the nfs-csi
// checkCSIHealth step declared three minutes, ran for over six and only
// stopped at the operation deadline, and a cancel could not make progress
// because cooperative cancellation waits for the in-flight step (R25).
func TestCommandStepExecutorEnforcesStepTimeout(t *testing.T) {
	task := execTaskFor(t, 150*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	_, err := CommandStepExecutor{}.Reconcile(ctx, task, io.Discard)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reconcile() error = %v, want context.DeadlineExceeded so the worker marks the task TimedOut", err)
	}
	if elapsed > 5*time.Second {
		t.Fatalf("Reconcile() returned after %s, want the step timeout (150ms) to bound it", elapsed)
	}
}

// Without a declared step timeout the executor keeps the task context, so the
// operation deadline still governs (no behaviour change for steps that never
// set a timeout).
func TestCommandStepExecutorKeepsTaskContextWhenStepHasNoTimeout(t *testing.T) {
	task := execTaskFor(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := CommandStepExecutor{}.Reconcile(ctx, task, io.Discard)
	elapsed := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Reconcile() error = %v, want the task context deadline to surface", err)
	}
	if elapsed < 150*time.Millisecond || elapsed > 5*time.Second {
		t.Fatalf("Reconcile() returned after %s, want it bounded by the task context (~200ms)", elapsed)
	}
}
