/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package k8s

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
)

func TestHealthWaitsThroughImagePullAndRequiresReady(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}})
	calls := 0
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		calls++
		pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "calico", Namespace: "kube-system"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}
		if calls < 3 {
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "calico", Image: "registry/calico/node:v3.31.5", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}}}
		} else {
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
		}
		return true, &corev1.PodList{Items: []corev1.Pod{pod}}, nil
	})
	health := &Health{Clientset: client}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := health.waitForReady(ctx, component.Options{}, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("health finished after %d polls, before image readiness", calls)
	}
}

func TestHealthTimeoutRetainsImageFailure(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "calico", Namespace: "kube-system"}, Status: corev1.PodStatus{Phase: corev1.PodPending, ContainerStatuses: []corev1.ContainerStatus{{Name: "node", Image: "registry/calico/node:v3.31.5", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ErrImagePull"}}}}}},
	)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	err := (&Health{Clientset: client}).waitForReady(ctx, component.Options{}, time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "ErrImagePull") || !strings.Contains(err.Error(), "registry/calico/node") {
		t.Fatalf("timeout lost actionable failure: %v", err)
	}
}

func TestHealthRejectsEmptyNodesAndMissingReadyCondition(t *testing.T) {
	for _, objects := range [][]runtime.Object{nil, {&corev1.Node{}}} {
		health := &Health{Clientset: fake.NewSimpleClientset(objects...)}
		if err := health.allNodesReady(context.Background(), component.Options{}); err == nil {
			t.Fatal("incomplete node state passed health")
		}
	}
}
