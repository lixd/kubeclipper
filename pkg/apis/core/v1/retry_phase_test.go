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

package v1

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestRetryClusterPhasePreservesOperationLifecycle(t *testing.T) {
	tests := []struct {
		name   string
		action string
		want   corev1.ClusterPhase
	}{
		{name: "create", action: corev1.OperationCreateCluster, want: corev1.ClusterInstalling},
		{name: "delete", action: corev1.OperationDeleteCluster, want: corev1.ClusterTerminating},
		{name: "upgrade", action: corev1.OperationUpgradeCluster, want: corev1.ClusterUpgrading},
		{name: "add nodes", action: corev1.OperationAddNodes, want: corev1.ClusterUpdating},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := &corev1.Operation{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{common.LabelOperationAction: tt.action}}}
			if got := retryClusterPhase(op); got != tt.want {
				t.Fatalf("retryClusterPhase() = %s, want %s", got, tt.want)
			}
		})
	}
}
