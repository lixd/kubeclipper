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

package cluster

import (
	"testing"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/pkg/clusteroperation"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestNodeOptionsValidate(t *testing.T) {
	cmd := &cobra.Command{}
	tests := []struct {
		name    string
		opts    NodeOptions
		wantErr bool
	}{
		{
			name: "add worker",
			opts: NodeOptions{Operation: clusteroperation.NodesOperationAdd, ClusterName: "demo", Workers: []string{"node-1"}},
		},
		{
			name: "remove worker",
			opts: NodeOptions{Operation: clusteroperation.NodesOperationRemove, ClusterName: "demo", Workers: []string{"node-1"}},
		},
		{
			name:    "invalid operation",
			opts:    NodeOptions{Operation: "replace", ClusterName: "demo", Workers: []string{"node-1"}},
			wantErr: true,
		},
		{
			name:    "missing cluster",
			opts:    NodeOptions{Operation: clusteroperation.NodesOperationAdd, Workers: []string{"node-1"}},
			wantErr: true,
		},
		{
			name:    "missing worker",
			opts:    NodeOptions{Operation: clusteroperation.NodesOperationAdd, ClusterName: "demo"},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.opts.Validate(cmd)
			if (err != nil) != test.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestWorkerEligible(t *testing.T) {
	cluster := &corev1.Cluster{
		Masters: corev1.WorkerNodeList{{ID: "master-1"}},
		Workers: corev1.WorkerNodeList{{ID: "worker-1"}},
	}
	tests := []struct {
		operation clusteroperation.NodesPatchOperation
		node      string
		want      bool
	}{
		{operation: clusteroperation.NodesOperationAdd, node: "worker-2", want: true},
		{operation: clusteroperation.NodesOperationAdd, node: "master-1", want: false},
		{operation: clusteroperation.NodesOperationAdd, node: "worker-1", want: false},
		{operation: clusteroperation.NodesOperationRemove, node: "worker-1", want: true},
		{operation: clusteroperation.NodesOperationRemove, node: "master-1", want: false},
		{operation: clusteroperation.NodesOperationRemove, node: "worker-2", want: false},
	}
	for _, test := range tests {
		if got := workerEligible(test.operation, test.node, cluster); got != test.want {
			t.Errorf("workerEligible(%s, %s) = %v, want %v", test.operation, test.node, got, test.want)
		}
	}
}
