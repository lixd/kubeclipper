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

package clusteroperation

import (
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

var (
	// c2 is kept for backward compatibility of other fixtures; the MakeCompare
	// tests build a fresh cluster per case because MakeCompare mutates it.
	c2     = newTestCluster()
	master = v1.WorkerNodeList{
		{
			ID: "1e3ea00f-1403-46e5-a486-70e4cb29d541",
		},
		{
			ID: "43ed594a-a76f-4370-a14d-551e7b6153de",
		},
		{
			ID: "c7a91d86-cd53-4c3f-85b0-fbc657778067",
		},
	}
	worker = v1.WorkerNodeList{
		{
			ID: "4cf1ad74-704c-4290-a523-e524e930245d",
		},
		{
			ID: "ae4ba282-27f9-4a93-8fe9-63f786781d48",
		},
	}
)

func newTestCluster() *v1.Cluster {
	return &v1.Cluster{
		TypeMeta: metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{
			Name: "demo",
		},

		Masters: v1.WorkerNodeList{
			{
				ID: "1e3ea00f-1403-46e5-a486-70e4cb29d541",
			},
			{
				ID: "43ed594a-a76f-4370-a14d-551e7b6153de",
			},
			{
				ID: "c7a91d86-cd53-4c3f-85b0-fbc657778067",
			},
		},
		Workers: v1.WorkerNodeList{
			{
				ID: "4cf1ad74-704c-4290-a523-e524e930245d",
			},
			{
				ID: "ae4ba282-27f9-4a93-8fe9-63f786781d48",
			},
		},
		ContainerRuntime: v1.ContainerRuntime{
			Type:        "docker",
			Version:     "19.03.12",
			DataRootDir: "/var/lib/docker",
		},
		KubeProxy: v1.KubeProxy{},
		Etcd:      v1.Etcd{},
		CNI: v1.CNI{
			ImageRegistry: "172.20.150.138:5000",
			Type:          "calico",
			Version:       "v3.21.2",
			Calico: &v1.Calico{
				IPv4AutoDetection: "first-found",
				IPv6AutoDetection: "first-found",
				Mode:              "Overlay-Vxlan-All",
				IPManger:          true,
				MTU:               1440,
			},
		},
		Networking: v1.Networking{
			IPFamily:      v1.IPFamilyIPv4,
			Services:      v1.NetworkRanges{CIDRBlocks: []string{"10.96.0.0/16"}},
			Pods:          v1.NetworkRanges{CIDRBlocks: []string{"172.25.0.0/24"}},
			DNSDomain:     "cluster.local",
			ProxyMode:     "ipvs",
			WorkerNodeVip: "169.254.169.100",
		},
	}
}

//extraMeta = &component.ExtraMetadata{
//	Masters: []component.Node{
//		{
//			ID:       "1e3ea00f-1403-46e5-a486-70e4cb29d541",
//			IPv4:     "192.168.1.1",
//			NodeIPv4: "192.168.2.1",
//			Region:   "default",
//		},
//		{
//			ID:       "43ed594a-a76f-4370-a14d-551e7b6153de",
//			IPv4:     "192.168.1.2",
//			NodeIPv4: "192.168.2.2",
//			Region:   "default",
//		},
//		{
//			ID:       "c7a91d86-cd53-4c3f-85b0-fbc657778067",
//			IPv4:     "192.168.1.3",
//			NodeIPv4: "192.168.2.3",
//			Region:   "default",
//		},
//	},
//	Workers: []component.Node{
//		{
//			ID:       "4cf1ad74-704c-4290-a523-e524e930245d",
//			IPv4:     "192.168.1.4",
//			NodeIPv4: "192.168.2.4",
//			Region:   "default",
//		},
//		{
//			ID:       "ae4ba282-27f9-4a93-8fe9-63f786781d48",
//			IPv4:     "192.168.1.5",
//			NodeIPv4: "192.168.2.5",
//			Region:   "default",
//		},
//	},
//}

func Test_MakeCompare(t *testing.T) {
	type args struct {
		cluster   *v1.Cluster
		patchNode *PatchNodes
	}
	tests := []struct {
		name         string
		arg          args
		wantErr      error
		wantMasters  int
		wantWorkers  int
		wantPatchLen int
	}{
		{
			name: "addWorker",
			arg: args{
				cluster: newTestCluster(),
				patchNode: &PatchNodes{
					Operation: "add",
					Nodes:     worker,
					Role:      "worker",
				},
			},
			wantErr:     nil,
			wantWorkers: 2,
		},
		{
			name: "removeWorker",
			arg: args{
				cluster: newTestCluster(),
				patchNode: &PatchNodes{
					Operation: "remove",
					Nodes:     worker,
					Role:      "worker",
				},
			},
			wantErr:     nil,
			wantWorkers: 0,
		},
		{
			name: "addMasterAlreadyInCluster",
			arg: args{
				cluster: newTestCluster(),
				patchNode: &PatchNodes{
					Operation: "add",
					Nodes:     master,
					Role:      "master",
				},
			},
			wantErr:      nil,
			wantMasters:  3,
			wantPatchLen: 0,
		},
		{
			name: "addMaster",
			arg: args{
				cluster: newTestCluster(),
				patchNode: &PatchNodes{
					Operation: "add",
					Nodes: v1.WorkerNodeList{
						{ID: "6b8456e8-2489-4321-bbb0-f8d75c065384"},
					},
					Role: "master",
				},
			},
			wantErr:      nil,
			wantMasters:  4,
			wantPatchLen: 1,
		},
		{
			name: "removeMasterKeepsQuorum",
			arg: args{
				cluster: newTestCluster(),
				patchNode: &PatchNodes{
					Operation: "remove",
					Nodes: v1.WorkerNodeList{
						{ID: "c7a91d86-cd53-4c3f-85b0-fbc657778067"},
					},
					Role: "master",
				},
			},
			wantErr:      nil,
			wantMasters:  2,
			wantPatchLen: 1,
		},
		{
			name: "removeMasterBreaksQuorum",
			arg: args{
				cluster: newTestCluster(),
				patchNode: &PatchNodes{
					Operation: "remove",
					Nodes: v1.WorkerNodeList{
						{ID: "43ed594a-a76f-4370-a14d-551e7b6153de"},
						{ID: "c7a91d86-cd53-4c3f-85b0-fbc657778067"},
					},
					Role: "master",
				},
			},
			wantErr: ErrInvalidNodesTopology,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.arg.patchNode.MakeCompare(test.arg.cluster); !errors.Is(err, test.wantErr) {
				t.Errorf(" MakeCompare() err: %v ", err)
			}
			if test.wantErr != nil {
				return
			}
			if test.wantMasters != 0 && len(test.arg.cluster.Masters) != test.wantMasters {
				t.Errorf(" MakeCompare() masters: got %d want %d", len(test.arg.cluster.Masters), test.wantMasters)
			}
			if test.wantWorkers != 0 && len(test.arg.cluster.Workers) != test.wantWorkers {
				t.Errorf(" MakeCompare() workers: got %d want %d", len(test.arg.cluster.Workers), test.wantWorkers)
			}
			if test.wantPatchLen != 0 && len(test.arg.patchNode.Nodes) != test.wantPatchLen {
				t.Errorf(" MakeCompare() patch nodes: got %d want %d", len(test.arg.patchNode.Nodes), test.wantPatchLen)
			}
		})
	}
}

//func Test_MakeOperation(t *testing.T) {
//	type args struct {
//		cluster    *v1.Cluster
//		meta       component.ExtraMetadata
//		patchNodes *PatchNodes
//	}
//	tests := []struct {
//		name    string
//		arg     args
//		wantErr error
//	}{
//		{
//			name: "test add worker node operation",
//			arg: args{
//				cluster: newTestCluster(),
//				meta:    *extraMeta,
//				patchNodes: &PatchNodes{
//					Operation: "add",
//					Nodes: []v1.WorkerNode{
//						{
//							ID: "6b8456e8-2489-4321-bbb0-f8d75c065384",
//						},
//					},
//					Role: "worker",
//				},
//			},
//			wantErr: nil,
//		},
//		{
//			name: "test remove worker node operation",
//			arg: args{
//				cluster: newTestCluster(),
//				meta:    *extraMeta,
//				patchNodes: &PatchNodes{
//					Operation: "remove",
//					Nodes: []v1.WorkerNode{
//						{
//							ID: "4cf1ad74-704c-4290-a523-e524e930245d",
//						},
//					},
//					Role: "worker",
//				},
//			},
//			wantErr: nil,
//		},
//		{
//			name: "test add master node operation",
//			arg: args{
//				cluster: newTestCluster(),
//				meta:    *extraMeta,
//				patchNodes: &PatchNodes{
//					Operation: "remove",
//					Nodes: []v1.WorkerNode{
//						{
//							ID: "1e3ea00f-1403-46e5-a486-70e4cb29d541",
//						},
//					},
//					Role: "master",
//				},
//			},
//			wantErr: ErrInvalidNodesRole,
//		},
//	}
//	for _, test := range tests {
//		t.Run(test.name, func(t *testing.T) {
//			_, err := test.arg.patchNodes.MakeOperation(test.arg.meta, test.arg.cluster)
//			if err != nil && err != test.wantErr && !IgnoreError(err) {
//				t.Errorf(" MakeOperation() error: %v ", err)
//			}
//		})
//	}
//
//}

const (
	availableMasterError = "no master node available"
)

func IgnoreError(err error) bool {
	return strings.Contains(err.Error(), availableMasterError)
}
