/*
 *
 *  * Copyright 2021 KubeClipper Authors.
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
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/kubeclipper/kubeclipper/pkg/constatns"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func Test_kubectlTerminal_renderTo(t *testing.T) {
	c := &KubectlTerminal{
		// ImageRegistryAddr: "192.168.234.130:5000",
		ImageRegistryAddr: "",
	}
	w := &bytes.Buffer{}
	err := c.renderTo(w)
	if err != nil {
		t.Fatal(err)
	}
	rendered := w.String()
	if strings.Count(rendered, "requiredDuringSchedulingIgnoredDuringExecution:") != 1 {
		t.Fatalf("expected one node affinity requirement, got manifest:\n%s", rendered)
	}
	if !strings.Contains(rendered, "node-role.kubernetes.io/master") ||
		!strings.Contains(rendered, "node-role.kubernetes.io/control-plane") {
		t.Fatalf("manifest must allow both control-plane label forms:\n%s", rendered)
	}
}

func TestKubectlTerminalInstallStepsWaitForApiConvergence(t *testing.T) {
	steps, err := (&KubectlTerminal{}).InstallSteps([]v1.StepNode{{ID: "master-1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 {
		t.Fatalf("got %d steps, want 1", len(steps))
	}
	if got, want := steps[0].Timeout.Duration, 2*time.Minute; got != want {
		t.Fatalf("timeout = %s, want %s", got, want)
	}
	if got, want := steps[0].RetryTimes, int32(3); got != want {
		t.Fatalf("retryTimes = %d, want %d", got, want)
	}
}

func TestFinalizeRemovedNodeState(t *testing.T) {
	cluster := &v1.Cluster{ContainerRuntime: v1.ContainerRuntime{DataRootDir: "/data/containerd"}}
	steps := FinalizeRemovedNodeState(cluster, []v1.StepNode{{ID: "worker-1"}})
	if len(steps) != 2 || steps[0].Name != "unmountCalicoRuntimeState" || steps[1].Name != "finalizeRemovedNodeState" {
		t.Fatalf("unexpected final cleanup steps: %+v", steps)
	}
	if len(steps[1].Commands) != 1 {
		t.Fatalf("unexpected cleanup commands: %+v", steps[1].Commands)
	}
	got := strings.Join(steps[1].Commands[0].ShellCommand, " ")
	for _, path := range []string{"/etc/containerd", "/data/containerd", "/etc/cni", "/var/lib/cni", "/var/run/calico"} {
		if !strings.Contains(got, path) {
			t.Errorf("cleanup command %q does not contain %q", got, path)
		}
	}
}

func TestKubeadmConfig_renderTo(t *testing.T) {
	type fields struct {
		ClusterConfigAPIVersion string
		ContainerRuntime        string
		Etcd                    v1.Etcd
		Networking              v1.Networking
		KubeProxy               v1.KubeProxy
		Kubelet                 v1.Kubelet
		ClusterName             string
		KubernetesVersion       string
		ControlPlaneEndpoint    string
		CertSANs                []string
		ImageRegistry           string
		FeatureGates            map[string]bool
		IgnorePreflightErrors   string
	}
	tests := []struct {
		name   string
		fields fields
		wantW  string
	}{
		{
			name: "base",
			fields: fields{
				ClusterConfigAPIVersion: "v1beta3",
				ContainerRuntime:        "containerd",
				Etcd:                    v1.Etcd{DataDir: "/var/lib/etcd"},
				Networking: v1.Networking{
					IPFamily:      v1.IPFamilyIPv4,
					Services:      v1.NetworkRanges{CIDRBlocks: []string{constatns.ClusterServiceSubnet}},
					Pods:          v1.NetworkRanges{CIDRBlocks: []string{constatns.ClusterPodSubnet}},
					DNSDomain:     "cluster.local",
					ProxyMode:     "ipvs",
					WorkerNodeVip: "8.8.8.8",
				},
				KubeProxy:             v1.KubeProxy{},
				Kubelet:               v1.Kubelet{RootDir: "/var/lib/kubelet"},
				ClusterName:           "test-cluster",
				KubernetesVersion:     "v1.23.6",
				ControlPlaneEndpoint:  "apiserver.cluster.local:6443",
				CertSANs:              []string{"127.0.0.1"},
				ImageRegistry:         "127.0.0.1:5000",
				IgnorePreflightErrors: "",
			},
		},
		{
			name: "node-ip as node-name",
			fields: fields{
				ClusterConfigAPIVersion: "v1beta3",
				ContainerRuntime:        "containerd",
				Etcd:                    v1.Etcd{DataDir: "/var/lib/etcd"},
				Networking: v1.Networking{
					IPFamily:      v1.IPFamilyIPv4,
					Services:      v1.NetworkRanges{CIDRBlocks: []string{constatns.ClusterServiceSubnet}},
					Pods:          v1.NetworkRanges{CIDRBlocks: []string{constatns.ClusterPodSubnet}},
					DNSDomain:     "cluster.local",
					ProxyMode:     "ipvs",
					WorkerNodeVip: "8.8.8.8",
				},
				KubeProxy:             v1.KubeProxy{},
				Kubelet:               v1.Kubelet{RootDir: "/var/lib/kubelet", NodeIP: "127.0.0.1", IPAsName: true},
				ClusterName:           "test-cluster",
				KubernetesVersion:     "v1.23.6",
				ControlPlaneEndpoint:  "apiserver.cluster.local:6443",
				CertSANs:              []string{"127.0.0.1"},
				ImageRegistry:         "127.0.0.1:5000",
				IgnorePreflightErrors: "NumCPU",
			},
		},
		{
			name: "add featureGates",
			fields: fields{
				ClusterConfigAPIVersion: "v1beta3",
				ContainerRuntime:        "containerd",
				Etcd:                    v1.Etcd{DataDir: "/var/lib/etcd"},
				Networking: v1.Networking{
					IPFamily:      v1.IPFamilyIPv4,
					Services:      v1.NetworkRanges{CIDRBlocks: []string{constatns.ClusterServiceSubnet}},
					Pods:          v1.NetworkRanges{CIDRBlocks: []string{constatns.ClusterPodSubnet}},
					DNSDomain:     "cluster.local",
					ProxyMode:     "ipvs",
					WorkerNodeVip: "8.8.8.8",
				},
				KubeProxy:            v1.KubeProxy{},
				Kubelet:              v1.Kubelet{RootDir: "/var/lib/kubelet", NodeIP: "127.0.0.1", IPAsName: true},
				ClusterName:          "test-cluster",
				KubernetesVersion:    "v1.23.6",
				ControlPlaneEndpoint: "apiserver.cluster.local:6443",
				CertSANs:             []string{"127.0.0.1"},
				ImageRegistry:        "127.0.0.1:5000",
				FeatureGates: map[string]bool{
					"AdmissionWebhookMatchConditions": true,
					"AggregatedDiscoveryEndpoint":     true,
				},
				IgnorePreflightErrors: "NumCPU,Swap",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stepper := &KubeadmConfig{
				ClusterConfigAPIVersion: tt.fields.ClusterConfigAPIVersion,
				ContainerRuntime:        tt.fields.ContainerRuntime,
				Etcd:                    tt.fields.Etcd,
				Networking:              tt.fields.Networking,
				KubeProxy:               tt.fields.KubeProxy,
				Kubelet:                 tt.fields.Kubelet,
				ClusterName:             tt.fields.ClusterName,
				KubernetesVersion:       tt.fields.KubernetesVersion,
				ControlPlaneEndpoint:    tt.fields.ControlPlaneEndpoint,
				CertSANs:                tt.fields.CertSANs,
				ImageRegistry:           tt.fields.ImageRegistry,
				FeatureGates:            tt.fields.FeatureGates,
				IgnorePreflightErrors:   parseIgnorePreflightErrors(tt.fields.IgnorePreflightErrors),
			}
			t.Logf("IgnorePreflightErrors %v,len %v", stepper.IgnorePreflightErrors, len(stepper.IgnorePreflightErrors))
			w := &bytes.Buffer{}
			err := stepper.renderTo(w)
			if err != nil {
				t.Errorf("renderTo() error = %v", err)
				return
			}
			t.Log(w.String())
		})
	}
}

func TestKubeadmConfigRendersNativeImageRepository(t *testing.T) {
	stepper := &KubeadmConfig{
		ClusterConfigAPIVersion: "v1beta4",
		ImageRegistry:           "127.0.0.1:5000",
	}
	output := &bytes.Buffer{}
	if err := stepper.renderTo(output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "imageRepository: 127.0.0.1:5000") {
		t.Fatalf("kubeadm image repository was not rendered:\n%s", output.String())
	}
	if strings.Contains(output.String(), "imageRegistry:") {
		t.Fatalf("kubeadm does not support imageRegistry:\n%s", output.String())
	}
}

func TestClusterNode_renderIPVSCarePod(t *testing.T) {
	type fields struct {
		NodeRole            string
		WorkerNodeVIP       string
		Masters             map[string]string
		ImageRegistry       string
		APIServerDomainName string
		JoinMasterIP        string
		EtcdDataPath        string
	}
	tests := []struct {
		name    string
		fields  fields
		wantW   string
		wantErr bool
	}{
		{
			name: "base",
			fields: fields{
				NodeRole:      NodeRoleWorker,
				WorkerNodeVIP: "10.0.0.2",
				Masters: map[string]string{
					"id": "dee4ee99-d102-48f6-8382-72a61160c353",
				},
				ImageRegistry:       "127.0.0.1:5000",
				APIServerDomainName: "domain",
				JoinMasterIP:        "10.0.0.1",
				EtcdDataPath:        "/var/etcd/lib",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stepper := &ClusterNode{
				NodeRole:            tt.fields.NodeRole,
				WorkerNodeVIP:       tt.fields.WorkerNodeVIP,
				Masters:             tt.fields.Masters,
				ImageRegistry:       tt.fields.ImageRegistry,
				APIServerDomainName: tt.fields.APIServerDomainName,
				JoinMasterIP:        tt.fields.JoinMasterIP,
				EtcdDataPath:        tt.fields.EtcdDataPath,
			}
			w := &bytes.Buffer{}
			err := stepper.renderIPVSCarePod(w)
			if err != nil {
				t.Errorf("renderIPVSCarePod() error = %v", err)
				return
			}
			t.Log(w.String())
		})
	}
}

func TestKubeadmConfig_renderJoin(t *testing.T) {
	type fields struct {
		ClusterConfigAPIVersion string
		ContainerRuntime        string
		Etcd                    v1.Etcd
		Networking              v1.Networking
		KubeProxy               v1.KubeProxy
		Kubelet                 v1.Kubelet
		ClusterName             string
		KubernetesVersion       string
		ControlPlaneEndpoint    string
		CertSANs                []string
		ImageRegistry           string
		Offline                 bool
		IsControlPlane          bool
		CACertHashes            string
		BootstrapToken          string
		CertificateKey          string
	}
	tests := []struct {
		name    string
		fields  fields
		wantW   string
		wantErr bool
	}{
		{
			name: "base",
			fields: fields{
				ClusterConfigAPIVersion: "v1beta3",
				ContainerRuntime:        "containerd",
				Etcd:                    v1.Etcd{DataDir: "/var/lib/etcd"},
				Networking: v1.Networking{
					IPFamily:      v1.IPFamilyIPv4,
					Services:      v1.NetworkRanges{CIDRBlocks: []string{constatns.ClusterServiceSubnet}},
					Pods:          v1.NetworkRanges{CIDRBlocks: []string{constatns.ClusterPodSubnet}},
					DNSDomain:     "cluster.local",
					ProxyMode:     "ipvs",
					WorkerNodeVip: "8.8.8.8",
				},
				KubeProxy:            v1.KubeProxy{},
				Kubelet:              v1.Kubelet{RootDir: "/var/lib/kubelet"},
				ClusterName:          "test-cluster",
				KubernetesVersion:    "v1.23.6",
				ControlPlaneEndpoint: "apiserver.cluster.local:6443",
				CertSANs:             []string{"127.0.0.1"},
				ImageRegistry:        "127.0.0.1:5000",
				Offline:              true,
				IsControlPlane:       true,
				CACertHashes:         "hash1",
				BootstrapToken:       "BootstrapToken",
				CertificateKey:       "CertificateKey",
			},
		},
		{
			name: "node-ip as node-name",
			fields: fields{
				ClusterConfigAPIVersion: "v1beta3",
				ContainerRuntime:        "containerd",
				Etcd:                    v1.Etcd{DataDir: "/var/lib/etcd"},
				Networking: v1.Networking{
					IPFamily:      v1.IPFamilyIPv4,
					Services:      v1.NetworkRanges{CIDRBlocks: []string{constatns.ClusterServiceSubnet}},
					Pods:          v1.NetworkRanges{CIDRBlocks: []string{constatns.ClusterPodSubnet}},
					DNSDomain:     "cluster.local",
					ProxyMode:     "ipvs",
					WorkerNodeVip: "8.8.8.8",
				},
				KubeProxy:            v1.KubeProxy{},
				Kubelet:              v1.Kubelet{RootDir: "/var/lib/kubelet", NodeIP: "127.0.0.1", IPAsName: true},
				ClusterName:          "test-cluster",
				KubernetesVersion:    "v1.23.6",
				ControlPlaneEndpoint: "apiserver.cluster.local:6443",
				CertSANs:             []string{"127.0.0.1"},
				ImageRegistry:        "127.0.0.1:5000",
				Offline:              true,
				IsControlPlane:       true,
				CACertHashes:         "hash1",
				BootstrapToken:       "BootstrapToken",
				CertificateKey:       "CertificateKey",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stepper := &KubeadmConfig{
				ClusterConfigAPIVersion: tt.fields.ClusterConfigAPIVersion,
				ContainerRuntime:        tt.fields.ContainerRuntime,
				Etcd:                    tt.fields.Etcd,
				Networking:              tt.fields.Networking,
				KubeProxy:               tt.fields.KubeProxy,
				Kubelet:                 tt.fields.Kubelet,
				ClusterName:             tt.fields.ClusterName,
				KubernetesVersion:       tt.fields.KubernetesVersion,
				ControlPlaneEndpoint:    tt.fields.ControlPlaneEndpoint,
				CertSANs:                tt.fields.CertSANs,
				ImageRegistry:           tt.fields.ImageRegistry,
				Offline:                 tt.fields.Offline,
				IsControlPlane:          tt.fields.IsControlPlane,
				CACertHashes:            tt.fields.CACertHashes,
				BootstrapToken:          tt.fields.BootstrapToken,
				CertificateKey:          tt.fields.CertificateKey,
			}
			w := &bytes.Buffer{}
			err := stepper.renderJoin(w)
			if err != nil {
				t.Errorf("renderJoin() error = %v", err)
				return
			}
			t.Log(w.String())
		})
	}
}
