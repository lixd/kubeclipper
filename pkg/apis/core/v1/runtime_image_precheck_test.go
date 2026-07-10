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

package v1

import (
	"context"
	"fmt"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestRequiredRuntimeImagesForKubernetesAndCalico(t *testing.T) {
	cluster := runtimeImagePrecheckCluster()
	images, err := requiredRuntimeImages(cluster)
	if err != nil {
		t.Fatalf("requiredRuntimeImages() error = %+v", err)
	}
	for _, want := range []string{
		"registry.local:5000/kube-apiserver:v1.36.1",
		"registry.local:5000/etcd:3.6.8-0",
		"registry.local:5000/coredns:v1.14.2",
		"registry.local:5000/pause:3.10.2",
		"registry.local:5000/fanux/lvscare:v1.1.1",
		"registry.local:5000/tigera/operator:v1.40.8",
		"registry.local:5000/calico/node:v3.31.5",
		"registry.local:5000/kubeclipper/kubectl:latest",
	} {
		if !containsString(images, want) {
			t.Fatalf("required images missing %s in %v", want, images)
		}
	}
}

func TestRequiredRuntimeImagesForSupportedKubernetesMinors(t *testing.T) {
	tests := []struct {
		name              string
		kubernetesVersion string
		cniVersion        string
		etcd              string
		coreDNS           string
		pause             string
		operator          string
	}{
		{
			name:              "v1.36",
			kubernetesVersion: "v1.36.1",
			cniVersion:        "v3.31.5",
			etcd:              "3.6.8-0",
			coreDNS:           "v1.14.2",
			pause:             "3.10.2",
			operator:          "v1.40.8",
		},
		{
			name:              "v1.35",
			kubernetesVersion: "v1.35.0",
			cniVersion:        "v3.29.6",
			etcd:              "3.6.6-0",
			coreDNS:           "v1.13.1",
			pause:             "3.10.1",
			operator:          "v1.36.14",
		},
		{
			name:              "v1.34",
			kubernetesVersion: "v1.34.2",
			cniVersion:        "v3.29.6",
			etcd:              "3.6.5-0",
			coreDNS:           "v1.12.1",
			pause:             "3.10.1",
			operator:          "v1.36.14",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := runtimeImagePrecheckCluster()
			cluster.KubernetesVersion = tt.kubernetesVersion
			cluster.CNI.Version = tt.cniVersion
			images, err := requiredRuntimeImages(cluster)
			if err != nil {
				t.Fatalf("requiredRuntimeImages() error = %+v", err)
			}
			for _, want := range []string{
				"registry.local:5000/kube-apiserver:" + tt.kubernetesVersion,
				"registry.local:5000/etcd:" + tt.etcd,
				"registry.local:5000/coredns:" + tt.coreDNS,
				"registry.local:5000/pause:" + tt.pause,
				"registry.local:5000/tigera/operator:" + tt.operator,
				"registry.local:5000/calico/node:" + tt.cniVersion,
			} {
				if !containsString(images, want) {
					t.Fatalf("required images missing %s in %v", want, images)
				}
			}
		})
	}
}

func TestPrecheckRuntimeImagesRequiresLocalRegistry(t *testing.T) {
	cluster := runtimeImagePrecheckCluster()
	cluster.LocalRegistry = ""
	err := (&handler{}).precheckRuntimeImages(context.Background(), cluster)
	if err == nil || !strings.Contains(err.Error(), "requires localRegistry") {
		t.Fatalf("precheckRuntimeImages() error = %v", err)
	}
}

func TestPrecheckRuntimeImagesReportsMissingImages(t *testing.T) {
	old := runtimeImageExists
	defer func() { runtimeImageExists = old }()
	runtimeImageExists = func(ctx context.Context, ref string) error {
		if strings.Contains(ref, "kube-apiserver") {
			return fmt.Errorf("missing")
		}
		return nil
	}
	err := (&handler{}).precheckRuntimeImages(context.Background(), runtimeImagePrecheckCluster())
	if err == nil || !strings.Contains(err.Error(), "registry.local:5000/kube-apiserver:v1.36.1") {
		t.Fatalf("precheckRuntimeImages() error = %v", err)
	}
}

func TestPrecheckRuntimeImagesSkipsOnlineCluster(t *testing.T) {
	cluster := runtimeImagePrecheckCluster()
	cluster.Annotations = map[string]string{}
	err := (&handler{}).precheckRuntimeImages(context.Background(), cluster)
	if err != nil {
		t.Fatalf("precheckRuntimeImages() error = %+v", err)
	}
}

func runtimeImagePrecheckCluster() *corev1.Cluster {
	return &corev1.Cluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "demo",
			Annotations: map[string]string{common.AnnotationOffline: "true"},
		},
		KubernetesVersion: "v1.36.1",
		LocalRegistry:     "registry.local:5000",
		CNI: corev1.CNI{
			Type:    "calico",
			Version: "v3.31.5",
		},
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
