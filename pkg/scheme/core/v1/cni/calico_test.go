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

package cni

import (
	"bytes"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/constatns"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestCNI_renderCalicoTo(t *testing.T) {
	tests := []struct {
		name    string
		stepper CalicoRunnable
		wantW   string
		wantErr bool
	}{
		{
			name: "base",
			stepper: CalicoRunnable{
				KubeletDataDir: "/var/lib/kubelet",
				BaseCni: BaseCni{
					DualStack:   true,
					PodIPv4CIDR: constatns.ClusterPodSubnet,
					PodIPv6CIDR: "aaa:bbb",
					CNI: v1.CNI{
						LocalRegistry: "172.0.0.1:5000",
						Type:          "calico",
						Version:       "v3.26.1",
						Calico: &v1.Calico{
							IPv4AutoDetection: "first-found",
							IPv6AutoDetection: "first-found",
							Mode:              "Overlay-Vxlan-All",
							IPManger:          true,
							MTU:               1440,
						},
					},
				},
			},
		},
		{
			name: "v3.26.1-with-kubeletDataDir",
			stepper: CalicoRunnable{
				KubeletDataDir: "/custom/kubelet",
				BaseCni: BaseCni{
					DualStack:   false,
					PodIPv4CIDR: "10.244.0.0/16",
					CNI: v1.CNI{
						LocalRegistry: "",
						Type:          "calico",
						Version:       "v3.26.1",
						Calico: &v1.Calico{
							IPv4AutoDetection: "first-found",
							Mode:              "Overlay-IPIP-All",
							IPManger:          true,
							MTU:               1440,
						},
					},
				},
			},
		},
		{
			name: "v3.26.1-without-kubeletDataDir",
			stepper: CalicoRunnable{
				KubeletDataDir: "",
				BaseCni: BaseCni{
					DualStack:   false,
					PodIPv4CIDR: "10.244.0.0/16",
					CNI: v1.CNI{
						LocalRegistry: "",
						Type:          "calico",
						Version:       "v3.26.1",
						Calico: &v1.Calico{
							IPv4AutoDetection: "first-found",
							Mode:              "Overlay-IPIP-All",
							IPManger:          true,
							MTU:               1440,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		tt.stepper.NodeAddressDetectionV4 = ParseNodeAddressDetection(tt.stepper.Calico.IPv4AutoDetection)
		tt.stepper.NodeAddressDetectionV6 = ParseNodeAddressDetection(tt.stepper.Calico.IPv6AutoDetection)
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			err := tt.stepper.renderCalicoTo(w)
			if (err != nil) != tt.wantErr {
				t.Errorf("renderCalicoTo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			output := w.String()
			if tt.name == "v3.26.1-with-kubeletDataDir" {
				if !strings.Contains(output, "/custom/kubelet") {
					t.Errorf("rendered template should contain custom kubeletDataDir: /custom/kubelet, got: %s", output)
				}
			}
			if tt.name == "v3.26.1-without-kubeletDataDir" {
				if !strings.Contains(output, "/var/lib/kubelet") {
					t.Errorf("rendered template should contain default kubeletDataDir: /var/lib/kubelet, got: %s", output)
				}
			}
			t.Log(output)
		})
	}
}
