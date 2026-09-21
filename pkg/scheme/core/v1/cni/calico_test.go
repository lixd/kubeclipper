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
	"encoding/json"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
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
						ImageRegistry: "172.0.0.1:5000",
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
						ImageRegistry: "",
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
						ImageRegistry: "",
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
			name: "v3.29.6",
			stepper: CalicoRunnable{
				KubeletDataDir: "/var/lib/kubelet",
				BaseCni: BaseCni{
					ResolvedImageRegistry: "172.0.0.1:5000",
					DualStack:             true,
					PodIPv4CIDR:           constatns.ClusterPodSubnet,
					PodIPv6CIDR:           "fd00::/64",
					CNI: v1.CNI{
						ImageRegistry: "172.0.0.1:5000",
						Type:          "calico",
						Version:       "v3.29.6",
						Calico: &v1.Calico{
							IPv4AutoDetection: "interface=eth0",
							IPv6AutoDetection: "interface=eth0",
							Mode:              "Overlay-Vxlan-All",
							IPManger:          true,
							MTU:               1440,
						},
					},
				},
			},
		},
		{
			name: "v3.31.5",
			stepper: CalicoRunnable{
				KubeletDataDir: "/var/lib/kubelet",
				BaseCni: BaseCni{
					ResolvedImageRegistry: "172.0.0.1:5000",
					DualStack:             true,
					PodIPv4CIDR:           constatns.ClusterPodSubnet,
					PodIPv6CIDR:           "fd00::/64",
					CNI: v1.CNI{
						ImageRegistry: "172.0.0.1:5000",
						Type:          "calico",
						Version:       "v3.31.5",
						Calico: &v1.Calico{
							IPv4AutoDetection: "interface=eth0",
							IPv6AutoDetection: "interface=eth0",
							Mode:              "Overlay-Vxlan-All",
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
			if tt.name == "v3.29.6" {
				if !strings.Contains(output, "v1.36.14") {
					t.Errorf("rendered template should contain tigera operator version v1.36.14, got: %s", output)
				}
				if !strings.Contains(output, "v3.29.6") {
					t.Errorf("rendered template should contain calicoctl version v3.29.6, got: %s", output)
				}
			}
			if tt.name == "v3.31.5" {
				if !strings.Contains(output, "registry: 172.0.0.1:5000") {
					t.Errorf("rendered template should use the resolved Registry address, got: %s", output)
				}
				if !strings.Contains(output, "v1.40.8") {
					t.Errorf("rendered template should contain tigera operator version v1.40.8, got: %s", output)
				}
				if !strings.Contains(output, "v3.31.5") {
					t.Errorf("rendered template should contain calicoctl version v3.31.5, got: %s", output)
				}
				if !strings.Contains(output, `kubeletVolumePluginPath: "None"`) {
					t.Errorf("rendered template should contain kubeletVolumePluginPath None, got: %s", output)
				}
				if !strings.Contains(output, "goldmane") {
					t.Errorf("rendered template should contain goldmane section, got: %s", output)
				}
				if !strings.Contains(output, "whisker") {
					t.Errorf("rendered template should contain whisker section, got: %s", output)
				}
			}
			t.Log(output)
		})
	}
}

func TestCalicoRuntimeImageRegistryIsSerialized(t *testing.T) {
	runnable := CalicoRunnable{BaseCni: BaseCni{ResolvedImageRegistry: "127.0.0.1:5000"}}
	data, err := json.Marshal(runnable)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"imageRegistry":"127.0.0.1:5000"`) {
		t.Fatalf("resolved Registry address missing from runtime command: %s", data)
	}
}

// Regression: an API request with cni.type=calico but without the optional
// calico block used to panic in InitStep (nil pointer dereference) and turned
// into a 500 from the create/dryRun handlers.
func TestCalicoInitStepDefaultsWhenCalicoBlockOmitted(t *testing.T) {
	cni := &v1.CNI{Type: "calico", Version: "v3.29.6"}
	networking := &v1.Networking{
		IPFamily: v1.IPFamilyIPv4,
		Pods:     v1.NetworkRanges{CIDRBlocks: []string{"10.244.0.0/16"}},
	}
	stepper := (&CalicoRunnable{}).InitStep(&component.ExtraMetadata{KubeletDataDir: "/var/lib/kubelet"}, cni, networking)
	r, ok := stepper.(*CalicoRunnable)
	if !ok {
		t.Fatalf("unexpected stepper type %T", stepper)
	}
	if r.Calico == nil {
		t.Fatal("InitStep must fill the missing calico block instead of keeping a nil pointer")
	}
	if r.Calico.IPv4AutoDetection != "first-found" || r.Calico.IPv6AutoDetection != "first-found" {
		t.Errorf("auto detection defaults not applied: %+v", r.Calico)
	}
	if r.Calico.Mode != "Overlay-Vxlan-All" || r.Calico.MTU != 1440 || !r.Calico.IPManger {
		t.Errorf("calico defaults do not match kcctl create flags: %+v", r.Calico)
	}
	if cni.Calico != nil {
		t.Error("InitStep must not mutate the caller's CNI object")
	}
	if r.NodeAddressDetectionV4.Type != "first-found" || r.NodeAddressDetectionV6.Type != "first-found" {
		t.Errorf("node address detection not derived from defaults: %+v %+v", r.NodeAddressDetectionV4, r.NodeAddressDetectionV6)
	}
	w := &bytes.Buffer{}
	if err := r.renderCalicoTo(w); err != nil {
		t.Fatalf("renderCalicoTo with defaulted calico block: %v", err)
	}
}

func TestCalicoInitStepPartialCalicoBlock(t *testing.T) {
	cni := &v1.CNI{
		Type:    "calico",
		Version: "v3.29.6",
		Calico:  &v1.Calico{Mode: "BGP", MTU: 1500},
	}
	networking := &v1.Networking{
		IPFamily: v1.IPFamilyDualStack,
		Pods:     v1.NetworkRanges{CIDRBlocks: []string{"10.244.0.0/16", "fd00::/64"}},
	}
	stepper := (&CalicoRunnable{}).InitStep(&component.ExtraMetadata{KubeletDataDir: "/var/lib/kubelet"}, cni, networking)
	r := stepper.(*CalicoRunnable)
	if r.Calico.Mode != "BGP" || r.Calico.MTU != 1500 {
		t.Errorf("explicit fields must be preserved, got: %+v", r.Calico)
	}
	if r.Calico.IPv4AutoDetection != "first-found" || r.Calico.IPv6AutoDetection != "first-found" {
		t.Errorf("empty fields must be defaulted, got: %+v", r.Calico)
	}
	if cni.Calico.IPv4AutoDetection != "" || cni.Calico.Mode != "BGP" {
		t.Errorf("InitStep must not mutate the caller's calico block, got: %+v", cni.Calico)
	}
}
