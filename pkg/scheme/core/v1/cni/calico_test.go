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
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	componentcommon "github.com/kubeclipper/kubeclipper/pkg/component/common"
	"github.com/kubeclipper/kubeclipper/pkg/constatns"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
)

func TestCalicoUninstallStepsCleanNetwork(t *testing.T) {
	runnable := &CalicoRunnable{BaseCni: BaseCni{CNI: v1.CNI{Calico: &v1.Calico{Mode: CalicoNetworkVXLANAll}}}}
	nodes := []v1.StepNode{{ID: "worker-1"}}
	steps, err := runnable.UninstallSteps(nodes)
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 1 || steps[0].Name != "cleanCalicoNetwork" || steps[0].Action != v1.ActionUninstall {
		t.Fatalf("unexpected uninstall steps: %+v", steps)
	}
	if len(steps[0].Commands) != 1 || steps[0].Commands[0].Type != v1.CommandCustom {
		t.Fatalf("unexpected cleanup command: %+v", steps[0].Commands)
	}
}

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

func TestCalicoInstallStepsWithContextAppliesResolvedChart(t *testing.T) {
	runnable := &CalicoRunnable{
		BaseCni: BaseCni{
			CNI: v1.CNI{
				Version: "v3.31.5",
				Offline: true,
			},
		},
	}
	plan := &deliveryapis.ResolvedArtifactPlan{Components: []deliveryapis.ResolvedComponent{
		{
			Kind:    "cni",
			Name:    "calico",
			Version: "v3.31.5",
			Transport: deliveryapis.TransportRef{
				Type:   deliveryapis.TransportOCI,
				Ref:    "registry.local:5000/kubeclipper/packages/cni/calico:v3.31.5",
				Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			},
			Contents: []deliveryapis.ArtifactContent{{Name: deliveryapis.ContentCharts, File: "charts.tgz"}},
		},
	}}
	ctx := component.WithResolvedArtifactPlan(context.Background(), plan)

	steps, err := runnable.InstallStepsWithContext(ctx, []v1.StepNode{{ID: "master-1"}}, "v1.32.0")
	if err != nil {
		t.Fatalf("InstallStepsWithContext() error: %+v", err)
	}

	var chart *componentcommon.Chart
	for _, step := range steps {
		if step.Name != "calico-chartLoad" || len(step.Commands) == 0 {
			continue
		}
		var decoded componentcommon.Chart
		if err := json.Unmarshal(step.Commands[0].CustomCommand, &decoded); err != nil {
			t.Fatalf("unmarshal chart custom command: %+v", err)
		}
		chart = &decoded
		break
	}
	if chart == nil {
		t.Fatalf("calico-chartLoad step not found in %+v", steps)
	}
	if chart.Transport.Ref != "registry.local:5000/kubeclipper/packages/cni/calico:v3.31.5" {
		t.Fatalf("chart transport = %+v", chart.Transport)
	}
	if chart.Kind != "cni" {
		t.Fatalf("chart kind = %q, want cni", chart.Kind)
	}
	if len(chart.Contents) != 1 || chart.Contents[0].Name != deliveryapis.ContentCharts {
		t.Fatalf("chart contents = %+v", chart.Contents)
	}
	var installRelease *v1.Step
	for i := range steps {
		if steps[i].Name == "installCalicoRelease" {
			installRelease = &steps[i]
			break
		}
	}
	if installRelease == nil || len(installRelease.Commands) == 0 {
		t.Fatalf("installCalicoRelease step not found in %+v", steps)
	}
	gotCommand := strings.Join(installRelease.Commands[0].ShellCommand, " ")
	wantChartPath := downloader.ChartPath("cni", "calico", "v3.31.5", "linux-"+runtime.GOARCH)
	if !strings.Contains(gotCommand, wantChartPath) {
		t.Fatalf("installCalicoRelease command = %q", gotCommand)
	}
}

func TestCalicoPrepareImagesSkipsTarballLoadWhenRegistryConfigured(t *testing.T) {
	runnable := &CalicoRunnable{
		BaseCni: BaseCni{
			CNI: v1.CNI{
				Type:    "calico",
				Version: "v3.24.5",
				Offline: true,
			},
			ResolvedImageRegistry: "registry.local:5000",
		},
	}

	steps, err := runnable.PrepareImages(context.Background(), []v1.StepNode{{ID: "master-1"}})
	if err != nil {
		t.Fatalf("PrepareImages() error: %+v", err)
	}
	if len(steps) != 0 {
		t.Fatalf("steps = %+v", steps)
	}
}

func TestCalicoPrepareImagesRequiresImageRegistry(t *testing.T) {
	runnable := &CalicoRunnable{
		BaseCni: BaseCni{
			CNI: v1.CNI{
				Type:    "calico",
				Version: "v3.24.5",
				Offline: true,
			},
		},
	}
	if _, err := runnable.PrepareImages(context.Background(), []v1.StepNode{{ID: "master-1"}}); err == nil || !strings.Contains(err.Error(), "imageRegistry") {
		t.Fatalf("PrepareImages() error = %+v, want imageRegistry error", err)
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
