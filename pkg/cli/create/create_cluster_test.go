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

package create

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/scheme"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

func TestUseDeliveryPolicyResolution(t *testing.T) {
	tests := []struct {
		name    string
		offline bool
		want    bool
	}{
		{name: "offline", offline: true, want: true},
		{name: "online", offline: false, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := &CreateClusterOptions{Offline: test.offline}
			if got := opts.useDeliveryPolicyResolution(); got != test.want {
				t.Fatalf("useDeliveryPolicyResolution() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCreateClusterRegistryFlags(t *testing.T) {
	out := &bytes.Buffer{}
	streams := options.IOStreams{In: strings.NewReader(""), Out: out, ErrOut: out}
	cmd := NewCmdCreateCluster(streams)
	cmd.SetOut(out)
	cmd.SetErr(out)
	cmd.SetArgs([]string{"--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	help := out.String()
	if !strings.Contains(help, "--image-registry string") {
		t.Fatalf("image registry flag missing from help:\n%s", help)
	}
	if strings.Contains(help, "--local-registry") || strings.Contains(help, "--insecure-registry") {
		t.Fatalf("removed registry flags remain in help:\n%s", help)
	}
}

func TestNewClusterLeavesOnlineImageRegistryEmpty(t *testing.T) {
	opts := NewCreateClusterOptions(options.IOStreams{})
	opts.Name = "demo"
	opts.Masters = []string{"node-1"}
	cluster := opts.newCluster()
	if cluster.ImageRegistry != "" {
		t.Fatalf("imageRegistry = %q, want empty", cluster.ImageRegistry)
	}
}

func TestRuleComponentVersionsUsesSelectedKubernetesRule(t *testing.T) {
	metas := decodedComponentMeta(t)

	got := ruleComponentVersions(metas, "v1.36.1", "cri", "containerd", "")
	want := []string{"2.2.4"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ruleComponentVersions() = %v, want %v", got, want)
	}

	got = ruleComponentVersions(metas, "v1.35.0", "cni", "calico", "v3.2")
	want = []string{"v3.29.6"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ruleComponentVersions() with prefix = %v, want %v", got, want)
	}
}

func TestRuleComponentOptionsPreservesPolicyDefault(t *testing.T) {
	options := ruleComponentOptions(decodedComponentMeta(t), "v1.36.1", "cni")
	if len(options) != 1 {
		t.Fatalf("ruleComponentOptions() length = %d, want 1", len(options))
	}
	if options[0].name != "calico" || options[0].version != "v3.31.5" || !options[0].isDefault {
		t.Fatalf("ruleComponentOptions() = %+v", options[0])
	}
}

func TestResolvableKubernetesVersionsOnlyUsesCompleteRules(t *testing.T) {
	metas := decodedComponentMeta(t)
	metas.Addons = append(metas.Addons,
		scheme.MetaResource{Type: "k8s", Name: "k8s", Version: "v1.34.2", Arch: "amd64"},
		scheme.MetaResource{Type: "k8s", Name: "k8s", Version: "v1.35.0", Arch: "amd64"},
	)

	got := resolvableKubernetesVersions(metas, "v1.3")
	want := []string{"v1.35.0", "v1.36.1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("resolvableKubernetesVersions() = %v, want %v", got, want)
	}
}

func TestArchitecturesForSelectedNodes(t *testing.T) {
	nodes := []v1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "node-amd64", Labels: map[string]string{common.LabelArchStable: "amd64"}},
			Status:     v1.NodeStatus{Ipv4DefaultIP: "10.0.0.10"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "node-arm64"},
			Status: v1.NodeStatus{
				NodeIpv4DefaultIP: "10.0.0.20",
				NodeInfo:          v1.NodeSystemInfo{Arch: "arm64"},
			},
		},
	}

	got, complete := architecturesForSelectedNodes(nodes, []string{"10.0.0.10", "node-arm64"})
	if !complete || !reflect.DeepEqual(got, []string{"amd64", "arm64"}) {
		t.Fatalf("architecturesForSelectedNodes() = %v, %v", got, complete)
	}
	if got, complete = architecturesForSelectedNodes(nodes, []string{"missing"}); complete || got != nil {
		t.Fatalf("missing node result = %v, %v", got, complete)
	}
}

func TestNewestVersionUsesSemanticVersionOrder(t *testing.T) {
	if got := newestVersion([]string{"v1.9.9", "v1.10.0", "v1.8.12"}); got != "v1.10.0" {
		t.Fatalf("newestVersion() = %q, want v1.10.0", got)
	}
	if got := newestVersion([]string{"release-a", "release-c", "release-b"}); got != "release-c" {
		t.Fatalf("newestVersion() fallback = %q, want release-c", got)
	}
}

func decodedComponentMeta(t *testing.T) *kc.ComponentMeta {
	t.Helper()
	raw := []byte(`{
		"rules": [
			{
				"name": "k8s",
				"version": "v1.35.0",
				"version_control": {
					"cri": [{"name":"containerd","version":"1.7.29","type":"cri","default":true}],
					"cni": [{"name":"calico","version":"v3.29.6","type":"cni","default":true}]
				}
			},
			{
				"name": "k8s",
				"version": "v1.36.1",
				"version_control": {
					"cri": [{"name":"containerd","version":"2.2.4","type":"cri","default":true}],
					"cni": [{"name":"calico","version":"v3.31.5","type":"cni","default":true}]
				}
			}
		],
		"addons": []
	}`)
	metas := &kc.ComponentMeta{}
	if err := json.Unmarshal(raw, metas); err != nil {
		t.Fatalf("decode component meta: %v", err)
	}
	return metas
}
