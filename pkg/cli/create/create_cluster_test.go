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
	"encoding/json"
	"reflect"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

func TestUseDeliveryPolicyResolution(t *testing.T) {
	tests := []struct {
		name          string
		offline       bool
		localRegistry string
		want          bool
	}{
		{name: "offline registry", offline: true, localRegistry: "registry.local:5000", want: true},
		{name: "online registry", offline: false, localRegistry: "registry.local:5000", want: false},
		{name: "offline without registry", offline: true, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := &CreateClusterOptions{Offline: test.offline, LocalRegistry: test.localRegistry}
			if got := opts.useDeliveryPolicyResolution(); got != test.want {
				t.Fatalf("useDeliveryPolicyResolution() = %v, want %v", got, test.want)
			}
		})
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
