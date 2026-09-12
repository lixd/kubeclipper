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

package apis

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func TestResolvedArtifactPlanCodecRoundTrip(t *testing.T) {
	want := &ResolvedArtifactPlan{
		KubernetesVersion: "v1.36.1",
		OS:                DefaultPackageOS,
		Arch:              "amd64",
		Components: []ResolvedComponent{{
			Slot:     "k8s",
			Kind:     "k8s",
			Name:     "k8s",
			Version:  "v1.36.1",
			Required: true,
			Transport: TransportRef{
				Type:   TransportOCI,
				Ref:    "registry.example/kubeclipper/packages/k8s/k8s:v1.36.1",
				Digest: "sha256:package",
			},
			Contents: []ArtifactContent{{Name: ContentConfigs, File: "configs.tar.gz", Digest: "sha256:config"}},
		}},
	}
	raw, err := EncodeResolvedArtifactPlan(want)
	if err != nil {
		t.Fatalf("EncodeResolvedArtifactPlan() error: %v", err)
	}
	got, err := DecodeResolvedArtifactPlan(raw)
	if err != nil {
		t.Fatalf("DecodeResolvedArtifactPlan() error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("decoded plan = %#v, want %#v", got, want)
	}
}

func TestResolvedArtifactPlanCodecNil(t *testing.T) {
	raw, err := EncodeResolvedArtifactPlan(nil)
	if err != nil {
		t.Fatalf("EncodeResolvedArtifactPlan(nil) error: %v", err)
	}
	if raw != nil {
		t.Fatalf("EncodeResolvedArtifactPlan(nil) = %#v, want nil", raw)
	}
	got, err := DecodeResolvedArtifactPlan((*runtime.RawExtension)(nil))
	if err != nil {
		t.Fatalf("DecodeResolvedArtifactPlan(nil) error: %v", err)
	}
	if got != nil {
		t.Fatalf("DecodeResolvedArtifactPlan(nil) = %#v, want nil", got)
	}
}

func TestResolvedArtifactPlanCodecRejectsMalformedData(t *testing.T) {
	if _, err := DecodeResolvedArtifactPlan(&runtime.RawExtension{Raw: []byte("{")}); err == nil {
		t.Fatal("DecodeResolvedArtifactPlan() expected malformed JSON error")
	}
}
