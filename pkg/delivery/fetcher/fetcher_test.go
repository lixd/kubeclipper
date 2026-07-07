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

package fetcher

import (
	"context"
	"strings"
	"testing"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

func TestNewForTransport(t *testing.T) {
	for _, tt := range []struct {
		name          string
		transportType string
		wantType      interface{}
		wantErr       bool
	}{
		{name: "oci", transportType: deliveryapis.TransportOCI, wantType: &OCIArtifactFetcher{}},
		{name: "unknown", transportType: "unknown", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fetcher, err := NewForTransport(tt.transportType, true)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("NewForTransport() expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewForTransport() error: %+v", err)
			}
			switch tt.wantType.(type) {
			case *OCIArtifactFetcher:
				if _, ok := fetcher.(*OCIArtifactFetcher); !ok {
					t.Fatalf("fetcher type = %T", fetcher)
				}
			}
		})
	}
}

func TestFetchComponentUsesResolvedComponentArch(t *testing.T) {
	result, err := FetchComponent(context.Background(), "amd64", deliveryapis.ResolvedComponent{
		Kind:    "cni",
		Name:    "calico",
		Version: "v3.31.5",
		Arch:    "arm64",
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    "registry.local:5000/kubeclipper/packages/cni/calico:v3.31.5",
			Digest: "sha256:4444444444444444444444444444444444444444444444444444444444444444",
		},
		Contents: []deliveryapis.ArtifactContent{
			{Name: deliveryapis.ContentCharts, File: "charts.tgz", MediaType: deliveryapis.MediaTypeChartsLayer},
		},
	}, true)
	if err != nil {
		t.Fatalf("FetchComponent() error: %+v", err)
	}
	if result.Arch != "arm64" {
		t.Fatalf("result arch = %q, want arm64", result.Arch)
	}
	if got := result.Files[deliveryapis.ContentCharts]; got != "/tmp/kc-downloader/packages/cni/calico/v3.31.5/linux-arm64/contents/charts.tgz" {
		t.Fatalf("chart path = %q", got)
	}
}

func TestValidatePlanRejectsInvalidComponents(t *testing.T) {
	validComponent := deliveryapis.ResolvedComponent{
		Kind:    "cni",
		Name:    "calico",
		Version: "v3.31.5",
		Arch:    "amd64",
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    "registry.local:5000/kubeclipper/packages/cni/calico:v3.31.5",
			Digest: "sha256:4444444444444444444444444444444444444444444444444444444444444444",
		},
		Contents: []deliveryapis.ArtifactContent{{Name: deliveryapis.ContentCharts, File: "charts.tgz"}},
	}
	for _, tt := range []struct {
		name       string
		components []deliveryapis.ResolvedComponent
		want       string
	}{
		{
			name: "empty components",
			want: "components are required",
		},
		{
			name:       "duplicate component",
			components: []deliveryapis.ResolvedComponent{validComponent, validComponent},
			want:       "duplicate component cni/calico:v3.31.5",
		},
		{
			name: "arch mismatch",
			components: []deliveryapis.ResolvedComponent{{
				Kind:      validComponent.Kind,
				Name:      validComponent.Name,
				Version:   validComponent.Version,
				Arch:      "arm64",
				Transport: validComponent.Transport,
			}},
			want: "arch \"arm64\" does not match plan arch \"amd64\"",
		},
		{
			name: "non oci transport",
			components: []deliveryapis.ResolvedComponent{{
				Kind:    validComponent.Kind,
				Name:    validComponent.Name,
				Version: validComponent.Version,
				Arch:    validComponent.Arch,
				Transport: deliveryapis.TransportRef{
					Type: "http",
					Ref:  "http://127.0.0.1/calico",
				},
			}},
			want: "transport \"http\" is not supported",
		},
		{
			name: "empty contents",
			components: []deliveryapis.ResolvedComponent{{
				Kind:      validComponent.Kind,
				Name:      validComponent.Name,
				Version:   validComponent.Version,
				Arch:      validComponent.Arch,
				Transport: validComponent.Transport,
			}},
			want: "contents are required",
		},
		{
			name: "empty content name",
			components: []deliveryapis.ResolvedComponent{{
				Kind:      validComponent.Kind,
				Name:      validComponent.Name,
				Version:   validComponent.Version,
				Arch:      validComponent.Arch,
				Transport: validComponent.Transport,
				Contents:  []deliveryapis.ArtifactContent{{File: "charts.tgz"}},
			}},
			want: "content[0] name is required",
		},
		{
			name: "duplicate content",
			components: []deliveryapis.ResolvedComponent{{
				Kind:      validComponent.Kind,
				Name:      validComponent.Name,
				Version:   validComponent.Version,
				Arch:      validComponent.Arch,
				Transport: validComponent.Transport,
				Contents: []deliveryapis.ArtifactContent{
					{Name: deliveryapis.ContentCharts, File: "charts.tgz"},
					{Name: deliveryapis.ContentCharts, File: "charts-copy.tgz"},
				},
			}},
			want: "duplicate content \"charts\"",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePlan(&deliveryapis.ResolvedArtifactPlan{
				KubernetesVersion: "v1.36.0",
				OS:                deliveryapis.DefaultPackageOS,
				Arch:              "amd64",
				Components:        tt.components,
			})
			if err == nil {
				t.Fatalf("validatePlan() error = nil, want %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validatePlan() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}
