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

package common

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

func TestApplyResolvedComponentToImager(t *testing.T) {
	imager := &Imager{PkgName: "calico", Version: "v3.30.0"}
	component := resolvedTestComponent("cni", "calico", "v3.30.0", deliveryapis.ContentImages)
	if err := ApplyResolvedComponent(imager, component); err != nil {
		t.Fatalf("ApplyResolvedComponent() error: %+v", err)
	}
	if imager.Transport.Ref != component.Transport.Ref {
		t.Fatalf("transport ref = %q, want %q", imager.Transport.Ref, component.Transport.Ref)
	}
	if imager.Kind != component.Kind {
		t.Fatalf("kind = %q, want %q", imager.Kind, component.Kind)
	}
	if imager.PkgName != component.Name {
		t.Fatalf("pkgName = %q, want %q", imager.PkgName, component.Name)
	}
	if imager.Arch != component.Arch {
		t.Fatalf("arch = %q, want %q", imager.Arch, component.Arch)
	}
	if len(imager.Contents) != 1 || imager.Contents[0].Name != deliveryapis.ContentImages {
		t.Fatalf("contents = %+v", imager.Contents)
	}
}

func TestApplyResolvedComponentToChart(t *testing.T) {
	chart := &Chart{PkgName: "calico", Version: "v3.30.0"}
	component := resolvedTestComponent("cni", "calico", "v3.30.0", deliveryapis.ContentCharts)
	if err := ApplyResolvedComponent(chart, component); err != nil {
		t.Fatalf("ApplyResolvedComponent() error: %+v", err)
	}
	if chart.Transport.Ref != component.Transport.Ref {
		t.Fatalf("transport ref = %q, want %q", chart.Transport.Ref, component.Transport.Ref)
	}
	if chart.Kind != component.Kind {
		t.Fatalf("kind = %q, want %q", chart.Kind, component.Kind)
	}
	if chart.PkgName != component.Name {
		t.Fatalf("pkgName = %q, want %q", chart.PkgName, component.Name)
	}
	if chart.Arch != component.Arch {
		t.Fatalf("arch = %q, want %q", chart.Arch, component.Arch)
	}
	if len(chart.Contents) != 1 || chart.Contents[0].Name != deliveryapis.ContentCharts {
		t.Fatalf("contents = %+v", chart.Contents)
	}
}

func TestFindResolvedComponent(t *testing.T) {
	plan := &deliveryapis.ResolvedArtifactPlan{Components: []deliveryapis.ResolvedComponent{
		resolvedTestComponent("cni", "calico", "v3.30.0", deliveryapis.ContentImages),
	}}
	component, ok := FindResolvedComponent(plan, "cni", "calico", "v3.30.0")
	if !ok {
		t.Fatalf("FindResolvedComponent() not found")
	}
	if component.Name != "calico" {
		t.Fatalf("component = %+v", component)
	}
	if _, ok = FindResolvedComponent(plan, "cni", "calico", "v3.29.1"); ok {
		t.Fatalf("FindResolvedComponent() found unexpected component")
	}
}

func TestImagerLegacyJSONCompatibility(t *testing.T) {
	var imager Imager
	if err := json.Unmarshal([]byte(`{"pkgName":"calico","version":"v3.30.0","offline":true}`), &imager); err != nil {
		t.Fatalf("Unmarshal() error: %+v", err)
	}
	if imager.Transport.Type != "" {
		t.Fatalf("legacy json unexpectedly set transport: %+v", imager.Transport)
	}
}

func TestImagerInstallRequiresResolvedTransport(t *testing.T) {
	imager := &Imager{PkgName: "metallb", Version: "v0.13.7", Offline: true}
	if _, err := imager.Install(context.Background(), component.Options{DryRun: true}); err == nil || !strings.Contains(err.Error(), "image from tarball has been removed") {
		t.Fatalf("Install() error = %+v, want removed tarball error", err)
	}
}

func TestImagerInstallRejectsUnsupportedResolvedTransport(t *testing.T) {
	imager := &Imager{
		PkgName: "metallb",
		Version: "v0.13.7",
		Offline: true,
		Transport: deliveryapis.TransportRef{
			Type: "http",
			Ref:  "unsupported://metallb/v0.13.7/amd64",
		},
	}
	if _, err := imager.Install(context.Background(), component.Options{DryRun: true}); err == nil || !strings.Contains(err.Error(), "image from tarball has been removed") {
		t.Fatalf("Install() error = %+v, want removed tarball error", err)
	}
}

func TestImagerInstallRequiresResolvedContents(t *testing.T) {
	imager := &Imager{
		Kind:    "lb",
		PkgName: "metallb",
		Version: "v0.13.7",
		Offline: true,
		CriName: "containerd",
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    "registry.local:5000/kubeclipper/packages/lb/metallb:v0.13.7",
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
	}
	if _, err := imager.Install(context.Background(), component.Options{DryRun: true}); err == nil || !strings.Contains(err.Error(), "image from tarball has been removed") {
		t.Fatalf("Install() error = %+v, want removed tarball error", err)
	}
}

func TestChartInstallRequiresResolvedTransport(t *testing.T) {
	chart := &Chart{PkgName: "calico", Version: "v3.31.5", Offline: true}
	if _, err := chart.Install(context.Background(), component.Options{DryRun: true}); err == nil {
		t.Fatalf("Install() expected missing resolved transport error")
	}
}

func TestChartInstallRejectsUnsupportedResolvedTransport(t *testing.T) {
	chart := &Chart{
		PkgName: "calico",
		Version: "v3.31.5",
		Offline: true,
		Transport: deliveryapis.TransportRef{
			Type: "http",
			Ref:  "unsupported://calico/v3.31.5/amd64",
		},
	}
	if _, err := chart.Install(context.Background(), component.Options{DryRun: true}); err == nil {
		t.Fatalf("Install() expected unsupported resolved transport error")
	}
}

func TestChartInstallRequiresResolvedContents(t *testing.T) {
	chart := &Chart{
		Kind:    "cni",
		PkgName: "calico",
		Version: "v3.31.5",
		Offline: true,
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    "registry.local:5000/kubeclipper/packages/cni/calico:v3.31.5",
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
	}
	if _, err := chart.Install(context.Background(), component.Options{DryRun: true}); err == nil || !strings.Contains(err.Error(), "resolved chart contents are required") {
		t.Fatalf("Install() error = %+v, want resolved chart contents error", err)
	}
}

func TestImagerInstallRequiresResolvedArtifactSource(t *testing.T) {
	imager := &Imager{PkgName: "metallb", Version: "v0.13.7", Offline: true, CriName: "containerd"}
	if _, err := imager.Install(context.Background(), component.Options{DryRun: true}); err == nil || !strings.Contains(err.Error(), "image from tarball has been removed") {
		t.Fatalf("Install() error = %+v, want removed tarball error", err)
	}
}

func TestChartInstallRequiresResolvedArtifactSource(t *testing.T) {
	chart := &Chart{PkgName: "calico", Version: "v3.31.5", Offline: true}
	if _, err := chart.Install(context.Background(), component.Options{DryRun: true}); err == nil {
		t.Fatalf("Install() expected missing delivery source error")
	}
}

func TestImagerArtifactKindDefaultsToLegacyImageKind(t *testing.T) {
	imager := &Imager{PkgName: "metallb", Version: "v0.13.7"}
	if got := imager.ArtifactKind(); got != imageName {
		t.Fatalf("ArtifactKind() = %q, want %q", got, imageName)
	}
	if got := imager.ArtifactPlatform(); got != archOrRuntime("") {
		t.Fatalf("ArtifactPlatform() = %q, want runtime arch", got)
	}
}

func TestChartArtifactKindDefaultsToLegacyChartKind(t *testing.T) {
	chart := &Chart{PkgName: "calico", Version: "v3.31.5"}
	if got := chart.ArtifactKind(); got != chartName {
		t.Fatalf("ArtifactKind() = %q, want %q", got, chartName)
	}
	if got := chart.ArtifactPlatform(); got != archOrRuntime("") {
		t.Fatalf("ArtifactPlatform() = %q, want runtime arch", got)
	}
}

func TestResolvedArtifactPlatformUsesPackageOSAndArch(t *testing.T) {
	chart := &Chart{Kind: "cni", PkgName: "calico", Version: "v3.31.5", Arch: "amd64"}
	if got := chart.ArtifactPlatform(); got != "linux-amd64" {
		t.Fatalf("chart ArtifactPlatform() = %q, want linux-amd64", got)
	}
	imager := &Imager{Kind: "lb", PkgName: "metallb", Version: "v0.13.7", Arch: "arm64"}
	if got := imager.ArtifactPlatform(); got != "linux-arm64" {
		t.Fatalf("imager ArtifactPlatform() = %q, want linux-arm64", got)
	}
}

func TestResolvePulledChartArchiveUsesNewArchiveWhenNameIsNotPredictable(t *testing.T) {
	dir := t.TempDir()
	oldArchive := filepath.Join(dir, "existing-v1.0.0.tgz")
	if err := os.WriteFile(oldArchive, []byte("old"), 0644); err != nil {
		t.Fatalf("write old archive: %+v", err)
	}
	before, err := chartArchives(dir)
	if err != nil {
		t.Fatalf("chartArchives() error: %+v", err)
	}
	pulledArchive := filepath.Join(dir, "actual-chart-name-v3.31.5.tgz")
	if err = os.WriteFile(pulledArchive, []byte("new"), 0644); err != nil {
		t.Fatalf("write pulled archive: %+v", err)
	}

	got, err := resolvePulledChartArchive(dir, "registry.local/kubeclipper/charts/tigera-operator", "v3.31.5", "charts.tgz", before)
	if err != nil {
		t.Fatalf("resolvePulledChartArchive() error: %+v", err)
	}
	if got != pulledArchive {
		t.Fatalf("resolvePulledChartArchive() = %q, want %q", got, pulledArchive)
	}
}

func resolvedTestComponent(kind, name, version, contentName string) deliveryapis.ResolvedComponent {
	return deliveryapis.ResolvedComponent{
		Kind:    kind,
		Name:    name,
		Version: version,
		Arch:    "arm64",
		Transport: deliveryapis.TransportRef{
			Type:   deliveryapis.TransportOCI,
			Ref:    "registry.local:5000/kubeclipper/packages/" + kind + "/" + name + ":" + version,
			Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
		},
		Contents: []deliveryapis.ArtifactContent{{Name: contentName, File: contentName + ".tar.gz"}},
	}
}
