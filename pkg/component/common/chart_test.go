/*
 *
 *  * Copyright 2024 KubeClipper Authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package common

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	containerregistry "github.com/google/go-containerregistry/pkg/registry"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/delivery/indexer"
	"github.com/kubeclipper/kubeclipper/pkg/delivery/publisher"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
)

func TestChartPackagePublishDiscoverAndAcquire(t *testing.T) {
	t.Setenv(downloader.CacheDirEnv, t.TempDir())
	server := httptest.NewServer(containerregistry.New())
	defer server.Close()
	config := &deliveryregistry.Config{Registry: strings.TrimPrefix(server.URL, "http://"), Scheme: deliveryregistry.SchemeHTTP}
	configPath := filepath.Join(t.TempDir(), "registry.json")
	if err := deliveryregistry.Write(configPath, config); err != nil {
		t.Fatal(err)
	}
	t.Setenv(deliveryregistry.ConfigPathEnv, configPath)
	chart := chartPackageArchive(t, "tigera-operator/Chart.yaml", []byte("apiVersion: v2\nname: tigera-operator\nversion: v3.31.5\n"))
	pkg := chartPackageArchive(t, "calico/v3.31.5/"+runtime.GOARCH+"/charts.tgz", chart)
	packagePath := filepath.Join(t.TempDir(), "calico.tar.gz")
	if err := os.WriteFile(packagePath, pkg, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := publisher.NewOCIArtifactPublisher().Publish(publisher.PublishRequest{
		PackagePath: packagePath, Kind: "cni", Name: "calico", Version: "v3.31.5",
		Arch: runtime.GOARCH, Registry: config.Registry, ContentProfile: deliveryapis.ContentProfileAddon,
		SourceRevision: "chart-package-test", RegistryConfig: config,
	})
	if err != nil {
		t.Fatal(err)
	}
	policy := deliveryapis.NewSupportPolicy("chart-package")
	policy.Spec.Policies = []deliveryapis.KubernetesSupportPolicy{{ComponentSlots: []deliveryapis.ComponentSlotRule{
		{Options: []deliveryapis.ComponentOption{{Kind: "cni", Name: "calico"}}},
	}}}
	inventory, err := indexer.NewRegistryPackageInventoryIndexerWithConfig(config).IndexRepositories(
		t.Context(), config.Registry, deliveryapis.SupportPolicyRepositories(policy))
	if err != nil {
		t.Fatal(err)
	}
	if len(inventory.Spec.Packages) != 1 {
		t.Fatalf("expected one discovered chart package, got %+v", inventory.Spec.Packages)
	}
	entry := inventory.Spec.Packages[0]
	if len(entry.Contents) != 1 || entry.Contents[0].Transport.Type != "" {
		t.Fatalf("expected chart inside the package, got %+v", entry.Contents)
	}
	step := &Chart{Kind: entry.Kind, PkgName: entry.Name, Version: entry.Version, Arch: runtime.GOARCH,
		Transport: entry.Transport, Contents: entry.Contents}
	chartPath, release, err := step.acquireChart(t.Context(), component.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	data, err := os.ReadFile(chartPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, chart) {
		t.Fatal("package extraction changed the Helm chart archive")
	}
}

func chartPackageArchive(t *testing.T, name string, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0644, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
