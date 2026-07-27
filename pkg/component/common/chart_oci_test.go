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

package common

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	containerregistry "github.com/google/go-containerregistry/pkg/registry"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliverypublisher "github.com/kubeclipper/kubeclipper/pkg/delivery/publisher"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "kubeclipper-common-publisher-lock-test-")
	if err != nil {
		panic(err)
	}
	if err = os.Setenv(deliverypublisher.PublishLockDirEnv, dir); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func TestPullHelmOCIChartArchiveAuthenticatedTLSByDigest(t *testing.T) {
	server := httptest.NewTLSServer(chartBasicAuth("robot$kc", "token", containerregistry.New()))
	defer server.Close()
	registry := strings.TrimPrefix(server.URL, "https://") + "/team-a"
	config := &deliveryregistry.Config{
		Registry: registry,
		Scheme:   deliveryregistry.SchemeHTTPS,
		Username: "robot$kc",
		Password: "token",
		CA:       chartServerCertificatePEM(t, server),
	}
	configPath := filepath.Join(t.TempDir(), "package-registry.json")
	if err := deliveryregistry.Write(configPath, config); err != nil {
		t.Fatalf("write Registry config: %v", err)
	}
	t.Setenv(deliveryregistry.ConfigPathEnv, configPath)

	chartPath := filepath.Join(t.TempDir(), "tigera-operator-v3.31.5.tgz")
	writeChartArchive(t, chartPath)
	published, err := deliverypublisher.PublishHelmChart(deliverypublisher.HelmChartPublishRequest{
		ChartPath: chartPath, Registry: registry,
		RepositoryPrefix: deliveryapis.ChartRepositoryPrefix,
		RegistryConfig:   config,
	})
	if err != nil {
		t.Fatalf("PublishHelmChart() error = %v", err)
	}

	destination := filepath.Join(t.TempDir(), "pulled.tgz")
	if err = pullHelmOCIChartArchive(t.Context(), published.Ref, published.Version, published.Digest, destination); err != nil {
		t.Fatalf("pullHelmOCIChartArchive() error = %v", err)
	}
	want, err := os.ReadFile(chartPath)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("pulled Helm chart archive differs from published content")
	}
}

func writeChartArchive(t *testing.T, path string) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	gzw := gzip.NewWriter(file)
	tw := tar.NewWriter(gzw)
	content := "apiVersion: v2\nname: tigera-operator\nversion: v3.31.5\n"
	if err = tw.WriteHeader(&tar.Header{Name: "tigera-operator/Chart.yaml", Mode: 0644, Size: int64(len(content))}); err == nil {
		_, err = tw.Write([]byte(content))
	}
	if closeErr := tw.Close(); err == nil {
		err = closeErr
	}
	if closeErr := gzw.Close(); err == nil {
		err = closeErr
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
}

func chartBasicAuth(username, password string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != username || pass != password {
			w.Header().Set("WWW-Authenticate", `Basic realm="registry"`)
			http.Error(w, "authentication required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func chartServerCertificatePEM(t *testing.T, server *httptest.Server) string {
	t.Helper()
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
}
