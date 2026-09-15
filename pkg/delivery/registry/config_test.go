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

package registry

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/crane"
	containerregistry "github.com/google/go-containerregistry/pkg/registry"
)

func TestCraneOptionsAuthenticatedTLSWithCustomCA(t *testing.T) {
	server := httptest.NewTLSServer(basicAuth("robot$kc", "token", containerregistry.New()))
	defer server.Close()

	config := &Config{
		Registry: strings.TrimPrefix(server.URL, "https://"),
		Scheme:   SchemeHTTPS,
		Username: "robot$kc",
		Password: "token",
		CA:       serverCertificatePEM(t, server),
	}
	opts, err := config.CraneOptions(t.Context())
	if err != nil {
		t.Fatalf("CraneOptions() error = %v", err)
	}
	if _, err = crane.Catalog(config.Registry, opts...); err != nil {
		t.Fatalf("Catalog() with custom CA and credentials error = %v", err)
	}
}

func TestCraneOptionsRejectsWrongCredentials(t *testing.T) {
	server := httptest.NewTLSServer(basicAuth("robot$kc", "token", containerregistry.New()))
	defer server.Close()
	config := &Config{
		Registry: strings.TrimPrefix(server.URL, "https://"),
		Scheme:   SchemeHTTPS,
		Username: "robot$kc",
		Password: "wrong",
		CA:       serverCertificatePEM(t, server),
	}
	opts, err := config.CraneOptions(t.Context())
	if err != nil {
		t.Fatalf("CraneOptions() error = %v", err)
	}
	if _, err = crane.Catalog(config.Registry, opts...); err == nil {
		t.Fatal("Catalog() with wrong credentials error = nil, want authentication failure")
	}
}

func TestCraneOptionsStrictTLSAndExplicitHTTP(t *testing.T) {
	tlsServer := httptest.NewTLSServer(containerregistry.New())
	defer tlsServer.Close()
	tlsConfig := &Config{Registry: strings.TrimPrefix(tlsServer.URL, "https://"), Scheme: SchemeHTTPS}
	tlsOpts, err := tlsConfig.CraneOptions(t.Context())
	if err != nil {
		t.Fatalf("CraneOptions() error = %v", err)
	}
	if _, err = crane.Catalog(tlsConfig.Registry, tlsOpts...); err == nil {
		t.Fatal("Catalog() without custom CA error = nil, want strict TLS verification failure")
	}

	httpServer := httptest.NewServer(containerregistry.New())
	defer httpServer.Close()
	httpConfig := &Config{Registry: strings.TrimPrefix(httpServer.URL, "http://"), Scheme: SchemeHTTP}
	httpOpts, err := httpConfig.CraneOptions(t.Context())
	if err != nil {
		t.Fatalf("CraneOptions() HTTP error = %v", err)
	}
	if _, err = crane.Catalog(httpConfig.Registry, httpOpts...); err != nil {
		t.Fatalf("Catalog() with explicit HTTP error = %v", err)
	}
}

func TestConfigWriteLoadAndResolveReference(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "package-registry.json")
	config := &Config{Registry: "harbor.example.com/kubeclipper", Scheme: SchemeHTTPS, Username: "robot", Password: "secret"}
	if err := Write(path, config); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("config mode = %o, want 0600", got)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if loaded.Password != "secret" {
		t.Fatalf("Load() password = %q, want secret", loaded.Password)
	}
	t.Setenv(ConfigPathEnv, path)
	resolved, err := ResolveReference("harbor.example.com/kubeclipper/packages/k8s/k8s@sha256:deadbeef")
	if err != nil {
		t.Fatalf("ResolveReference() error = %v", err)
	}
	if resolved.Registry != config.Registry || resolved.Password != config.Password {
		t.Fatalf("ResolveReference() = %#v, want registry credentials", resolved)
	}
}

func TestConfigRejectsUnsafeAndIncompleteOptions(t *testing.T) {
	if err := (&Config{Registry: "registry.example.com", Username: "user"}).Validate(); err == nil {
		t.Fatal("Validate() incomplete credentials error = nil")
	}
	if err := (&Config{Registry: "registry.example.com", Scheme: SchemeHTTP, SkipTLSVerify: true}).Validate(); err == nil {
		t.Fatal("Validate() HTTP skip TLS error = nil")
	}
}

func TestConfigRejectsDifferentRegistryAndReference(t *testing.T) {
	config := &Config{Registry: "harbor.example.com/team-a", Scheme: SchemeHTTPS, Username: "robot", Password: "token"}
	if err := config.ValidateRegistry("harbor.example.com/team-b"); err == nil {
		t.Fatal("ValidateRegistry() mismatch error = nil")
	}
	if err := config.ValidateReference("harbor.example.com/team-b/kubeclipper/packages/cri/containerd:2.2.4"); err == nil {
		t.Fatal("ValidateReference() mismatch error = nil")
	}
	if err := config.ValidateReference("harbor.example.com/team-a/kubeclipper/packages/cri/containerd:2.2.4"); err != nil {
		t.Fatalf("ValidateReference() matching reference error = %v", err)
	}
}

func basicAuth(username, password string, next http.Handler) http.Handler {
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

func serverCertificatePEM(t *testing.T, server *httptest.Server) string {
	t.Helper()
	certificate := server.Certificate()
	if certificate == nil {
		t.Fatal("test TLS server certificate is nil")
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}))
}
