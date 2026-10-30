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

package fetcher

import (
	"context"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
)

func TestBlobTransportAuthenticationTLSAndRedirectIsolation(t *testing.T) {
	for _, mode := range []string{"http", "custom-ca", "skip-verify", "untrusted", "wrong-password", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			var redirectedAuth atomic.Bool
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "" {
					redirectedAuth.Store(true)
				}
				_, _ = w.Write([]byte("blob"))
			}))
			defer target.Close()
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				user, password, ok := r.BasicAuth()
				if !ok || user != "user" || password != "secret" {
					w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
					w.WriteHeader(401)
					return
				}
				if r.URL.Path == "/v2/" {
					w.WriteHeader(200)
					return
				}
				if mode == "redirect" {
					http.Redirect(w, r, target.URL+"/blob?signed=private-value", 307)
					return
				}
				if r.URL.Path != "/v2/prefix/package/blobs/sha256:test" {
					t.Errorf("unexpected blob path %s", r.URL.Path)
				}
				_, _ = w.Write([]byte("blob"))
			})
			var server *httptest.Server
			secure := mode == "custom-ca" || mode == "skip-verify" || mode == "untrusted"
			if secure {
				server = httptest.NewTLSServer(handler)
			} else {
				server = httptest.NewServer(handler)
			}
			defer server.Close()
			scheme := "http"
			if secure {
				scheme = "https"
			}
			host := strings.TrimPrefix(server.URL, scheme+"://")
			cfg := &deliveryregistry.Config{Registry: host + "/prefix", Scheme: scheme, Username: "user", Password: "secret"}
			if mode == "custom-ca" {
				cfg.CA = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}))
			}
			if mode == "skip-verify" {
				cfg.SkipTLSVerify = true
			}
			if mode == "wrong-password" {
				cfg.Password = "wrong"
			}
			fetch := NewOCIArtifactFetcherWithConfig(false, cfg)
			client, endpoint, err := fetch.blobClient(context.Background(), host+"/prefix/package@sha256:"+strings.Repeat("a", 64))
			if mode == "untrusted" {
				if err == nil {
					t.Fatal("invalid transport accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			req, _ := http.NewRequestWithContext(context.Background(), "GET", endpoint+"sha256:test", nil)
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(safeNetworkError(err))
			}
			defer resp.Body.Close()
			if mode == "wrong-password" {
				if resp.StatusCode != 401 {
					t.Fatal("wrong credentials accepted")
				}
				return
			}
			data, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != 200 || string(data) != "blob" {
				t.Fatal(resp.StatusCode, err, string(data))
			}
			if mode == "redirect" && redirectedAuth.Load() {
				t.Fatal("registry credentials leaked to redirect host")
			}
		})
	}
}
