/*
 *
 *  * Copyright 2021 KubeClipper Authors.
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

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"sigs.k8s.io/yaml"
)

// These tests exercise real command parsing and HTTP clients against controlled
// protocol responses. They do not establish live platform acceptance.
func releaseCommand(t *testing.T, handler http.HandlerFunc, args ...string) (*cobra.Command, *bytes.Buffer, *bytes.Buffer) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	cfg := filepath.Join(t.TempDir(), "config")
	data := fmt.Sprintf(`{"servers":{"test":{"server":%q}},"users":{"test":{"token":"test-only"}},"contexts":{"test":{"server":"test","user":"test"}},"current-context":"test"}`, server.URL)
	if err := os.WriteFile(cfg, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	cmd := NewKubeClipperCommand(strings.NewReader(""), out, errOut)
	cmd.SetOut(out)
	cmd.SetErr(errOut)
	cmd.SetArgs(append(args, "--config", cfg))
	return cmd, out, errOut
}

func decodeDocument(t *testing.T, format string, data []byte) map[string]any {
	t.Helper()
	if format == "yaml" {
		var err error
		data, err = yaml.YAMLToJSON(data)
		if err != nil {
			t.Fatal(err)
		}
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("invalid %s output %q: %v", format, data, err)
	}
	return result
}

func TestReleaseCLIListShapes(t *testing.T) {
	for _, resource := range []string{"cluster", "node", "user", "role", "configmap", "registry", "operation"} {
		for _, format := range []string{"json", "yaml"} {
			for _, count := range []int{0, 1, 2} {
				t.Run(fmt.Sprintf("%s/%s/%d", resource, format, count), func(t *testing.T) {
					items := make([]any, count)
					for i := range items {
						items[i] = map[string]any{"metadata": map[string]any{"name": fmt.Sprintf("item-%d", i)}}
					}
					args := []string{"get", resource, "-o", format}
					if resource == "operation" {
						args = []string{"operation", "list", "-o", format}
					}
					cmd, out, _ := releaseCommand(t, func(w http.ResponseWriter, r *http.Request) {
						// Include nil Items as well as populated lists.
						if count == 0 {
							items = nil
						}
						_ = json.NewEncoder(w).Encode(map[string]any{"items": items, "totalCount": count})
					}, args...)
					if err := cmd.Execute(); err != nil {
						t.Fatal(err)
					}
					result := decodeDocument(t, format, out.Bytes())
					list, ok := result["items"].([]any)
					if !ok || len(list) != count {
						t.Fatalf("unstable list output: %s", out.String())
					}
				})
			}
		}
	}
}

func TestReleaseCLIDetailStaysSingleObject(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		cmd, out, _ := releaseCommand(t, func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, `{"metadata":{"name":"alpha"}}`)
		}, "get", "cluster", "alpha", "-o", format)
		if err := cmd.Execute(); err != nil {
			t.Fatal(err)
		}
		result := decodeDocument(t, format, out.Bytes())
		if result["items"] != nil || result["metadata"].(map[string]any)["name"] != "alpha" {
			t.Fatalf("detail is not a single object: %s", out.String())
		}
	}
}

func TestReleaseCLIRejectsInvalidArgumentsBeforeConfig(t *testing.T) {
	for _, args := range [][]string{
		{"get", "cluster", "alpha", "beta"},
		{"get", "cluster", "-o", "jsno"},
		{"get", "cluster/alpha"},
		{"delete", "cluster", "alpha", "beta"},
		{"delete", "user", "alpha", "--force"},
		{"operation", "list", "extra"},
		{"operation", "list", "-o", "jsno"},
		{"operation", "logs"},
		{"operation", "logs", "op", "--cluster", "alpha"},
		{"operation", "logs", "op", "--max-length", "-1"},
		{"create", "cluster", "--name", "alpha", "--master", "node", "-o", "jsno"},
		{"create", "user", "extra"},
		{"create", "registry", "extra"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			cmd := NewKubeClipperCommand(strings.NewReader(""), io.Discard, io.Discard)
			cmd.SetArgs(append(args, "--config", filepath.Join(t.TempDir(), "missing")))
			err := cmd.Execute()
			if err == nil || strings.Contains(err.Error(), "auth config") {
				t.Fatalf("expected local usage error, got %v", err)
			}
		})
	}
}

func TestReleaseCLILogsPropagatesFailures(t *testing.T) {
	for _, failure := range []string{"operation", "tasks", "logs"} {
		t.Run(failure, func(t *testing.T) {
			cmd, out, _ := releaseCommand(t, func(w http.ResponseWriter, r *http.Request) {
				path := r.URL.Path
				isTasks := strings.HasSuffix(path, "/operationtasks")
				isLogs := strings.HasSuffix(path, "/logs")
				if failure == "operation" || failure == "tasks" && isTasks || failure == "logs" && isLogs {
					w.WriteHeader(http.StatusServiceUnavailable)
					fmt.Fprint(w, `{"code":503,"reason":"ServiceUnavailable","message":"injected unavailable"}`)
					return
				}
				if isTasks {
					fmt.Fprint(w, `{"items":[{"metadata":{"name":"task"},"spec":{"stepID":"step","nodeRef":{"name":"node"}}}]}`)
				} else {
					fmt.Fprint(w, `{"metadata":{"name":"op","uid":"op-uid"},"spec":{"steps":[{"id":"step"}]},"status":{"phase":"Succeeded"}}`)
				}
			}, "operation", "logs", "op")
			err := cmd.Execute()
			if err == nil || !strings.Contains(err.Error(), "injected unavailable") {
				t.Fatalf("request failure lost: %v", err)
			}
			if strings.Contains(out.String(), "unavailable") {
				t.Fatalf("diagnostic leaked to stdout: %s", out.String())
			}
		})
	}
}

func TestReleaseCLIDeletionReportsSubmission(t *testing.T) {
	for _, force := range []bool{false, true} {
		args := []string{"delete", "cluster", "alpha"}
		if force {
			args = append(args, "--force", "-y")
		}
		cmd, out, _ := releaseCommand(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodDelete || (r.URL.Query().Get("force") == "true") != force {
				t.Errorf("unexpected delete request: %s %s", r.Method, r.URL)
			}
		}, args...)
		if err := cmd.Execute(); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), " deleted") || !strings.Contains(out.String(), "requested") {
			t.Fatalf("misleading deletion result: %s", out.String())
		}
		if force && !strings.Contains(out.String(), "manually") {
			t.Fatalf("missing host-cleanup warning: %s", out.String())
		}
	}
}

func TestReleaseCLINamedWatchStructuredOutput(t *testing.T) {
	for _, format := range []string{"json", "yaml"} {
		cmd, out, _ := releaseCommand(t, func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("watch") != "true" {
				fmt.Fprint(w, `{"metadata":{"name":"alpha"}}`)
				return
			}
			if r.URL.Query().Get("fieldSelector") != "metadata.name=alpha" {
				t.Errorf("watch is not scoped to the requested name: %s", r.URL)
			}
			fmt.Fprint(w, `{"type":"MODIFIED","object":{"metadata":{"name":"beta"}}}{"type":"MODIFIED","object":{"metadata":{"name":"alpha"},"status":{"phase":"Running"},"extra":"preserved"}}{"type":"ERROR","object":{"message":"watch denied","code":403}}`)
		}, "get", "cluster", "alpha", "--watch", "-o", format)
		err := cmd.Execute()
		if err == nil || !strings.Contains(err.Error(), "watch denied") {
			t.Fatalf("server watch error swallowed: %v", err)
		}
		var documents []map[string]any
		if format == "json" {
			decoder := json.NewDecoder(out)
			for {
				var item map[string]any
				err := decoder.Decode(&item)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				documents = append(documents, item)
			}
		} else {
			for _, document := range strings.Split(out.String(), "---\n") {
				documents = append(documents, decodeDocument(t, format, []byte(document)))
			}
		}
		if len(documents) != 2 {
			t.Fatalf("unexpected watch output: %#v", documents)
		}
		if documents[1]["type"] != "MODIFIED" || documents[1]["object"].(map[string]any)["extra"] != "preserved" {
			t.Fatalf("watch lost event fields: %#v", documents)
		}
	}
}

func TestReleaseCLIWatchCancellation(t *testing.T) {
	started := make(chan struct{})
	cmd, _, _ := releaseCommand(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") != "true" {
			fmt.Fprint(w, `{"items":[]}`)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
	}, "get", "cluster", "--watch")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not stop on cancellation")
	}
}

func TestReleaseCLIWatchRejectsMalformedFrames(t *testing.T) {
	cmd, _, _ := releaseCommand(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("watch") == "true" {
			fmt.Fprint(w, `{"type":`)
			return
		}
		fmt.Fprint(w, `{"items":[]}`)
	}, "get", "cluster", "--watch")
	if err := cmd.Execute(); err == nil {
		t.Fatal("malformed frame must fail rather than reconnect indefinitely")
	}
}
