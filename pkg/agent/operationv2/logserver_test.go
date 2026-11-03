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

package operationv2

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/kubeclipper/kubeclipper/pkg/oplog"
)

type staticLogs struct{}

func (staticLogs) GetStepLogContent(_, _ string, _ int64, _ int) (content []byte, delivered, size int64, err error) {
	content = []byte("task log")
	return content, int64(len(content)), int64(len(content)), nil
}

type missingLogs struct{}

func (missingLogs) GetStepLogContent(string, string, int64, int) (content []byte, delivered, size int64, err error) {
	return nil, 0, 0, os.ErrNotExist
}

func verifiedRequest(target, commonName string) *http.Request {
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, target, http.NoBody)
	certificate := &x509.Certificate{Subject: pkix.Name{CommonName: commonName}}
	request.TLS = &tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{certificate},
		VerifiedChains:   [][]*x509.Certificate{{certificate}},
	}
	return request
}

func TestLogHandlerRequiresServerIdentity(t *testing.T) {
	handler := &LogHandler{Logs: staticLogs{}}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/v1/tasks/task-uid/logs", http.NoBody))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}

	response = httptest.NewRecorder()
	handler.ServeHTTP(response, verifiedRequest("/v1/tasks/task-uid/logs", "another-client"))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestLogHandlerReadsByTaskUID(t *testing.T) {
	handler := &LogHandler{Logs: staticLogs{}}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, verifiedRequest("/v1/tasks/task-uid/logs?offset=0&limit=128", DefaultServerClientIdentity))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var body oplog.LogContentResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Content != "task log" {
		t.Fatalf("content = %q", body.Content)
	}
}

func TestLogHandlerReturnsEmptyResponseForTaskWithoutLog(t *testing.T) {
	handler := &LogHandler{Logs: missingLogs{}}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, verifiedRequest("/v1/tasks/task-uid/logs", DefaultServerClientIdentity))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var body oplog.LogContentResponse
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Content != "" || body.DeliverySize != 0 || body.LogSize != 0 {
		t.Fatalf("empty log response = %#v", body)
	}
}

func TestLogHandlerPreservesUTF8AcrossPages(t *testing.T) {
	logs, err := oplog.NewOperationLog(&oplog.Options{Dir: t.TempDir(), OplogThreshold: oplog.MaximumThreshold})
	if err != nil {
		t.Fatal(err)
	}
	want := strings.Repeat("ASCII 中文日志 😀\n", 20)
	if err := logs.CreateStepLogFileAndAppend("task-uid", "task", []byte(want)); err != nil {
		t.Fatal(err)
	}
	handler := &LogHandler{Logs: logs}
	for _, limit := range []int{1, 2, 3, 4, 64, maxLogReadSize} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			var got strings.Builder
			for offset := int64(0); offset < int64(len(want)); {
				response := httptest.NewRecorder()
				target := fmt.Sprintf("/v1/tasks/task-uid/logs?offset=%d&limit=%d", offset, limit)
				handler.ServeHTTP(response, verifiedRequest(target, DefaultServerClientIdentity))
				if response.Code != http.StatusOK {
					t.Fatalf("status = %d", response.Code)
				}
				var body oplog.LogContentResponse
				if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if !utf8.ValidString(body.Content) || body.DeliverySize != int64(len(body.Content)) || body.DeliverySize == 0 || body.DeliverySize > int64(max(limit, utf8.UTFMax)) {
					t.Fatalf("invalid page at offset %d: %#v", offset, body)
				}
				got.WriteString(body.Content)
				offset += body.DeliverySize
			}
			if got.String() != want {
				t.Fatal("paginated log differs from original UTF-8 log")
			}
		})
	}
}

func TestLogHandlerWaitsForIncompleteUTF8Write(t *testing.T) {
	logs, err := oplog.NewOperationLog(&oplog.Options{Dir: t.TempDir(), OplogThreshold: oplog.MaximumThreshold})
	if err != nil {
		t.Fatal(err)
	}
	if err := logs.CreateStepLogFileAndAppend("task-uid", "task", []byte("start \xe4\xb8")); err != nil {
		t.Fatal(err)
	}
	handler := &LogHandler{Logs: logs}
	read := func(offset int64) oplog.LogContentResponse {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, verifiedRequest(fmt.Sprintf("/v1/tasks/task-uid/logs?offset=%d", offset), DefaultServerClientIdentity))
		var body oplog.LogContentResponse
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return body
	}
	first := read(0)
	if first.Content != "start " || first.DeliverySize != 6 || first.LogSize != 8 {
		t.Fatalf("incomplete write response = %#v", first)
	}
	if err := logs.CreateStepLogFileAndAppend("task-uid", "task", []byte("\xad\n")); err != nil {
		t.Fatal(err)
	}
	second := read(first.DeliverySize)
	if second.Content != "中\n" || second.DeliverySize != 4 || second.LogSize != 10 {
		t.Fatalf("completed write response = %#v", second)
	}
}

func TestLogHandlerRejectsTraversalAndOversizedReads(t *testing.T) {
	handler := &LogHandler{Logs: staticLogs{}}
	for _, target := range []string{
		"/v1/tasks/../logs",
		"/v1/tasks/task-uid/logs?offset=-1",
		"/v1/tasks/task-uid/logs?limit=1048577",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, verifiedRequest(target, DefaultServerClientIdentity))
		if response.Code == http.StatusOK {
			t.Fatalf("unsafe request %q was accepted", target)
		}
	}
}
