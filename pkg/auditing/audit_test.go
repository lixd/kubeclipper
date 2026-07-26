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

package auditing

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/auditing/option"

	"k8s.io/apiserver/pkg/apis/audit"

	"github.com/kubeclipper/kubeclipper/pkg/server/request"
)

func Test_auditing_AddBackend(t *testing.T) {

	type args struct {
		backend Backend
	}
	tests := []struct {
		name string
		args args
		want []Backend
	}{
		{
			name: "addbackend test",
			args: args{
				backend: ConsoleBackend{},
			},
			want: []Backend{
				ConsoleBackend{},
				ConsoleBackend{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &auditing{
				backends: []Backend{
					ConsoleBackend{},
				},
			}
			a.AddBackend(tt.args.backend)
			if !reflect.DeepEqual(a.backends, tt.want) {
				t.Errorf("AddBackend() = %v, want = %v,", a.backends, tt.want)
			}
		})
	}
}

func Test_auditing_Enabled(t *testing.T) {
	type fields struct {
		level    audit.Level
		backends []Backend
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "enabled testing",
			fields: fields{
				level:    audit.LevelMetadata,
				backends: nil,
			},
			want: true,
		},
		{
			name: "",
			fields: fields{
				level:    "other",
				backends: nil,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &auditing{
				backends: tt.fields.backends,
				auditOptions: &option.AuditOptions{
					AuditLevel: tt.fields.level,
				},
			}
			if got := a.Enabled(); got != tt.want {
				t.Errorf("Enabled() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_auditing_LogRequestObject(t *testing.T) {
	type fields struct {
		level    audit.Level
		backends []Backend
	}
	type args struct {
		req  *http.Request
		info *request.Info
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "LogRequestObject test",
			fields: fields{
				level:    audit.LevelRequest,
				backends: nil,
			},
			args: args{
				req: &http.Request{
					Body: io.NopCloser(bytes.NewBufferString("testBody")),
					Header: map[string][]string{
						"Content-Type": {"application/json"},
					},
					URL: &url.URL{
						RawQuery: "testQuery=test",
					},
					ContentLength: 10,
				},
				info: &request.Info{
					IsResourceRequest: false,
					Path:              "testPath",
					Verb:              "create",
					APIGroup:          "testAPIGroup",
					APIVersion:        "testVersion",
					Resource:          "testResource",
					Subresource:       "testSubresource",
					Name:              "testname",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &auditing{
				backends: tt.fields.backends,
				auditOptions: &option.AuditOptions{
					AuditLevel: tt.fields.level,
				},
			}
			_ = a.LogRequestObject(tt.args.req, tt.args.info)
		})
	}
}

func Test_auditing_LogResponseObject(t *testing.T) {
	type fields struct {
		level    audit.Level
		backends []Backend
	}
	type args struct {
		e    *audit.Event
		resp *ResponseCapture
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "LogResponse test",
			fields: fields{
				level:    audit.LevelMetadata,
				backends: make([]Backend, 0),
			},
			args: args{
				e: &audit.Event{},
				resp: &ResponseCapture{
					status: 200,
					body:   bytes.NewBuffer([]byte("")),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &auditing{
				backends: tt.fields.backends,
				auditOptions: &option.AuditOptions{
					AuditLevel: tt.fields.level,
				},
			}
			a.LogResponseObject(tt.args.e, tt.args.resp)
		})
	}
}

func TestAuditingRedactsJSONRequestWithoutMutatingBody(t *testing.T) {
	body := []byte(`{
		"metadata":{"name":"private-registry"},
		"auth":{"username":"robot","password":"registry-password"},
		"ssh":{"privateKey":"ssh-private-key","private_key_password":"key-password"},
		"credentials":[{"access_token":"access-token-value"},{"refresh-token":"refresh-token-value"}],
		"clientKey":"client-key","pkPassword":"pk-password","secret":{"nested":"value"},
		"description":"keep-me"
	}`)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://example.test/api/core/v1/registries", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	event := (&auditing{auditOptions: &option.AuditOptions{AuditLevel: audit.LevelRequest}}).LogRequestObject(req, &request.Info{
		Path:     req.URL.Path,
		Verb:     "create",
		Resource: "registries",
	})
	if event == nil || event.RequestObject == nil {
		t.Fatal("request object was not recorded")
	}
	for _, secret := range []string{"registry-password", "ssh-private-key", "key-password", "access-token-value", "refresh-token-value", "client-key", "pk-password", `"nested":"value"`} {
		if bytes.Contains(event.RequestObject.Raw, []byte(secret)) {
			t.Fatalf("audit request contains secret %q: %s", secret, event.RequestObject.Raw)
		}
	}
	if !bytes.Contains(event.RequestObject.Raw, []byte(`"description":"keep-me"`)) {
		t.Fatalf("non-sensitive field was not preserved: %s", event.RequestObject.Raw)
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, body) {
		t.Fatalf("handler body changed:\n got: %s\nwant: %s", restored, body)
	}
}

func TestAuditingRedactsJSONResponse(t *testing.T) {
	event := &audit.Event{Level: audit.LevelRequestResponse}
	resp := &ResponseCapture{
		status: http.StatusOK,
		body: bytes.NewBufferString(`{
			"items":[{"token":"response-token","auth":{"password":"response-password"}}],
			"message":"visible"
		}`),
	}
	(&auditing{auditOptions: &option.AuditOptions{AuditLevel: audit.LevelRequestResponse}}).LogResponseObject(event, resp)
	if event.ResponseObject == nil {
		t.Fatal("response object was not recorded")
	}
	for _, secret := range []string{"response-token", "response-password"} {
		if bytes.Contains(event.ResponseObject.Raw, []byte(secret)) {
			t.Fatalf("audit response contains secret %q: %s", secret, event.ResponseObject.Raw)
		}
	}
	var value map[string]any
	if err := json.Unmarshal(event.ResponseObject.Raw, &value); err != nil {
		t.Fatalf("redacted response is not valid JSON: %v", err)
	}
	if value["message"] != "visible" {
		t.Fatalf("non-sensitive response field changed: %#v", value)
	}
}

func TestAuditingRedactsDeployConfigMapData(t *testing.T) {
	body := []byte(`{
		"apiVersion":"core.kubeclipper.io/v1",
		"kind":"ConfigMap",
		"metadata":{"name":"deploy-config"},
		"data":{
			"deploy-config":"ssh:\n  user: root\n  privateKey: ssh-server-private-key\n  password: ssh-password\nagents:\n  10.0.0.2:\n    token: agent-token\nregion: keep-me\n",
			"arbitrary-format":"private-key-data-that-is-not-json-or-yaml"
		},
		"immutable":true
	}`)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://example.test/api/core/v1/configmaps", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	event := (&auditing{auditOptions: &option.AuditOptions{AuditLevel: audit.LevelRequest}}).LogRequestObject(req, &request.Info{
		Path:     req.URL.Path,
		Verb:     "create",
		Resource: "configmaps",
	})
	if event == nil || event.RequestObject == nil {
		t.Fatal("request object was not recorded")
	}
	for _, secret := range []string{"ssh-server-private-key", "ssh-password", "agent-token", "private-key-data-that-is-not-json-or-yaml"} {
		if bytes.Contains(event.RequestObject.Raw, []byte(secret)) {
			t.Fatalf("audit request contains deploy config data %q: %s", secret, event.RequestObject.Raw)
		}
	}
	for _, visible := range []string{`"name":"deploy-config"`, `"data":"******"`, `"immutable":true`} {
		if !bytes.Contains(event.RequestObject.Raw, []byte(visible)) {
			t.Fatalf("audit request lost expected data %q: %s", visible, event.RequestObject.Raw)
		}
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, body) {
		t.Fatalf("handler body changed:\n got: %s\nwant: %s", restored, body)
	}

	ordinary := []byte(`{"metadata":{"name":"ordinary"},"data":{"settings":"keep-settings"}}`)
	if got := redactAuditPayload(ordinary, "configmaps"); !bytes.Contains(got, []byte("keep-settings")) {
		t.Fatalf("ordinary ConfigMap data was hidden: %s", got)
	}
}

func TestRedactAuditPayloadPreservesNonJSON(t *testing.T) {
	for _, body := range [][]byte{
		[]byte("plain response token=secret"),
		[]byte(`{"password":"secret"} trailing`),
	} {
		if got := redactAuditPayload(body, ""); !bytes.Equal(got, body) {
			t.Fatalf("non-JSON payload changed: got %q, want %q", got, body)
		}
	}
}

func TestAuditingLoginBehaviorIsPreserved(t *testing.T) {
	body := []byte(`{"username":"admin","password":"login-password"}`)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://example.test/oauth/login", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	event := (&auditing{auditOptions: &option.AuditOptions{AuditLevel: audit.LevelRequest}}).LogRequestObject(req, &request.Info{
		Path: req.URL.Path,
		Verb: "create",
	})
	if event.RequestObject != nil {
		t.Fatalf("login request object must not be recorded: %s", event.RequestObject.Raw)
	}
	if event.User.Username != "admin" {
		t.Fatalf("login username = %q, want admin", event.User.Username)
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(restored, body) {
		t.Fatalf("login handler body changed: got %q, want %q", restored, body)
	}
}
