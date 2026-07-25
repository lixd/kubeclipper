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

package filters

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/emicklei/go-restful"
	"k8s.io/apiserver/pkg/authentication/user"

	"github.com/kubeclipper/kubeclipper/pkg/client/clientrest"
	"github.com/kubeclipper/kubeclipper/pkg/server/request"
)

func TestProtectInformerQuery(t *testing.T) {
	tests := []struct {
		name     string
		username string
		wantRaw  bool
	}{
		{name: "regular user", username: "admin", wantRaw: false},
		{name: "internal informer", username: clientrest.InternalInformerUser, wantRaw: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			httpReq := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/registries", http.NoBody)
			httpReq.Header.Set(clientrest.QueryTypeHeader, clientrest.InformerQuery)
			httpReq = httpReq.WithContext(request.WithUser(httpReq.Context(), &user.DefaultInfo{Name: tt.username}))
			req := restful.NewRequest(httpReq)
			resp := restful.NewResponse(httptest.NewRecorder())
			called := false
			chain := &restful.FilterChain{Target: func(_ *restful.Request, _ *restful.Response) { called = true }}

			ProtectInformerQuery(req, resp, chain)

			if !called {
				t.Fatal("filter did not continue the chain")
			}
			if got := clientrest.IsInformerRawQuery(req.Request); got != tt.wantRaw {
				t.Fatalf("raw query = %v, want %v", got, tt.wantRaw)
			}
		})
	}
}
