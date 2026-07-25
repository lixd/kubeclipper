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
	"github.com/emicklei/go-restful"

	"github.com/kubeclipper/kubeclipper/pkg/client/clientrest"
	"github.com/kubeclipper/kubeclipper/pkg/server/request"
)

// ProtectInformerQuery prevents authenticated API users from selecting the
// unredacted view reserved for the server's internal informer client.
func ProtectInformerQuery(req *restful.Request, response *restful.Response, chain *restful.FilterChain) {
	if clientrest.IsInformerRawQuery(req.Request) {
		userInfo, ok := request.UserFrom(req.Request.Context())
		if !ok || userInfo.GetName() != clientrest.InternalInformerUser {
			req.Request.Header.Del(clientrest.QueryTypeHeader)
		}
	}
	chain.ProcessFilter(req, response)
}
