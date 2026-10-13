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

package node

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/gorilla/websocket"
	"github.com/onsi/ginkgo"

	v1 "github.com/kubeclipper/kubeclipper/pkg/apis/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/query"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
	"github.com/kubeclipper/kubeclipper/pkg/utils/certs"
	"github.com/kubeclipper/kubeclipper/test/framework"
)

var _ = SIGDescribe("[Serial]", func() {
	f := framework.NewDefaultFramework("node")
	var nodeID, nodeIP string

	ginkgo.BeforeEach(func() {
		ginkgo.By("Check that there are enough available nodes")
		nodes, err := f.Client.ListNodes(context.TODO(), kc.Queries{
			Pagination:    query.NoPagination(),
			LabelSelector: fmt.Sprintf("!%s", common.LabelNodeDisable),
		})
		framework.ExpectNoError(err)
		nodeID = nodes.Items[0].Name
		nodeIP = nodes.Items[0].Status.Ipv4DefaultIP
	})

	ginkgo.It("[Fast] [new-node] [Enable] [Disable] should disable and enable node of kubeclipper platform", func() {
		ginkgo.By("disable node")
		err := f.Client.DisableNode(context.TODO(), nodeID)
		framework.ExpectNoError(err)

		ginkgo.By("check node is disabled")
		nodeList, err := f.Client.DescribeNode(context.TODO(), nodeID)
		framework.ExpectNoError(err)
		if _, ok := nodeList.Items[0].Labels[common.LabelNodeDisable]; !ok {
			framework.Failf("Fail to disable node")
		} else {
			ginkgo.By("node is disabled")
		}

		ginkgo.By("enable node")
		err = f.Client.EnableNode(context.TODO(), nodeID)
		framework.ExpectNoError(err)

		ginkgo.By("check node is enabled")
		nodeList, err = f.Client.DescribeNode(context.TODO(), nodeID)
		framework.ExpectNoError(err)
		if _, ok := nodeList.Items[0].Labels[common.LabelNodeDisable]; !ok {
			ginkgo.By("node is enabled")
		} else {
			framework.Failf("Fail to enabled node")
		}
	})
	ginkgo.It("[Fast] [new-node] [Terminal] should connect node ssh", func() {
		ginkgo.By("Get public key")
		pub, err := f.Client.GetPublicKey(context.TODO())
		framework.ExpectNoError(err)

		ginkgo.By("Get msg")
		pubkey, _ := base64.StdEncoding.DecodeString(pub.PublicKey)
		username := os.Getenv("KUBECLIPPER_E2E_SSH_USERNAME")
		if username == "" {
			username = "root"
		}
		password := os.Getenv("KUBECLIPPER_E2E_SSH_PASSWORD")
		if password == "" {
			password = "Thinkbig1"
		}
		u, err := certs.RsaEncrypt([]byte(username), pubkey)
		framework.ExpectNoError(err)
		p, err := certs.RsaEncrypt([]byte(password), pubkey)
		framework.ExpectNoError(err)
		credential := &v1.SSHCredential{
			Username: u,
			Password: p,
			Port:     22,
		}
		jsonBody, err := json.Marshal(credential)
		msg := base64.StdEncoding.EncodeToString(jsonBody)
		framework.ExpectNoError(err)

		ginkgo.By("connect node")
		query := url.Values{}
		query.Set("token", f.Client.Token())
		query.Set("msg", msg)
		scheme := "ws"
		dialer := *websocket.DefaultDialer
		if f.Client.Scheme() == "https" {
			scheme = "wss"
			dialer.TLSClientConfig = f.Client.HTTPClient().Transport.(*http.Transport).TLSClientConfig.Clone()
			dialer.TLSClientConfig.NextProtos = []string{"http/1.1"}
		}
		endpoint := fmt.Sprintf("%s://%s%s/%s/terminal?%s", scheme, f.Client.Host(), kc.ListNodesPath, nodeID, query.Encode())
		ws, _, err := dialer.Dial(endpoint, nil)
		framework.ExpectNoError(err)

		ginkgo.By("check the node is connected")
		for {
			_, data, err := ws.ReadMessage()
			if err != nil {
				framework.Failf("Fail to connect node: %v", err)
			}
			if string(data) == "" {
				framework.Failf("Fail to connect node")
			} else {
				break
			}
		}

		ginkgo.By("close the connection")
		err = ws.Close()
		framework.ExpectNoError(err)
	})
	ginkgo.It("[Fast] [new-node] [Detail] [Info] should get info and detail of node", func() {
		ginkgo.By("show node info")
		nodeList, err := f.Client.ListNodes(context.TODO(), kc.Queries{
			Pagination: query.NoPagination(),
			FuzzySearch: map[string]string{
				"default-ip": nodeIP,
			},
		})
		framework.ExpectNoError(err)
		if len(nodeList.Items) == 0 {
			framework.Failf("show node info e2e test failed, no such node")
		}

		ginkgo.By("show node detail")
		nodeList, err = f.Client.DescribeNode(context.TODO(), nodeID)
		framework.ExpectNoError(err)
		if len(nodeList.Items) == 0 {
			framework.Failf("show node detail e2e test failed, no such node")
		}
	})
})
