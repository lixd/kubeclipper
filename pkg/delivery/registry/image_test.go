/*
 *
 *  * Copyright 2026 KubeClipper Authors.
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

package registry

import (
	"os/exec"
	"strings"
	"testing"
)

func TestRuntimeImageRepositoriesMatchPackaging(t *testing.T) {
	for _, tc := range []struct{ source, want string }{
		{"registry.k8s.io/kube-apiserver:v1.37.0", "kube-apiserver"},
		{"registry.k8s.io/coredns/coredns:v1.14.6", "coredns"},
		{"registry.k8s.io/pause:3.10.2", "pause"},
		{"docker.io/calico/node:v3.31.5", "calico-node"},
		{"quay.io/tigera/operator:v1.40.8", "tigera-operator"},
		{"docker.io/caas4/nfsplugin:v4.1.0", "nfs-plugin"},
		{"docker.io/caas4/csi-attacher:v3.2.1", "nfs-csi-attacher"},
		{"docker.io/caas4/frr:v7.5.1", "metallb-frr"},
		{"fanux/lvscare:v1.1.1", "lvscare"},
		{"kubeclipper/kubectl:latest", "kubectl"},
		{"nicolaka/netshoot:v0.13", "netshoot"},
		{"docker.io/library/busybox:1.36", "library-busybox"},
	} {
		t.Run(tc.source, func(t *testing.T) {
			mirror := "registry.cn-hangzhou.aliyuncs.com/kubeclipper-release"
			got, err := RuntimeImageRepository(tc.source, mirror)
			if err != nil || got != mirror+"/"+tc.want {
				t.Fatalf("RuntimeImageRepository() = %q, %v", got, err)
			}
			python, err := exec.LookPath("python3")
			if err != nil {
				t.Skip("python3 unavailable for packaging contract check")
			}
			out, err := exec.Command(python, "../../../scripts/open-packaging/image_repository.py", tc.source, mirror).CombinedOutput()
			if err != nil {
				t.Fatalf("packaging mapper: %v: %s", err, out)
			}
			suffix := tc.source[strings.LastIndex(tc.source, ":"):]
			if strings.TrimSpace(string(out)) != got+suffix {
				t.Fatalf("packaging maps %q differently: %s != %s", tc.source, out, got+suffix)
			}
		})
	}
}
