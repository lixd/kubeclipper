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

package cni

import (
	"strings"
	"testing"
)

func TestClusterRuleDeletionPreservesHostRules(t *testing.T) {
	saved := `*filter
:INPUT ACCEPT [0:0]
:HOST-FIREWALL - [0:0]
:KUBE-SERVICES - [0:0]
:cali-FORWARD - [0:0]
-A INPUT -j HOST-FIREWALL
-A INPUT -m comment --comment "keep -j KUBE-SERVICES here" -j ACCEPT
-A INPUT -m comment --comment "owned rule" -j KUBE-SERVICES
-A HOST-FIREWALL -g cali-FORWARD
-A KUBE-SERVICES -j cali-FORWARD
-A cali-FORWARD -j ACCEPT
COMMIT
*nat
:POSTROUTING ACCEPT [0:0]
:HOST-NAT - [0:0]
-A POSTROUTING -j HOST-NAT
COMMIT
`
	got := clusterRuleDeletion(saved)
	for _, want := range []string{"-D INPUT -m comment --comment \"owned rule\" -j KUBE-SERVICES", "-D HOST-FIREWALL -g cali-FORWARD", "-F KUBE-SERVICES", "-F cali-FORWARD", "-X KUBE-SERVICES", "-X cali-FORWARD"} {
		if !strings.Contains(got, want) {
			t.Fatalf("missing deletion %q in %s", want, got)
		}
	}
	for _, keep := range []string{"keep -j", "-D INPUT -j HOST-FIREWALL", "-F INPUT", "-X HOST-FIREWALL", "*nat"} {
		if strings.Contains(got, keep) {
			t.Fatalf("host rule affected: %s", got)
		}
	}
	if clusterRuleDeletion("*filter\n:INPUT ACCEPT [0:0]\nCOMMIT\n") != "" {
		t.Fatal("empty cleanup produced changes")
	}
}
