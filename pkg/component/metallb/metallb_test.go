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

package metallb

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	"github.com/stretchr/testify/assert"
)

func TestCheckMetalLBPodStatusWaitsForControllerAvailability(t *testing.T) {
	tempDir := t.TempDir()
	capturePath := filepath.Join(tempDir, "kubectl-args")
	kubectlPath := filepath.Join(tempDir, "kubectl")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$KUBECTL_ARGS_FILE\"\nexit \"${KUBECTL_EXIT_CODE:-0}\"\n"
	if err := os.WriteFile(kubectlPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tempDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("KUBECTL_ARGS_FILE", capturePath)

	n := &MetalLB{}
	if err := n.checkMetalLBPodStatus(context.Background(), component.Options{}); err != nil {
		t.Fatalf("expected kubectl wait to succeed, got: %v", err)
	}

	args, err := os.ReadFile(capturePath)
	if err != nil {
		t.Fatal(err)
	}
	expected := "wait\n--for=condition=Available\ndeployment/controller\n-n\nmetallb-system\n--timeout=5s\n"
	if got := string(args); got != expected {
		t.Fatalf("unexpected kubectl arguments:\n got: %q\nwant: %q", got, expected)
	}

	t.Setenv("KUBECTL_EXIT_CODE", "1")
	if err := n.checkMetalLBPodStatus(context.Background(), component.Options{}); err == nil {
		t.Fatal("expected kubectl wait failure to be returned")
	}
}

var lb = &MetalLB{
	ImageRepoMirror: "192.168.10.10:5000",
	ManifestsDir:    "/tmp/.metallb",
	Mode:            "BGP",
	Addresses:       []string{"192.168.20.20-192.168.20.30"},
	Version:         "v0.13.7",
}

func TestRenderTo(t *testing.T) {
	sb := &strings.Builder{}
	if err := lb.renderTo(sb); err != nil {
		assert.FailNow(t, "deploy template render failed, err: %v", err)
	}
	// BGP mode should be deployed FRR
	if !strings.Contains(sb.String(), "frr-startup") {
		t.Error("BGP mode should be deployed FRR")
	}

	nlb := *lb
	nlb.Mode = "L2"
	sb1 := &strings.Builder{}
	if err := nlb.renderTo(sb1); err != nil {
		assert.FailNow(t, "deploy template render failed, err: %v", err)
	}
	// L2 mode should not be deployed FRR
	if strings.Contains(sb1.String(), "frr-startup") {
		t.Error("L2 mode should not be deployed FRR")
	}
}

func TestBGPPeerASNSchemaUsesInt64(t *testing.T) {
	sb := &strings.Builder{}
	if err := lb.renderTo(sb); err != nil {
		assert.FailNow(t, "deploy template render failed, err: %v", err)
	}

	if got := strings.Count(sb.String(), "format: int64\n                maximum: 4294967295"); got != 4 {
		t.Fatalf("expected all four BGPPeer ASN fields across v1beta1 and v1beta2 to use int64, got %d", got)
	}
}

func TestRenderIPAddressPool(t *testing.T) {
	expected := `
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: first-pool
  namespace: metallb-system
spec:
  addresses:
  - 192.168.20.20-192.168.20.30
`
	sb := &strings.Builder{}
	if err := lb.renderIPAddressPool(sb); err != nil {
		assert.FailNow(t, "ip address pool template render failed, err: %v", err)
	}
	if !assert.Equal(t, expected, sb.String()) {
		t.Errorf("expected is not the same as actual")
	}
}

func TestRenderAdvertisement(t *testing.T) {
	expected := `
apiVersion: metallb.io/v1beta1
kind: BGPAdvertisement
metadata:
  name: local
  namespace: metallb-system
`
	sb := &strings.Builder{}
	if err := lb.renderAdvertisement(sb); err != nil {
		assert.FailNow(t, "advertisement template render failed, err: %v", err)
	}
	if !assert.Equal(t, expected, sb.String()) {
		t.Errorf("expected is not the same as actual")
	}
}
