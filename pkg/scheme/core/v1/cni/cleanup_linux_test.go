//go:build linux

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
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/vishvananda/netlink"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestCalicoCleanupDryRunDoesNotTouchNetwork(t *testing.T) {
	// An already canceled context must not reach namespace, link or rule cleanup.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (&CalicoRunnable{}).Uninstall(ctx, component.Options{DryRun: true}); err != nil {
		t.Fatal(err)
	}
}

func TestCalicoCleanupIsolatedLinuxNetwork(t *testing.T) {
	if os.Getenv("KC_TEST_NETWORK_CLEANUP") != "1" {
		t.Skip("requires a disposable Linux network/mount namespace")
	}
	run := func(name string, args ...string) string {
		t.Helper()
		out, err := exec.Command(name, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v: %s", name, args, err, out)
		}
		return string(out)
	}
	run("ip", "netns", "add", "cni-kc-cleanup")
	defer exec.Command("ip", "netns", "del", "cni-kc-cleanup").Run()
	run("ip", "netns", "add", "host-preserve")
	defer exec.Command("ip", "netns", "del", "host-preserve").Run()
	run("ip", "link", "add", "cali-kc-test", "type", "veth", "peer", "name", "kc-peer")
	run("ip", "link", "add", "vxlan.calico", "type", "dummy")
	run("ip", "link", "add", "host-keep", "type", "dummy")
	defer exec.Command("ip", "link", "del", "host-keep").Run()
	run("iptables", "-N", "KUBE-KC-TEST")
	run("iptables", "-N", "cali-KC-TEST")
	run("iptables", "-N", "HOST-KEEP")
	run("iptables", "-A", "INPUT", "-j", "KUBE-KC-TEST")
	run("iptables", "-A", "KUBE-KC-TEST", "-j", "cali-KC-TEST")
	run("iptables", "-A", "HOST-KEEP", "-m", "comment", "--comment", "keep -j KUBE-KC-TEST here", "-j", "ACCEPT")
	run("iptables", "-A", "INPUT", "-j", "HOST-KEEP")
	t.Setenv("KUBECLIPPER_CACHE_DIR", t.TempDir())
	runnable := &CalicoRunnable{BaseCni: BaseCni{CNI: v1.CNI{Type: "calico", Version: "v3.31.5", Calico: &v1.Calico{Mode: CalicoNetworkVXLANAll}}}}
	if _, err := runnable.Uninstall(context.Background(), component.Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := runnable.Uninstall(context.Background(), component.Options{}); err != nil {
		t.Fatalf("cleanup is not idempotent: %v", err)
	}
	for _, name := range []string{"cali-kc-test", "vxlan.calico"} {
		if _, err := netlink.LinkByName(name); err == nil {
			t.Fatalf("left link %s", name)
		}
	}
	if _, err := netlink.LinkByName("host-keep"); err != nil {
		t.Fatal("removed host link", err)
	}
	if _, err := os.Stat(filepath.Join("/var/run/netns", "cni-kc-cleanup")); !os.IsNotExist(err) {
		t.Fatal("CNI namespace remains", err)
	}
	if _, err := os.Stat(filepath.Join("/var/run/netns", "host-preserve")); err != nil {
		t.Fatal("removed host namespace", err)
	}
	rules := run("iptables-save")
	if strings.Contains(rules, ":KUBE-KC-TEST") || strings.Contains(rules, ":cali-KC-TEST") {
		t.Fatal("cluster rules remain", rules)
	}
	if !strings.Contains(rules, ":HOST-KEEP") || !strings.Contains(rules, "keep -j KUBE-KC-TEST here") {
		t.Fatal("removed host rules", rules)
	}
}
