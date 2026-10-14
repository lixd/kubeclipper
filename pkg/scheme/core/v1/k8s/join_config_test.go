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

package k8s

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
)

func TestJoinCmdUsesExistingClusterConfiguration(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TMPDIR", dir)
	t.Setenv("KC_JOIN_TEST_TRACE", filepath.Join(dir, "config-path"))
	writeExecutable := func(name, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"+body), 0700); err != nil {
			t.Fatal(err)
		}
	}
	writeExecutable("kubectl", `
if [ "$KC_JOIN_TEST_FAIL_CONFIG" = 1 ]; then exit 6; fi
if [ "$KC_JOIN_TEST_EMPTY_CONFIG" = 1 ]; then exit 0; fi
printf 'apiVersion: kubeadm.k8s.io/v1beta4\nkind: ClusterConfiguration\nkubernetesVersion: v1.37.0\n'
`)
	writeExecutable("kubeadm", `
if [ "$1" = token ]; then
  printf 'kubeadm join 127.0.0.1:6443 --token abcdef.0123456789abcdef --discovery-token-ca-cert-hash sha256:1111111111111111111111111111111111111111111111111111111111111111\n'
  exit 0
fi
config=''
while [ "$#" -gt 0 ]; do
  if [ "$1" = --config ]; then shift; config="$1"; fi
  shift
done
[ -n "$config" ] || exit 14
case "$(cat "$config")" in
  *'kubernetesVersion: v1.37.0'*) ;;
  *) exit 15 ;;
esac
printf '%s' "$config" > "$KC_JOIN_TEST_TRACE"
printf '[upload-certs] Using certificate key:\n1111111111111111111111111111111111111111111111111111111111111111\n'
`)
	step := JoinCmd{ContainerRuntime: "containerd", ControlPlane: true}
	output, err := step.Install(context.Background(), component.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(output), "--control-plane --certificate-key") {
		t.Fatal("control-plane join command lacks the uploaded certificate key")
	}
	configPath, err := os.ReadFile(filepath.Join(dir, "config-path"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(string(configPath)); !os.IsNotExist(err) {
		t.Fatalf("temporary kubeadm configuration was not removed: %v", err)
	}
	if err := os.Remove(filepath.Join(dir, "config-path")); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KC_JOIN_TEST_FAIL_CONFIG", "1")
	if _, err := step.Install(context.Background(), component.Options{}); err == nil {
		t.Fatal("missing cluster configuration must fail without defaulting to a public version")
	}
	if _, err := os.Stat(filepath.Join(dir, "config-path")); !os.IsNotExist(err) {
		t.Fatal("certificate upload ran after cluster configuration lookup failed")
	}
	t.Setenv("KC_JOIN_TEST_FAIL_CONFIG", "0")
	t.Setenv("KC_JOIN_TEST_EMPTY_CONFIG", "1")
	if _, err := step.Install(context.Background(), component.Options{}); err == nil {
		t.Fatal("empty cluster configuration must fail without querying a public version")
	}
}
