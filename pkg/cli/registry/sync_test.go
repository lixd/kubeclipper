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

package registry

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
)

const syncTestManifest = `apiVersion: delivery.kubeclipper.io/v1alpha1
kind: ReleaseManifest
metadata:
  name: kubeclipper-resources
  version: v2.0.0
  sourceRevision: 72589807386d3a8919e26e68782a02d717d17f4c
registries:
  package: ghcr.io/kubeclipper/kubeclipper
  image: ghcr.io/kubeclipper/kubeclipper
artifacts:
- type: package-image
  component: {kind: bootstrap, name: kubeclipper, version: v2.0.0}
  source: ghcr.io/kubeclipper/kubeclipper/kubeclipper/packages/bootstrap/kubeclipper:v2.0.0
  target: kubeclipper/packages/bootstrap/kubeclipper:v2.0.0
  sourceRevision: 72589807386d3a8919e26e68782a02d717d17f4c
  digest: sha256:ca1f23cd9cfc48c0d594431d1b570148cd6a0178985c7050f1fb7cd980a946da
  platforms: ["linux/amd64"]
- type: package-image
  component: {kind: bootstrap, name: etcd, version: 3.5.21}
  source: ghcr.io/kubeclipper/kubeclipper/kubeclipper/packages/bootstrap/etcd:3.5.21
  target: kubeclipper/packages/bootstrap/etcd:3.5.21
  sourceRevision: 72589807386d3a8919e26e68782a02d717d17f4c
  digest: sha256:ca1f23cd9cfc48c0d594431d1b570148cd6a0178985c7050f1fb7cd980a946db
  platforms: ["linux/amd64"]
- type: package-image
  component: {kind: bootstrap, name: console, version: v1.6.0}
  source: ghcr.io/kubeclipper/kubeclipper/kubeclipper/packages/bootstrap/console:v1.6.0
  target: kubeclipper/packages/bootstrap/console:v1.6.0
  sourceRevision: 72589807386d3a8919e26e68782a02d717d17f4c
  digest: sha256:ca1f23cd9cfc48c0d594431d1b570148cd6a0178985c7050f1fb7cd980a946dc
  platforms: ["linux/amd64"]
- type: package-image
  component: {kind: bootstrap, name: registry, version: 3.1.1}
  source: ghcr.io/kubeclipper/kubeclipper/kubeclipper/packages/bootstrap/registry:3.1.1
  target: kubeclipper/packages/bootstrap/registry:3.1.1
  sourceRevision: 72589807386d3a8919e26e68782a02d717d17f4c
  digest: sha256:ca1f23cd9cfc48c0d594431d1b570148cd6a0178985c7050f1fb7cd980a946dd
  platforms: ["linux/amd64"]
- type: package-image
  component: {kind: k8s, name: k8s, version: v1.37.0}
  source: ghcr.io/kubeclipper/kubeclipper/kubeclipper/packages/k8s/k8s:v1.37.0
  target: kubeclipper/packages/k8s/k8s:v1.37.0
  sourceRevision: 72589807386d3a8919e26e68782a02d717d17f4c
  digest: sha256:e2ab0e717011d1a2906cec37da949756b368904e4c30a49cf2e2f5704d466468
  platforms: ["linux/amd64"]
`

func TestSyncValidateArgs(t *testing.T) {
	cases := []struct {
		name string
		o    SyncOptions
		want string
	}{
		{"no manifest", SyncOptions{Target: "harbor.local/kc"}, "--manifest is required"},
		{"no target", SyncOptions{ManifestPath: "x"}, "--target is required"},
		{"bad arch", SyncOptions{ManifestPath: "x", Target: "t", Arch: "riscv"}, "--arch must be"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.o.ValidateArgs()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ValidateArgs() = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestSyncDryRunPlan(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "manifest.yaml")
	if err := os.WriteFile(path, []byte(syncTestManifest), 0o600); err != nil {
		t.Fatal(err)
	}
	out := &bytes.Buffer{}
	o := &SyncOptions{
		IOStreams:    options.IOStreams{Out: out},
		ManifestPath: path,
		Target:       "harbor.local/kubeclipper",
		Arch:         "amd64",
		DryRun:       true,
	}
	if err := o.ValidateArgs(); err != nil {
		t.Fatal(err)
	}
	if err := o.Run(nil); err != nil {
		t.Fatalf("dry-run sync: %v", err)
	}
	got := out.String()
	for _, want := range []string{
		"would copy ghcr.io/kubeclipper/kubeclipper/kubeclipper/packages/k8s/k8s:v1.37.0",
		"-> harbor.local/kubeclipper/kubeclipper/packages/k8s/k8s:v1.37.0",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("dry-run output %q missing %q", got, want)
		}
	}
}
