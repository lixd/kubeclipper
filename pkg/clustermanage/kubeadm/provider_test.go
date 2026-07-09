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

package kubeadm

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/cli/config"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

type staticKubeadmPolicyStore struct {
	policy *deliveryapis.SupportPolicy
}

func (s staticKubeadmPolicyStore) Get(ctx context.Context) (*deliveryapis.SupportPolicy, error) {
	return s.policy, nil
}

func (s staticKubeadmPolicyStore) Update(ctx context.Context, mutator func(*deliveryapis.SupportPolicy) error) error {
	return fmt.Errorf("not implemented")
}

func TestBootstrapBinaryInstallHook(t *testing.T) {
	hook := bootstrapBinaryInstallHook("kubeclipper-agent", false)
	if !strings.Contains(hook, "install -m 0755 "+filepath.Join(config.DefaultPkgPath, "kubeclipper-agent")) {
		t.Fatalf("hook = %q", hook)
	}
	if strings.Contains(hook, "command -v") {
		t.Fatalf("unexpected missing-only guard in hook: %q", hook)
	}
}

func TestBootstrapBinaryInstallHookOnlyIfMissing(t *testing.T) {
	hook := bootstrapBinaryInstallHook("etcdctl", true)
	if !strings.Contains(hook, "command -v etcdctl") {
		t.Fatalf("hook = %q", hook)
	}
	if !strings.Contains(hook, "install -m 0755 "+filepath.Join(config.DefaultPkgPath, "etcdctl")) {
		t.Fatalf("hook = %q", hook)
	}
}

func TestResolveBootstrapBinaryComponent(t *testing.T) {
	catalog := deliveryapis.NewPackageInventory("default")
	catalog.Spec.Packages = []deliveryapis.PackageEntry{
		{
			Kind:           "bootstrap",
			Name:           "kubeclipper",
			Version:        "v1.7.0",
			Arch:           "amd64",
			ContentProfile: deliveryapis.ContentProfileBinary,
			Transport: deliveryapis.TransportRef{
				Type:   deliveryapis.TransportOCI,
				Ref:    "registry.local:5000/kubeclipper/packages/bootstrap/kubeclipper:v1.7.0",
				Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
			},
			Contents: []deliveryapis.ArtifactContent{
				{Name: "kubeclipper-agent", File: "kubeclipper-agent", MediaType: deliveryapis.MediaTypeBinaryLayer},
			},
		},
	}
	oldIndexer := registryPackageInventoryIndexer
	registryPackageInventoryIndexer = func(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
		return catalog, nil
	}
	defer func() {
		registryPackageInventoryIndexer = oldIndexer
	}()

	component, err := resolveBootstrapBinaryComponent(context.Background(), "registry.local:5000", staticKubeadmPolicyStore{policy: kubeadmBootstrapPolicy()}, "amd64", "v1.36.0", "kubeclipper", "kubeclipper-agent")
	if err != nil {
		t.Fatalf("resolveBootstrapBinaryComponent() error = %v", err)
	}
	if component.Kind != "bootstrap" || component.Name != "kubeclipper" {
		t.Fatalf("component = %+v", component)
	}
	if component.Contents[0].Name != "kubeclipper-agent" {
		t.Fatalf("content = %+v", component.Contents[0])
	}
}

func TestResolveBootstrapBinaryComponentNotFound(t *testing.T) {
	catalog := deliveryapis.NewPackageInventory("default")
	oldIndexer := registryPackageInventoryIndexer
	registryPackageInventoryIndexer = func(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
		return catalog, nil
	}
	defer func() {
		registryPackageInventoryIndexer = oldIndexer
	}()

	_, err := resolveBootstrapBinaryComponent(context.Background(), "registry.local:5000", staticKubeadmPolicyStore{policy: kubeadmBootstrapPolicy()}, "amd64", "v1.36.0", "kubeclipper", "kubeclipper-agent")
	if err == nil {
		t.Fatalf("resolveBootstrapBinaryComponent() expected error")
	}
	if !isBootstrapBinaryNotFound(err) {
		t.Fatalf("expected bootstrap binary not found error, got %v", err)
	}
}

func TestResolveBootstrapBinaryComponentReturnsRegistryErrorWithoutFileFallback(t *testing.T) {
	oldIndexer := registryPackageInventoryIndexer
	registryPackageInventoryIndexer = func(ctx context.Context, registry string) (*deliveryapis.PackageInventory, error) {
		return nil, fmt.Errorf("registry unavailable")
	}
	defer func() {
		registryPackageInventoryIndexer = oldIndexer
	}()

	_, err := resolveBootstrapBinaryComponent(context.Background(), "registry.local:5000", staticKubeadmPolicyStore{policy: kubeadmBootstrapPolicy()}, "amd64", "v1.36.0", "kubeclipper", "kubeclipper-agent")
	if err == nil {
		t.Fatalf("resolveBootstrapBinaryComponent() expected error")
	}
	if !strings.Contains(err.Error(), "registry unavailable") {
		t.Fatalf("error = %v, want registry error", err)
	}
}

func kubeadmBootstrapPolicy() *deliveryapis.SupportPolicy {
	policy := deliveryapis.NewSupportPolicy("default")
	policy.Spec.Policies = []deliveryapis.KubernetesSupportPolicy{{
		Name:  "k8s-v1.36",
		Match: deliveryapis.PolicyMatch{KubernetesVersion: "v1.36.*"},
		ComponentSlots: []deliveryapis.ComponentSlotRule{{
			Slot:      "bootstrap-kubeclipper",
			Selection: deliveryapis.SelectionOneOf,
			Required:  true,
			Default:   deliveryapis.ComponentChoice{Name: "kubeclipper", Version: "v1.7.0"},
			Options: []deliveryapis.ComponentOption{{
				Kind:            "bootstrap",
				Name:            "kubeclipper",
				AllowedVersions: []string{"v1.7.0"},
			}},
		}},
	}}
	return policy
}

func TestNormalizeKernelArch(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "x86_64", in: "x86_64", want: "amd64"},
		{name: "aarch64", in: "aarch64", want: "arm64"},
		{name: "passthrough", in: "s390x", want: "s390x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeKernelArch(tt.in); got != tt.want {
				t.Fatalf("normalizeKernelArch() = %q, want %q", got, tt.want)
			}
		})
	}
}
