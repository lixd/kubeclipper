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

package apis

import (
	"context"
	"testing"
)

type staticInventoryStore struct {
	catalog *PackageInventory
	err     error
}

func (s staticInventoryStore) Get(ctx context.Context) (*PackageInventory, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.catalog, nil
}

func TestResolveBootstrapBinaryFromStores(t *testing.T) {
	component, err := ResolveBootstrapBinaryFromStores(context.Background(), staticInventoryStore{
		catalog: &PackageInventory{
			Spec: PackageInventorySpec{
				Packages: []PackageEntry{
					{
						Kind:           "bootstrap",
						Name:           "kubeclipper",
						Version:        "v1.7.0",
						Arch:           "amd64",
						ContentProfile: ContentProfileBinary,
						Transport: TransportRef{
							Type:   TransportOCI,
							Ref:    "registry.local/kubeclipper/packages/bootstrap/kubeclipper:v1.7.0",
							Digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
						},
						Contents: []ArtifactContent{
							{Name: "kubeclipper-server", File: "kubeclipper-server", MediaType: MediaTypeBinaryLayer},
							{Name: "kubeclipper-agent", File: "kubeclipper-agent", MediaType: MediaTypeBinaryLayer},
						},
					},
				},
			},
		},
	}, staticPolicyStore{
		policy: bootstrapPolicy(),
	}, BootstrapBinaryResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates: []PackageCandidate{
			{Kind: "bootstrap", Name: "kubeclipper"},
		},
		Contents: []string{"kubeclipper-agent"},
	})
	if err != nil {
		t.Fatalf("ResolveBootstrapBinaryFromStores() error: %v", err)
	}
	if component.Name != "kubeclipper" || len(component.Contents) != 1 || component.Contents[0].Name != "kubeclipper-agent" {
		t.Fatalf("component = %+v", component)
	}
}

func TestResolveBootstrapBinaryFromStoresNotFound(t *testing.T) {
	_, err := ResolveBootstrapBinaryFromStores(context.Background(), staticInventoryStore{
		catalog: NewPackageInventory("default"),
	}, staticPolicyStore{
		policy: bootstrapPolicy(),
	}, BootstrapBinaryResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates: []PackageCandidate{
			{Kind: "bootstrap", Name: "kubeclipper"},
		},
		Contents: []string{"kubeclipper-agent"},
	})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestResolveExtensionArtifactFromStores(t *testing.T) {
	component, err := ResolveExtensionArtifactFromStores(context.Background(), staticInventoryStore{
		catalog: &PackageInventory{
			Spec: PackageInventorySpec{
				Packages: []PackageEntry{
					{
						Kind:           "extension",
						Name:           "kubectl-terminal",
						Version:        "v1.0.0",
						Arch:           "amd64",
						ContentProfile: ContentProfileExtension,
						Transport: TransportRef{
							Type:   TransportOCI,
							Ref:    "registry.local/kubeclipper/packages/extension/kubectl-terminal:v1.0.0",
							Digest: "sha256:2222222222222222222222222222222222222222222222222222222222222222",
						},
						Contents: []ArtifactContent{{Name: ContentImages, File: "images.tar.gz", MediaType: MediaTypeImagesLayer}},
					},
				},
			},
		},
	}, staticPolicyStore{
		policy: extensionPolicy(),
	}, ExtensionResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates: []ExtensionCandidate{
			{Kind: "extension", Name: "kubectl-terminal"},
		},
	})
	if err != nil {
		t.Fatalf("ResolveExtensionArtifactFromStores() error: %v", err)
	}
	if component.Name != "kubectl-terminal" || component.Slot != "extension" {
		t.Fatalf("component = %+v", component)
	}
}

func TestResolveExtensionArtifactFromStoresNil(t *testing.T) {
	_, err := ResolveExtensionArtifactFromStores(context.Background(), nil, staticPolicyStore{policy: extensionPolicy()}, ExtensionResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates: []ExtensionCandidate{
			{Kind: "extension", Name: "kubectl-terminal"},
		},
	})
	if err == nil {
		t.Fatalf("expected error")
	}
}
