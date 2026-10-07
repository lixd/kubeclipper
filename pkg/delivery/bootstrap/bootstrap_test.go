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

package bootstrap

import (
	"context"
	"errors"
	"testing"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

type fakeBootstrapIndexer struct {
	refreshErr        error
	refreshInventory  *deliveryapis.PackageInventory
	refreshCalls      int
	fallbackInventory *deliveryapis.PackageInventory
	fallbackRepos     []string
	fallbackCalls     int
}

func (f *fakeBootstrapIndexer) Refresh(_ context.Context, _ string) (*deliveryapis.PackageInventory, error) {
	f.refreshCalls++
	if f.refreshErr != nil {
		return nil, f.refreshErr
	}
	return f.refreshInventory, nil
}

func (f *fakeBootstrapIndexer) IndexRepositories(_ context.Context, _ string, repositories []string) (*deliveryapis.PackageInventory, error) {
	f.fallbackCalls++
	f.fallbackRepos = append(f.fallbackRepos, repositories...)
	return f.fallbackInventory, nil
}

// GHCR denies the registry-wide catalog to anonymous and most tokens, which
// broke every deploy; the bootstrap packages live at fixed repositories that
// are pullable by path, so the refresh must fall back to indexing those (R30).
func TestRefreshBootstrapInventoryFallsBackWhenCatalogDenied(t *testing.T) {
	fallbackInventory := &deliveryapis.PackageInventory{}
	fake := &fakeBootstrapIndexer{
		refreshErr:        errors.New("GET https://ghcr.io/token?scope=registry%3Acatalog%3A*: DENIED"),
		fallbackInventory: fallbackInventory,
	}

	inventory, err := refreshBootstrapInventory(context.Background(), fake, "ghcr.io/lixd/kubeclipper", deployBootstrapAssets)
	if err != nil {
		t.Fatalf("refreshBootstrapInventory() = %v, want the fixed-repository fallback to succeed", err)
	}
	if inventory != fallbackInventory {
		t.Fatal("the fallback inventory was not used")
	}
	want := []string{
		"kubeclipper/packages/bootstrap/kubeclipper",
		"kubeclipper/packages/bootstrap/etcd",
		"kubeclipper/packages/bootstrap/console",
		"kubeclipper/packages/bootstrap/registry",
	}
	if len(fake.fallbackRepos) != len(want) {
		t.Fatalf("fallback repositories = %v, want %v", fake.fallbackRepos, want)
	}
	for i, repo := range want {
		if fake.fallbackRepos[i] != repo {
			t.Fatalf("fallback repositories[%d] = %q, want %q", i, fake.fallbackRepos[i], repo)
		}
	}
}

// A working catalog pass must not trigger the fixed-repository fallback.
func TestRefreshBootstrapInventoryUsesCatalogWhenAvailable(t *testing.T) {
	catalogInventory := &deliveryapis.PackageInventory{}
	fake := &fakeBootstrapIndexer{refreshInventory: catalogInventory}

	inventory, err := refreshBootstrapInventory(context.Background(), fake, "172.16.131.146:5003", deployBootstrapAssets)
	if err != nil {
		t.Fatalf("refreshBootstrapInventory() = %v", err)
	}
	if inventory != catalogInventory {
		t.Fatal("the catalog inventory was not used")
	}
	if fake.fallbackCalls != 0 {
		t.Fatalf("fallback ran %d times, want 0 when the catalog works", fake.fallbackCalls)
	}
}
