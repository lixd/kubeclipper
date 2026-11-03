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

package v1

import (
	"context"
	"testing"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

type repositoryInventoryProbe struct{ reads, refreshes int }

func (*repositoryInventoryProbe) Index(context.Context, string) (*deliveryapis.PackageInventory, error) {
	panic("policy indexing must not read the registry catalog")
}
func (p *repositoryInventoryProbe) IndexRepositories(context.Context, string, []string) (*deliveryapis.PackageInventory, error) {
	p.reads++
	return deliveryapis.NewPackageInventory("test"), nil
}
func (p *repositoryInventoryProbe) RefreshRepositories(context.Context, string, []string) (*deliveryapis.PackageInventory, error) {
	p.refreshes++
	return deliveryapis.NewPackageInventory("test"), nil
}

type inventoryPolicyStub struct{}

func (inventoryPolicyStub) Get(context.Context) (*deliveryapis.SupportPolicy, error) {
	return &deliveryapis.SupportPolicy{}, nil
}
func (inventoryPolicyStub) Update(context.Context, func(*deliveryapis.SupportPolicy) error) error {
	panic("unexpected policy update")
}

func TestPolicyInventoryRefreshBypassesRepositoryCache(t *testing.T) {
	indexer := &repositoryInventoryProbe{}
	store := indexedInventoryStore{registry: "registry.local", indexer: indexer, policy: inventoryPolicyStub{}}
	if _, err := store.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if indexer.reads != 1 || indexer.refreshes != 1 {
		t.Fatalf("reads=%d refreshes=%d, want 1/1", indexer.reads, indexer.refreshes)
	}
}
