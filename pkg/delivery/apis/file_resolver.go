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
	"fmt"
	"os"
)

func ResolveBootstrapBinaryFromStores(ctx context.Context, inventoryStore InventoryStore, policyStore PolicyStore, req BootstrapBinaryResolveRequest) (ResolvedComponent, error) {
	if inventoryStore == nil {
		return ResolvedComponent{}, fmt.Errorf("inventory store is nil")
	}
	if policyStore == nil {
		return ResolvedComponent{}, fmt.Errorf("policy store is nil")
	}
	inventory, err := inventoryStore.Get(ctx)
	if err != nil {
		return ResolvedComponent{}, err
	}
	policy, err := policyStore.Get(ctx)
	if err != nil {
		return ResolvedComponent{}, err
	}
	component, err := ResolveBootstrapBinary(inventory, policy, req)
	if err != nil {
		if resolverErr, ok := err.(*ResolverError); ok && resolverErr.Code == ErrArtifactNotPublished {
			name := ""
			if len(req.Candidates) > 0 {
				name = req.Candidates[0].Name
			}
			return ResolvedComponent{}, fmt.Errorf("bootstrap binary %s for arch %q: %w", name, req.Arch, os.ErrNotExist)
		}
		return ResolvedComponent{}, err
	}
	return component, nil
}

func ResolveExtensionArtifactFromStores(ctx context.Context, inventoryStore InventoryStore, policyStore PolicyStore, req ExtensionResolveRequest) (ResolvedComponent, error) {
	if inventoryStore == nil {
		return ResolvedComponent{}, fmt.Errorf("inventory store is nil")
	}
	if policyStore == nil {
		return ResolvedComponent{}, fmt.Errorf("policy store is nil")
	}
	inventory, err := inventoryStore.Get(ctx)
	if err != nil {
		return ResolvedComponent{}, err
	}
	policy, err := policyStore.Get(ctx)
	if err != nil {
		return ResolvedComponent{}, err
	}
	component, err := ResolveExtensionArtifact(inventory, policy, req)
	if err != nil {
		if resolverErr, ok := err.(*ResolverError); ok && resolverErr.Code == ErrArtifactNotPublished {
			return ResolvedComponent{}, fmt.Errorf("extension candidates for arch %q: %w", req.Arch, os.ErrNotExist)
		}
		return ResolvedComponent{}, err
	}
	return component, nil
}
