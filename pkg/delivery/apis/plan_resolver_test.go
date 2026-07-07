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

type staticPolicyStore struct {
	policy *SupportPolicy
	err    error
}

func (s staticPolicyStore) Get(ctx context.Context) (*SupportPolicy, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.policy, nil
}

func (s staticPolicyStore) Update(ctx context.Context, mutator func(*SupportPolicy) error) error {
	if s.err != nil {
		return s.err
	}
	if mutator == nil {
		return nil
	}
	return mutator(s.policy)
}

func TestResolveArtifactsFromStores(t *testing.T) {
	plan, err := ResolveArtifactsFromStores(context.Background(), staticInventoryStore{
		catalog: resolverCatalog(),
	}, staticPolicyStore{
		policy: resolverPolicy(),
	}, ResolveRequest{
		KubernetesVersion: "v1.36.0",
		Arch:              "amd64",
	})
	if err != nil {
		t.Fatalf("ResolveArtifactsFromStores() error: %v", err)
	}
	if len(plan.Components) != 3 {
		t.Fatalf("components length = %d", len(plan.Components))
	}
}
