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

package apis

import "testing"

func TestDefaultSupportPolicy(t *testing.T) {
	policy := DefaultSupportPolicy()
	if err := policy.Validate(); err != nil {
		t.Fatalf("DefaultSupportPolicy() validation failed: %v", err)
	}

	wantVersions := []string{"v1.36.*", "v1.35.*", "v1.34.*"}
	if len(policy.Spec.Policies) != len(wantVersions) {
		t.Fatalf("policy count = %d, want %d", len(policy.Spec.Policies), len(wantVersions))
	}
	for index, want := range wantVersions {
		if got := policy.Spec.Policies[index].Match.KubernetesVersion; got != want {
			t.Errorf("policy[%d] Kubernetes version = %q, want %q", index, got, want)
		}
	}
}
