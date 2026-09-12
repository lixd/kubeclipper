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

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
)

// EncodeResolvedArtifactPlan stores a package plan in an API object's
// RawExtension without putting package blobs or credentials into etcd.
func EncodeResolvedArtifactPlan(plan *ResolvedArtifactPlan) (*runtime.RawExtension, error) {
	if plan == nil {
		return nil, nil
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return nil, fmt.Errorf("marshal resolved package plan: %w", err)
	}
	return &runtime.RawExtension{Raw: raw}, nil
}

// DecodeResolvedArtifactPlan decodes the package plan persisted on a Cluster.
// A nil extension means that the cluster has not yet been initialized with an
// OCI package plan.
func DecodeResolvedArtifactPlan(raw *runtime.RawExtension) (*ResolvedArtifactPlan, error) {
	if raw == nil || len(raw.Raw) == 0 {
		return nil, nil
	}
	plan := &ResolvedArtifactPlan{}
	if err := json.Unmarshal(raw.Raw, plan); err != nil {
		return nil, fmt.Errorf("decode resolved package plan: %w", err)
	}
	return plan, nil
}
