/*
 *
 *  * Copyright 2026 KubeClipper Authors.
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

package v1

import (
	"strings"
	"testing"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

func TestAppendResolvedComponentRejectsDuplicateIdentity(t *testing.T) {
	plan := &deliveryapis.ResolvedArtifactPlan{}
	first := deliveryapis.ResolvedComponent{
		Slot:    "addon-metallb",
		Kind:    "lb",
		Name:    "metallb",
		Version: "v0.13.7",
	}
	if err := appendResolvedComponent(plan, first); err != nil {
		t.Fatalf("appendResolvedComponent() error: %+v", err)
	}
	if len(plan.Components) != 1 {
		t.Fatalf("components length = %d, want 1", len(plan.Components))
	}
	err := appendResolvedComponent(plan, deliveryapis.ResolvedComponent{
		Slot:    "addon-metallb-copy",
		Kind:    "lb",
		Name:    "metallb",
		Version: "v0.13.7",
	})
	if err == nil {
		t.Fatalf("appendResolvedComponent() expected duplicate error")
	}
	if !strings.Contains(err.Error(), "already contains lb/metallb:v0.13.7") {
		t.Fatalf("appendResolvedComponent() error = %v", err)
	}
	if len(plan.Components) != 1 {
		t.Fatalf("components length = %d, want duplicate rejected", len(plan.Components))
	}
}
