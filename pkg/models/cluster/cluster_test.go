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

package cluster

import (
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/query"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/utils/strutil"
)

func TestRegistryFuzzyFilterRedactsWithoutMutatingSource(t *testing.T) {
	list := &v1.RegistryList{Items: []v1.Registry{{RegistrySpec: v1.RegistrySpec{
		RegistryAuth: &v1.RegistryAuth{Username: "robot", Password: "secret"},
	}}}}

	got := RegistryFuzzyFilter(list, query.New())
	if len(got) != 1 {
		t.Fatalf("got %d registries, want 1", len(got))
	}
	registry, ok := got[0].(*v1.Registry)
	if !ok {
		t.Fatalf("filtered object type = %T, want *v1.Registry", got[0])
	}
	if password := registry.RegistryAuth.Password; password != strutil.SensitiveData {
		t.Fatalf("filtered password = %q, want redacted", password)
	}
	if password := list.Items[0].RegistryAuth.Password; password != "secret" {
		t.Fatalf("source password mutated to %q", password)
	}
}

func TestClusterFuzzyFilterRedactsWithoutMutatingSource(t *testing.T) {
	list := &v1.ClusterList{Items: []v1.Cluster{{Status: v1.ClusterStatus{Registries: []v1.RegistrySpec{{
		RegistryAuth: &v1.RegistryAuth{Username: "robot", Password: "secret"},
	}}}}}}

	got := (&clusterOperator{}).clusterFuzzyFilter(list, query.New())
	if len(got) != 1 {
		t.Fatalf("got %d clusters, want 1", len(got))
	}
	cluster, ok := got[0].(*v1.Cluster)
	if !ok {
		t.Fatalf("filtered object type = %T, want *v1.Cluster", got[0])
	}
	if password := cluster.Status.Registries[0].RegistryAuth.Password; password != strutil.SensitiveData {
		t.Fatalf("filtered password = %q, want redacted", password)
	}
	if password := list.Items[0].Status.Registries[0].RegistryAuth.Password; password != "secret" {
		t.Fatalf("source password mutated to %q", password)
	}
}
