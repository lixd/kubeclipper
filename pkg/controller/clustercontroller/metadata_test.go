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

package clustercontroller

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/golang/mock/gomock"

	"github.com/kubeclipper/kubeclipper/pkg/clusteroperation"
	"github.com/kubeclipper/kubeclipper/pkg/component"
	clustermock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1/cri"
)

func TestClusterMetadataPreservesRegistryPrefixForNodeExpansion(t *testing.T) {
	ctrl := gomock.NewController(t)
	op := clustermock.NewMockOperator(ctrl)
	reg := &v1.Registry{RegistrySpec: v1.RegistrySpec{Host: "registry.example.com:5251", RepositoryPrefix: "qualified/team", Scheme: "https"}}
	op.EXPECT().GetRegistry(gomock.Any(), "default").Return(reg, nil).Times(2)
	c := &v1.Cluster{ImageRegistry: "default", KubernetesVersion: "v1.28.0", ContainerRuntime: v1.ContainerRuntime{Type: v1.CRIContainerd, Version: "1.7.0"}}
	r := &ClusterReconciler{ClusterOperator: op}
	meta, err := r.assembleClusterExtraMetadata(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	ctx := component.WithExtraMetadata(context.Background(), *meta)
	steps, err := clusteroperation.GetCriStep(ctx, c, op, v1.ActionInstall, []v1.StepNode{{ID: "worker"}})
	if err != nil {
		t.Fatal(err)
	}
	var runtime cri.ContainerdRunnable
	if err = json.Unmarshal(steps[0].Commands[0].CustomCommand, &runtime); err != nil {
		t.Fatal(err)
	}
	want := "registry.example.com:5251/qualified/team"
	if runtime.ImageRegistry != want {
		t.Errorf("node-add runtime imageRegistry = %q, want %q; controller metadata = %q; repaired cluster field = %q", runtime.ImageRegistry, want, meta.ImageRegistry, c.ResolvedImageRegistry)
	}
}
