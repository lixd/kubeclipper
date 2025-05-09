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

package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	apimachineryErrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/kubeclipper/kubeclipper/pkg/controller/clustercontroller"
	"github.com/kubeclipper/kubeclipper/pkg/query"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

// TODO: copy from ClusterController, Remove after refactoring
func (h *handler) getClusterCRIRegistries(ctx context.Context, c *v1.Cluster) ([]v1.RegistrySpec, error) {
	type mirror struct {
		ImageRepoMirror string `json:"imageRepoMirror"`
	}
	insecureRegistry := append([]string{}, c.ContainerRuntime.InsecureRegistry...)
	sort.Strings(insecureRegistry)
	// add addons mirror registry
	for _, a := range c.Addons {
		var m mirror
		err := json.Unmarshal(a.Config.Raw, &m)
		if err != nil {
			continue
		}
		if m.ImageRepoMirror != "" {
			idx, ok := sort.Find(len(insecureRegistry), func(i int) int {
				return strings.Compare(m.ImageRepoMirror, insecureRegistry[i])
			})
			if !ok {
				if idx == len(insecureRegistry) {
					insecureRegistry = append(insecureRegistry, m.ImageRepoMirror)
				} else {
					insecureRegistry = append(insecureRegistry[:idx+1], insecureRegistry[idx:]...)
					insecureRegistry[idx] = m.ImageRepoMirror
				}
			}
		}
	}

	registries := make([]v1.RegistrySpec, 0, len(insecureRegistry)*2+len(c.ContainerRuntime.Registries))
	// insecure registry
	for _, host := range insecureRegistry {
		registries = appendUniqueRegistry(registries,
			v1.RegistrySpec{Scheme: "http", Host: host},
			v1.RegistrySpec{Scheme: "https", Host: host, SkipVerify: true})
	}

	validRegistries := c.ContainerRuntime.Registries[:0]
	for _, reg := range c.ContainerRuntime.Registries {
		if reg.RegistryRef == nil || *reg.RegistryRef == "" {
			// fix reg.RegistryRef=""
			reg.RegistryRef = nil
			registries = appendUniqueRegistry(registries,
				v1.RegistrySpec{Scheme: "http", Host: reg.InsecureRegistry},
				v1.RegistrySpec{Scheme: "https", Host: reg.InsecureRegistry, SkipVerify: true})
			validRegistries = append(validRegistries, reg)
			continue
		}
		registry, err := h.clusterOperator.GetRegistry(ctx, *reg.RegistryRef)
		if err != nil {
			if apimachineryErrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get registry %s:%w", *reg.RegistryRef, err)
		}
		registries = appendUniqueRegistry(registries, registry.RegistrySpec)
		validRegistries = append(validRegistries, reg)
	}
	c.ContainerRuntime.Registries = validRegistries
	return registries, nil
}

// TODO: copy from ClusterController, Remove after refactoring
func appendUniqueRegistry(s []v1.RegistrySpec, items ...v1.RegistrySpec) []v1.RegistrySpec {
	for _, r := range items {
		key := r.Scheme + r.Host
		idx, ok := sort.Find(len(s), func(i int) int {
			return strings.Compare(key, s[i].Scheme+s[i].Host)
		})
		if !ok {
			if idx == len(s) {
				s = append(s, r)
			} else {
				s = append(s[:idx+1], s[idx:]...)
				s[idx] = r
			}
		}
	}
	return s
}

func registriesEqual(a, b []v1.RegistrySpec) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (h *handler) getCRIRegistriesStep(ctx context.Context, cluster *v1.Cluster, registries []v1.RegistrySpec) (*v1.Step, error) {
	if !registriesEqual(cluster.Status.Registries, registries) {
		q := query.New()
		q.LabelSelector = fmt.Sprintf("%s=%s", common.LabelClusterName, cluster.Name)

		nodeList, err := h.clusterOperator.ListNodes(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("list")
		}
		return criRegistryUpdateStep(cluster, registries, nodeList.Items)
	}
	return nil, nil
}

func criRegistryUpdateStep(cluster *v1.Cluster, registries []v1.RegistrySpec, nodes []v1.Node) (*v1.Step, error) {
	wrapNodes := make([]*v1.Node, 0, len(nodes))
	for i := range nodes {
		wrapNodes[i] = &nodes[i]
	}
	return clustercontroller.CRIRegistryUpdateStep(cluster, registries, wrapNodes)
}
