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
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1/cni"
)

type runtimeImageVersions struct {
	Etcd    string
	CoreDNS string
	Pause   string
}

var (
	kubernetesRuntimeImageVersions = map[string]runtimeImageVersions{
		"1.36": {Etcd: "3.6.8-0", CoreDNS: "v1.14.2", Pause: "3.10.2"},
	}
	calicoOperatorVersions = map[string]string{
		"v3.29.6": "v1.36.14",
		"v3.31.5": "v1.40.8",
	}
	runtimeImageExists = func(ctx context.Context, ref string) error {
		parsed, err := name.ParseReference(ref, name.Insecure)
		if err != nil {
			return err
		}
		_, err = remote.Head(parsed, remote.WithContext(ctx))
		return err
	}
)

func (h *handler) precheckRuntimeImages(ctx context.Context, cluster *corev1.Cluster) error {
	if cluster == nil || !cluster.Offline() {
		return nil
	}
	if strings.TrimSpace(cluster.LocalRegistry) == "" {
		return fmt.Errorf("offline cluster requires localRegistry; runtime image tarball loading has been removed")
	}
	images, err := requiredRuntimeImages(cluster)
	if err != nil {
		return err
	}
	var missing []string
	for _, image := range images {
		if err := runtimeImageExists(ctx, image); err != nil {
			missing = append(missing, image)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("local registry %s is missing runtime images: %s; publish them with scripts/open-packaging/push-runtime-images.sh or sync them with Harbor before creating the cluster",
			cluster.LocalRegistry, strings.Join(missing, ", "))
	}
	return nil
}

func requiredRuntimeImages(cluster *corev1.Cluster) ([]string, error) {
	registry := strings.TrimSuffix(strings.TrimSpace(cluster.LocalRegistry), "/")
	if registry == "" {
		return nil, fmt.Errorf("localRegistry is required")
	}
	versions, ok := kubernetesRuntimeImageVersions[kubernetesMinor(cluster.KubernetesVersion)]
	if !ok {
		return nil, fmt.Errorf("runtime image manifest for Kubernetes %s is not available; add an image BOM before creating the cluster", cluster.KubernetesVersion)
	}
	images := []string{
		registry + "/kube-apiserver:" + cluster.KubernetesVersion,
		registry + "/kube-controller-manager:" + cluster.KubernetesVersion,
		registry + "/kube-scheduler:" + cluster.KubernetesVersion,
		registry + "/kube-proxy:" + cluster.KubernetesVersion,
		registry + "/etcd:" + versions.Etcd,
		registry + "/coredns:" + versions.CoreDNS,
		registry + "/pause:" + versions.Pause,
		registry + "/kubeclipper/kubectl:latest",
	}
	if _, onlyK8s := cluster.Annotations[common.AnnotationOnlyInstallKubernetesComp]; !onlyK8s && cluster.CNI.Type == "calico" {
		calicoImages, err := requiredCalicoImages(registry, cluster.CNI.Version)
		if err != nil {
			return nil, err
		}
		images = append(images, calicoImages...)
	}
	return uniqueSorted(images), nil
}

func requiredCalicoImages(registry, version string) ([]string, error) {
	images := []string{
		registry + "/calico/cni:" + version,
		registry + "/calico/kube-controllers:" + version,
		registry + "/calico/node:" + version,
		// Some Calico versions still reference this image from init containers.
		registry + "/calico/pod2daemon-flexvol:" + version,
	}
	if cni.UseCalicoOperator(version) {
		operatorVersion, ok := calicoOperatorVersions[version]
		if !ok {
			return nil, fmt.Errorf("runtime image manifest for Calico %s is not available; add an image BOM before creating the cluster", version)
		}
		images = append(images,
			registry+"/tigera/operator:"+operatorVersion,
			registry+"/calico/typha:"+version,
			registry+"/calico/apiserver:"+version,
		)
	}
	return images, nil
}

func kubernetesMinor(version string) string {
	parts := strings.Split(strings.TrimPrefix(version, "v"), ".")
	if len(parts) < 2 {
		return strings.TrimPrefix(version, "v")
	}
	return parts[0] + "." + parts[1]
}

func uniqueSorted(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
