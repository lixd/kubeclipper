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

// DefaultSupportPolicy is installed by kcctl deploy and is also the template
// emitted by kcctl delivery-policy template.
func DefaultSupportPolicy() *SupportPolicy {
	policy := NewSupportPolicy("default")
	policy.Spec.Policies = []KubernetesSupportPolicy{
		defaultKubernetesSupportPolicy("k8s-v1.36", "v1.36.*", "2.2.4", "v3.31.5"),
		defaultKubernetesSupportPolicy("k8s-v1.35", "v1.35.*", "1.7.29", "v3.29.6"),
		defaultKubernetesSupportPolicy("k8s-v1.34", "v1.34.*", "1.7.29", "v3.29.6"),
	}
	return policy
}

func defaultKubernetesSupportPolicy(name, kubernetesVersion, containerdVersion, calicoVersion string) KubernetesSupportPolicy {
	return KubernetesSupportPolicy{
		Name:  name,
		Match: PolicyMatch{KubernetesVersion: kubernetesVersion},
		ComponentSlots: []ComponentSlotRule{
			defaultSupportComponentSlot("cri", "cri", "containerd", containerdVersion),
			defaultSupportComponentSlot("cni", "cni", "calico", calicoVersion),
			defaultSupportComponentSlot("k8s-extension", "k8s-extension", "k8s-extension", "v1"),
			defaultSupportComponentSlot("bootstrap-kubeclipper", "bootstrap", "kubeclipper", "v2.0.0"),
			defaultSupportComponentSlot("bootstrap-etcd", "bootstrap", "etcd", "3.5.21"),
		},
	}
}

func defaultSupportComponentSlot(slot, kind, name, version string) ComponentSlotRule {
	return ComponentSlotRule{
		Slot:      slot,
		Selection: SelectionOneOf,
		Required:  true,
		Default:   ComponentChoice{Name: name, Version: version},
		Options: []ComponentOption{
			{Kind: kind, Name: name, AllowedVersions: []string{version}},
		},
	}
}
