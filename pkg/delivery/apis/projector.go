/*
 *
 *  * Copyright 2021 KubeClipper Authors.
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
	"sort"

	"github.com/kubeclipper/kubeclipper/pkg/scheme"
)

type ComponentMetaProjection struct {
	Rules       []map[string]interface{}
	Addons      []scheme.MetaResource
	Unavailable []ComponentAvailability
}

type ComponentAvailability struct {
	KubernetesVersion string `json:"kubernetesVersion"`
	Slot              string `json:"slot"`
	Kind              string `json:"kind"`
	Name              string `json:"name"`
	Version           string `json:"version"`
	Arch              string `json:"arch,omitempty"`
	Reason            string `json:"reason"`
}

type ProjectOptions struct {
	Archs              []string
	KubeClipperVersion string
}

func ProjectComponentMeta(inventory *PackageInventory, policy *SupportPolicy, opts ProjectOptions) (*ComponentMetaProjection, error) {
	if err := inventory.Validate(); err != nil {
		return nil, err
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	archSet := makeStringSet(opts.Archs)
	packages := filterSelectablePackages(inventory.Spec.Packages, archSet)
	projection := &ComponentMetaProjection{
		Addons:      projectAddons(packages, policy, opts, archSet),
		Rules:       projectRules(packages, policy, opts, archSet),
		Unavailable: projectUnavailable(inventory.Spec.Packages, policy, opts, archSet),
	}
	return projection, nil
}

func filterSelectablePackages(packages []PackageEntry, archSet map[string]struct{}) []PackageEntry {
	var out []PackageEntry
	for _, pkg := range packages {
		if len(archSet) > 0 {
			if _, ok := archSet[pkg.Arch]; !ok {
				continue
			}
		}
		out = append(out, pkg)
	}
	return out
}

func projectAddons(packages []PackageEntry, policy *SupportPolicy, opts ProjectOptions, archSet map[string]struct{}) []scheme.MetaResource {
	supported := make(map[string]PackageEntry)
	k8sPackages := packagesByKind(packages, "k8s")
	for _, k8sPkg := range k8sPackages {
		supportPolicy, ok := selectProjectedPolicy(policy.Spec.Policies, k8sPkg.Version, opts.KubeClipperVersion)
		if !ok {
			continue
		}
		supported[PackageKey(k8sPkg)] = k8sPkg
		for _, slot := range supportPolicy.ComponentSlots {
			for _, pkg := range projectSlotPackages(packages, slot, archSet) {
				supported[PackageKey(pkg)] = pkg
			}
		}
	}
	addons := make([]scheme.MetaResource, 0, len(supported))
	for _, pkg := range packages {
		if _, ok := supported[PackageKey(pkg)]; !ok {
			continue
		}
		addons = append(addons, scheme.MetaResource{
			Type:    pkg.Kind,
			Name:    pkg.Name,
			Version: pkg.Version,
			Arch:    pkg.Arch,
		})
	}
	sort.SliceStable(addons, func(i, j int) bool {
		if addons[i].Type != addons[j].Type {
			return addons[i].Type < addons[j].Type
		}
		if addons[i].Name != addons[j].Name {
			return addons[i].Name < addons[j].Name
		}
		if addons[i].Version != addons[j].Version {
			return addons[i].Version < addons[j].Version
		}
		return addons[i].Arch < addons[j].Arch
	})
	return addons
}

func projectRules(packages []PackageEntry, policy *SupportPolicy, opts ProjectOptions, archSet map[string]struct{}) []map[string]interface{} {
	var rules []map[string]interface{}
	k8sPackages := packagesByKind(packages, "k8s")
	for _, k8sPkg := range k8sPackages {
		supportPolicy, ok := selectProjectedPolicy(policy.Spec.Policies, k8sPkg.Version, opts.KubeClipperVersion)
		if !ok {
			continue
		}
		versionControl := make(map[string]interface{}, len(supportPolicy.ComponentSlots))
		resolvable := true
		for _, slot := range supportPolicy.ComponentSlots {
			validAddons := projectSlotAddons(packages, slot, archSet)
			if len(validAddons) == 0 && slot.Required {
				resolvable = false
				break
			}
			versionControl[slot.Slot] = validAddons
		}
		if !resolvable || len(versionControl) == 0 {
			continue
		}
		rules = append(rules, map[string]interface{}{
			"name":            k8sPkg.Name,
			"version":         k8sPkg.Version,
			"type":            k8sPkg.Kind,
			"arch":            k8sPkg.Arch,
			"version_control": versionControl,
		})
	}
	return rules
}

func projectUnavailable(packages []PackageEntry, policy *SupportPolicy, opts ProjectOptions, archSet map[string]struct{}) []ComponentAvailability {
	var unavailable []ComponentAvailability
	seen := make(map[string]struct{})
	k8sPackages := packagesByKind(packages, "k8s")
	for _, k8sPkg := range k8sPackages {
		supportPolicy, ok := selectProjectedPolicy(policy.Spec.Policies, k8sPkg.Version, opts.KubeClipperVersion)
		if !ok {
			continue
		}
		for _, slot := range supportPolicy.ComponentSlots {
			for _, option := range slot.Options {
				for _, version := range option.AllowedVersions {
					for _, missing := range unavailablePackageArchs(packages, option, version, archSet) {
						item := ComponentAvailability{
							KubernetesVersion: k8sPkg.Version,
							Slot:              slot.Slot,
							Kind:              option.Kind,
							Name:              option.Name,
							Version:           version,
							Arch:              missing.arch,
							Reason:            missing.reason,
						}
						key := item.KubernetesVersion + "|" + item.Slot + "|" + item.Kind + "|" + item.Name + "|" + item.Version + "|" + item.Arch + "|" + item.Reason
						if _, ok := seen[key]; ok {
							continue
						}
						seen[key] = struct{}{}
						unavailable = append(unavailable, item)
					}
				}
			}
		}
	}
	sort.SliceStable(unavailable, func(i, j int) bool {
		if unavailable[i].KubernetesVersion != unavailable[j].KubernetesVersion {
			return unavailable[i].KubernetesVersion < unavailable[j].KubernetesVersion
		}
		if unavailable[i].Slot != unavailable[j].Slot {
			return unavailable[i].Slot < unavailable[j].Slot
		}
		if unavailable[i].Kind != unavailable[j].Kind {
			return unavailable[i].Kind < unavailable[j].Kind
		}
		if unavailable[i].Name != unavailable[j].Name {
			return unavailable[i].Name < unavailable[j].Name
		}
		if unavailable[i].Version != unavailable[j].Version {
			return unavailable[i].Version < unavailable[j].Version
		}
		return unavailable[i].Arch < unavailable[j].Arch
	})
	return unavailable
}

type missingPackageArch struct {
	arch   string
	reason string
}

func unavailablePackageArchs(packages []PackageEntry, option ComponentOption, version string, archSet map[string]struct{}) []missingPackageArch {
	if len(archSet) == 0 {
		if packageVersionExists(packages, option, version) {
			return nil
		}
		return []missingPackageArch{{reason: "notPublished"}}
	}
	archs := sortedStringSet(archSet)
	var missing []missingPackageArch
	for _, arch := range archs {
		if packageVersionArchExists(packages, option, version, arch) {
			continue
		}
		reason := "notPublished"
		if packageVersionExists(packages, option, version) {
			reason = "archUnavailable"
		}
		missing = append(missing, missingPackageArch{arch: arch, reason: reason})
	}
	return missing
}

func packageVersionExists(packages []PackageEntry, option ComponentOption, version string) bool {
	for _, pkg := range packages {
		if pkg.Kind == option.Kind && pkg.Name == option.Name && pkg.Version == version {
			return true
		}
	}
	return false
}

func packageVersionArchExists(packages []PackageEntry, option ComponentOption, version, arch string) bool {
	for _, pkg := range packages {
		if pkg.Kind == option.Kind && pkg.Name == option.Name && pkg.Version == version && pkg.Arch == arch {
			return true
		}
	}
	return false
}

func sortedStringSet(set map[string]struct{}) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

func selectProjectedPolicy(policies []KubernetesSupportPolicy, kubernetesVersion, kubeClipperVersion string) (KubernetesSupportPolicy, bool) {
	for _, supportPolicy := range policies {
		if !matchKubernetesVersion(kubernetesVersion, supportPolicy.Match.KubernetesVersion) {
			continue
		}
		if supportPolicy.Match.KubeClipperVersion != "" && kubeClipperVersion != "" && supportPolicy.Match.KubeClipperVersion != kubeClipperVersion {
			continue
		}
		return supportPolicy, true
	}
	return KubernetesSupportPolicy{}, false
}

func projectSlotAddons(packages []PackageEntry, slot ComponentSlotRule, archSet map[string]struct{}) []map[string]interface{} {
	var validAddons []map[string]interface{}
	for _, pkg := range projectSlotPackages(packages, slot, archSet) {
		validAddons = append(validAddons, map[string]interface{}{
			"name":    pkg.Name,
			"version": pkg.Version,
			"type":    pkg.Kind,
			"arch":    pkg.Arch,
			"default": slot.Default.Name == pkg.Name && slot.Default.Version == pkg.Version,
		})
	}
	return validAddons
}

func projectSlotPackages(packages []PackageEntry, slot ComponentSlotRule, archSet map[string]struct{}) []PackageEntry {
	var validPackages []PackageEntry
	for _, option := range slot.Options {
		for _, pkg := range packages {
			if pkg.Kind != option.Kind || pkg.Name != option.Name {
				continue
			}
			if !hasString(option.AllowedVersions, pkg.Version) {
				continue
			}
			if len(archSet) > 1 && !availableForAllArchs(packages, option, pkg.Version, archSet) {
				continue
			}
			validPackages = append(validPackages, pkg)
		}
	}
	return validPackages
}

func availableForAllArchs(packages []PackageEntry, option ComponentOption, version string, archSet map[string]struct{}) bool {
	found := make(map[string]struct{}, len(archSet))
	for _, pkg := range packages {
		if pkg.Kind != option.Kind || pkg.Name != option.Name || pkg.Version != version {
			continue
		}
		if _, ok := archSet[pkg.Arch]; ok {
			found[pkg.Arch] = struct{}{}
		}
	}
	return len(found) == len(archSet)
}

func packagesByKind(packages []PackageEntry, kind string) []PackageEntry {
	var out []PackageEntry
	for _, pkg := range packages {
		if pkg.Kind == kind {
			out = append(out, pkg)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Version != out[j].Version {
			return out[i].Version < out[j].Version
		}
		return out[i].Arch < out[j].Arch
	})
	return out
}

func matchKubernetesVersion(version, pattern string) bool {
	if pattern == "" {
		return false
	}
	if len(pattern) > 1 && pattern[len(pattern)-1:] == "*" {
		return len(version) >= len(pattern)-1 && version[:len(pattern)-1] == pattern[:len(pattern)-1]
	}
	return version == pattern
}

func makeStringSet(values []string) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		set[value] = struct{}{}
	}
	return set
}
