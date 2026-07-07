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
	"fmt"
	"sort"
	"strings"
)

const (
	ErrUnsupportedKubernetesVersion = "UnsupportedKubernetesVersion"
	ErrUnsupportedComponentSlot     = "UnsupportedComponentSlot"
	ErrUnsupportedComponentChoice   = "UnsupportedComponentChoice"
	ErrUnsupportedComponentVersion  = "UnsupportedComponentVersion"
	ErrComponentConstraintViolation = "ComponentConstraintViolation"
	ErrArtifactNotPublished         = "ArtifactNotPublished"
	ErrArtifactArchUnavailable      = "ArtifactArchUnavailable"
	ErrDuplicateResolvedComponent   = "DuplicateResolvedComponent"
)

type ResolverError struct {
	Code    string
	Message string
}

func (e *ResolverError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func ResolveArtifacts(inventory *PackageInventory, policy *SupportPolicy, req ResolveRequest) (*ResolvedArtifactPlan, error) {
	if err := inventory.Validate(); err != nil {
		return nil, err
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	if req.KubernetesVersion == "" {
		return nil, &ResolverError{Code: ErrUnsupportedKubernetesVersion, Message: "kubernetesVersion is required"}
	}
	if req.Arch == "" {
		return nil, &ResolverError{Code: ErrArtifactArchUnavailable, Message: "arch is required"}
	}
	req.OS = packageOS(req.OS)
	supportPolicy, err := selectSupportPolicy(policy, req)
	if err != nil {
		return nil, err
	}
	selected, err := selectComponents(supportPolicy, req)
	if err != nil {
		return nil, err
	}
	selected = filterClusterInstallComponents(selected)
	if err = validateRelations(supportPolicy.Constraints, selected); err != nil {
		return nil, err
	}
	plan := &ResolvedArtifactPlan{
		KubernetesVersion: req.KubernetesVersion,
		OS:                req.OS,
		Arch:              req.Arch,
	}
	kubernetesComponent := selectedComponent{
		Slot:     "k8s",
		Kind:     "k8s",
		Name:     "k8s",
		Version:  req.KubernetesVersion,
		Required: true,
	}
	if err = validateSelectedComponentIdentities(append([]selectedComponent{kubernetesComponent}, selected...)); err != nil {
		return nil, err
	}
	pkg, err := resolvePackage(inventory, req, kubernetesComponent)
	if err != nil {
		return nil, err
	}
	plan.Components = append(plan.Components, ResolvedComponent{
		Slot:      kubernetesComponent.Slot,
		Kind:      kubernetesComponent.Kind,
		Name:      kubernetesComponent.Name,
		Version:   kubernetesComponent.Version,
		OS:        pkg.OS,
		Arch:      pkg.Arch,
		Required:  kubernetesComponent.Required,
		Transport: pkg.Transport,
		Contents:  pkg.Contents,
	})
	for _, selectedComponent := range selected {
		pkg, err := resolvePackage(inventory, req, selectedComponent)
		if err != nil {
			return nil, err
		}
		plan.Components = append(plan.Components, ResolvedComponent{
			Slot:      selectedComponent.Slot,
			Kind:      selectedComponent.Kind,
			Name:      selectedComponent.Name,
			Version:   selectedComponent.Version,
			OS:        pkg.OS,
			Arch:      pkg.Arch,
			Required:  selectedComponent.Required,
			Transport: pkg.Transport,
			Contents:  pkg.Contents,
		})
	}
	return plan, nil
}

func filterClusterInstallComponents(components []selectedComponent) []selectedComponent {
	filtered := components[:0]
	for _, component := range components {
		if strings.HasPrefix(component.Slot, "bootstrap") || strings.HasPrefix(component.Slot, "extension") {
			continue
		}
		filtered = append(filtered, component)
	}
	return filtered
}

type selectedComponent struct {
	Slot     string
	Kind     string
	Name     string
	Version  string
	Required bool
}

func selectSupportPolicy(policy *SupportPolicy, req ResolveRequest) (KubernetesSupportPolicy, error) {
	for _, supportPolicy := range policy.Spec.Policies {
		if !matchKubernetesVersion(req.KubernetesVersion, supportPolicy.Match.KubernetesVersion) {
			continue
		}
		if supportPolicy.Match.KubeClipperVersion != "" && req.KubeClipperVersion != "" && supportPolicy.Match.KubeClipperVersion != req.KubeClipperVersion {
			continue
		}
		return supportPolicy, nil
	}
	return KubernetesSupportPolicy{}, &ResolverError{Code: ErrUnsupportedKubernetesVersion, Message: fmt.Sprintf("no policy matched kubernetes version %q", req.KubernetesVersion)}
}

func selectComponents(policy KubernetesSupportPolicy, req ResolveRequest) ([]selectedComponent, error) {
	slotMap := make(map[string]ComponentSlotRule, len(policy.ComponentSlots))
	for _, slot := range policy.ComponentSlots {
		slotMap[slot.Slot] = slot
	}
	for slotName := range req.Components {
		if _, ok := slotMap[slotName]; !ok {
			return nil, &ResolverError{Code: ErrUnsupportedComponentSlot, Message: fmt.Sprintf("slot %q is not declared", slotName)}
		}
	}
	var selected []selectedComponent
	for _, slot := range policy.ComponentSlots {
		choice, requested := req.Components[slot.Slot]
		if !requested {
			if slot.Required || slot.Default.Name != "" {
				choice = slot.Default
			} else {
				continue
			}
		}
		option, err := optionForChoice(slot, choice)
		if err != nil {
			return nil, err
		}
		selected = append(selected, selectedComponent{
			Slot:     slot.Slot,
			Kind:     option.Kind,
			Name:     choice.Name,
			Version:  choice.Version,
			Required: slot.Required,
		})
	}
	return selected, nil
}

func validateSelectedComponentIdentities(components []selectedComponent) error {
	seen := make(map[string]string, len(components))
	for _, component := range components {
		key := relationKey(component.Kind, component.Name)
		if slot, ok := seen[key]; ok {
			return &ResolverError{
				Code:    ErrDuplicateResolvedComponent,
				Message: fmt.Sprintf("component %s selected by slots %q and %q", key, slot, component.Slot),
			}
		}
		seen[key] = component.Slot
	}
	return nil
}

func optionForChoice(slot ComponentSlotRule, choice ComponentChoice) (ComponentOption, error) {
	for _, option := range slot.Options {
		if option.Name != choice.Name {
			continue
		}
		if !hasString(option.AllowedVersions, choice.Version) {
			return ComponentOption{}, &ResolverError{Code: ErrUnsupportedComponentVersion, Message: fmt.Sprintf("slot %q option %q does not allow version %q", slot.Slot, choice.Name, choice.Version)}
		}
		return option, nil
	}
	return ComponentOption{}, &ResolverError{Code: ErrUnsupportedComponentChoice, Message: fmt.Sprintf("slot %q does not contain option %q", slot.Slot, choice.Name)}
}

func resolvePackage(inventory *PackageInventory, req ResolveRequest, selected selectedComponent) (PackageEntry, error) {
	report, err := analyzePackages(inventory, MatchPackageRequest{
		Candidates: []PackageCandidate{
			{Kind: selected.Kind, Name: selected.Name},
		},
		Version: selected.Version,
		OS:      req.OS,
		Arch:    req.Arch,
	})
	if err != nil {
		return PackageEntry{}, err
	}
	if len(report.Matches) > 0 {
		return preferResolvedPackage(report.Matches), nil
	}
	if report.FoundOtherArch {
		return PackageEntry{}, &ResolverError{Code: ErrArtifactArchUnavailable, Message: fmt.Sprintf("artifact %s/%s:%s is not available for arch %q", selected.Kind, selected.Name, selected.Version, req.Arch)}
	}
	return PackageEntry{}, &ResolverError{Code: ErrArtifactNotPublished, Message: fmt.Sprintf("artifact %s/%s:%s is not published", selected.Kind, selected.Name, selected.Version)}
}

func preferResolvedPackage(matches []PackageEntry) PackageEntry {
	if len(matches) == 1 {
		return matches[0]
	}
	selected := matches[0]
	for _, pkg := range matches[1:] {
		if resolvedPackagePreference(pkg) > resolvedPackagePreference(selected) {
			selected = pkg
		}
	}
	return selected
}

func resolvedPackagePreference(pkg PackageEntry) int {
	score := 0
	if pkg.Transport.Type == TransportOCI {
		score += 20
	}
	return score
}

func validateRelations(relations []VersionRelation, selected []selectedComponent) error {
	selectedMap := make(map[string]selectedComponent, len(selected))
	for _, component := range selected {
		selectedMap[relationKey(component.Kind, component.Name)] = component
	}
	for _, relation := range relations {
		when, ok := selectedMap[relationKey(relation.When.Kind, relation.When.Name)]
		if !ok || !selectorMatchesSelected(relation.When, when) {
			continue
		}
		for _, required := range relation.Requires {
			component, ok := selectedMap[relationKey(required.Kind, required.Name)]
			if !ok || !selectorMatchesSelected(required, component) {
				return &ResolverError{Code: ErrComponentConstraintViolation, Message: fmt.Sprintf("%s requires %s", selectorString(relation.When), selectorString(required))}
			}
		}
		for _, forbidden := range relation.Forbids {
			component, ok := selectedMap[relationKey(forbidden.Kind, forbidden.Name)]
			if ok && selectorMatchesSelected(forbidden, component) {
				return &ResolverError{Code: ErrComponentConstraintViolation, Message: fmt.Sprintf("%s forbids %s", selectorString(relation.When), selectorString(forbidden))}
			}
		}
	}
	return nil
}

func relationKey(kind, name string) string {
	return kind + "/" + name
}

func selectorMatchesSelected(selector RelationSelector, component selectedComponent) bool {
	if selector.Kind != "" && selector.Kind != component.Kind {
		return false
	}
	if selector.Name != "" && selector.Name != component.Name {
		return false
	}
	if selector.Version != "" && selector.Version != component.Version {
		return false
	}
	if len(selector.VersionIn) > 0 && !hasString(selector.VersionIn, component.Version) {
		return false
	}
	return true
}

func selectorString(selector RelationSelector) string {
	versions := append([]string(nil), selector.VersionIn...)
	sort.Strings(versions)
	parts := []string{selector.Kind, selector.Name}
	if selector.Version != "" {
		parts = append(parts, selector.Version)
	}
	if len(versions) > 0 {
		parts = append(parts, strings.Join(versions, ","))
	}
	return strings.Join(parts, "/")
}
