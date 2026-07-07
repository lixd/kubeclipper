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

package apis

import (
	"fmt"
	"strings"
)

type policyPackageResolveRequest struct {
	SlotPrefix         string
	OS                 string
	Arch               string
	KubernetesVersion  string
	KubeClipperVersion string
	Version            string
	Candidates         []PackageCandidate
	Explicit           bool
}

type PolicyPackageResolveRequest struct {
	SlotPrefix         string
	OS                 string
	Arch               string
	KubernetesVersion  string
	KubeClipperVersion string
	Kind               string
	Name               string
	Version            string
}

func ResolvePolicyPackage(inventory *PackageInventory, policy *SupportPolicy, req PolicyPackageResolveRequest) (ResolvedComponent, error) {
	return resolvePolicyPackage(inventory, policy, policyPackageResolveRequest{
		SlotPrefix:         req.SlotPrefix,
		OS:                 req.OS,
		Arch:               req.Arch,
		KubernetesVersion:  req.KubernetesVersion,
		KubeClipperVersion: req.KubeClipperVersion,
		Version:            req.Version,
		Candidates: []PackageCandidate{
			{Kind: req.Kind, Name: req.Name},
		},
		Explicit: true,
	})
}

func resolvePolicyPackage(inventory *PackageInventory, policy *SupportPolicy, req policyPackageResolveRequest) (ResolvedComponent, error) {
	if err := inventory.Validate(); err != nil {
		return ResolvedComponent{}, err
	}
	if err := policy.Validate(); err != nil {
		return ResolvedComponent{}, err
	}
	if req.KubernetesVersion == "" {
		return ResolvedComponent{}, &ResolverError{Code: ErrUnsupportedKubernetesVersion, Message: "kubernetesVersion is required"}
	}
	if req.Arch == "" {
		return ResolvedComponent{}, &ResolverError{Code: ErrArtifactArchUnavailable, Message: "arch is required"}
	}
	supportPolicy, err := selectSupportPolicy(policy, ResolveRequest{
		KubernetesVersion:  req.KubernetesVersion,
		KubeClipperVersion: req.KubeClipperVersion,
	})
	if err != nil {
		return ResolvedComponent{}, err
	}
	component, err := selectPolicyPackageComponent(supportPolicy, req)
	if err != nil {
		return ResolvedComponent{}, err
	}
	pkg, err := resolvePackage(inventory, ResolveRequest{OS: req.OS, Arch: req.Arch}, component)
	if err != nil {
		return ResolvedComponent{}, err
	}
	return ResolvedComponent{
		Slot:      component.Slot,
		Kind:      component.Kind,
		Name:      component.Name,
		Version:   component.Version,
		OS:        pkg.OS,
		Arch:      pkg.Arch,
		Required:  component.Required,
		Transport: pkg.Transport,
		Contents:  pkg.Contents,
	}, nil
}

func selectPolicyPackageComponent(policy KubernetesSupportPolicy, req policyPackageResolveRequest) (selectedComponent, error) {
	if req.Explicit {
		return selectExplicitPolicyPackageComponent(policy, req)
	}
	components, err := selectComponents(policy, ResolveRequest{})
	if err != nil {
		return selectedComponent{}, err
	}
	var matches []selectedComponent
	for _, component := range components {
		if req.SlotPrefix != "" && component.Slot != req.SlotPrefix && !strings.HasPrefix(component.Slot, req.SlotPrefix+"-") {
			continue
		}
		if req.Version != "" && component.Version != req.Version {
			continue
		}
		if !matchesPackageCandidate(PackageEntry{Kind: component.Kind, Name: component.Name}, req.Candidates) {
			continue
		}
		matches = append(matches, component)
	}
	if len(matches) == 0 {
		return selectedComponent{}, &ResolverError{Code: ErrUnsupportedComponentChoice, Message: fmt.Sprintf("policy does not select a %s package matching requested candidates", req.SlotPrefix)}
	}
	if len(matches) > 1 {
		return selectedComponent{}, fmt.Errorf("policy selects multiple %s packages matching requested candidates", req.SlotPrefix)
	}
	return matches[0], nil
}

func selectExplicitPolicyPackageComponent(policy KubernetesSupportPolicy, req policyPackageResolveRequest) (selectedComponent, error) {
	if req.Version == "" {
		return selectedComponent{}, &ResolverError{Code: ErrUnsupportedComponentVersion, Message: "explicit policy package version is required"}
	}
	var matches []selectedComponent
	for _, slot := range policy.ComponentSlots {
		if req.SlotPrefix != "" && slot.Slot != req.SlotPrefix && !strings.HasPrefix(slot.Slot, req.SlotPrefix+"-") {
			continue
		}
		for _, option := range slot.Options {
			if !matchesPackageCandidate(PackageEntry{Kind: option.Kind, Name: option.Name}, req.Candidates) {
				continue
			}
			if !hasString(option.AllowedVersions, req.Version) {
				continue
			}
			matches = append(matches, selectedComponent{
				Slot:     slot.Slot,
				Kind:     option.Kind,
				Name:     option.Name,
				Version:  req.Version,
				Required: slot.Required,
			})
		}
	}
	if len(matches) == 0 {
		return selectedComponent{}, &ResolverError{Code: ErrUnsupportedComponentChoice, Message: fmt.Sprintf("policy does not allow %s package matching requested candidates", req.SlotPrefix)}
	}
	if len(matches) > 1 {
		return selectedComponent{}, fmt.Errorf("policy allows multiple %s packages matching requested candidates", req.SlotPrefix)
	}
	return matches[0], nil
}
