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

import "fmt"

type ExtensionCandidate struct {
	Kind string
	Name string
}

type ExtensionResolveRequest struct {
	OS                 string
	Arch               string
	KubernetesVersion  string
	KubeClipperVersion string
	Version            string
	Candidates         []ExtensionCandidate
}

func ResolveExtensionArtifact(inventory *PackageInventory, policy *SupportPolicy, req ExtensionResolveRequest) (ResolvedComponent, error) {
	if len(req.Candidates) == 0 {
		return ResolvedComponent{}, fmt.Errorf("extension resolve candidates are required")
	}
	return resolvePolicyPackage(inventory, policy, policyPackageResolveRequest{
		SlotPrefix:         "extension",
		OS:                 req.OS,
		Arch:               req.Arch,
		KubernetesVersion:  req.KubernetesVersion,
		KubeClipperVersion: req.KubeClipperVersion,
		Version:            req.Version,
		Candidates:         extensionCandidatesToPackages(req.Candidates),
	})
}

func extensionCandidatesToPackages(candidates []ExtensionCandidate) []PackageCandidate {
	out := make([]PackageCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		out = append(out, PackageCandidate{Kind: candidate.Kind, Name: candidate.Name})
	}
	return out
}
