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

type BootstrapBinaryResolveRequest struct {
	OS                 string
	Arch               string
	KubernetesVersion  string
	KubeClipperVersion string
	Version            string
	Candidates         []PackageCandidate
	Contents           []string
}

func ResolveBootstrapBinary(inventory *PackageInventory, policy *SupportPolicy, req BootstrapBinaryResolveRequest) (ResolvedComponent, error) {
	if len(req.Candidates) == 0 {
		return ResolvedComponent{}, fmt.Errorf("bootstrap binary resolve candidates are required")
	}
	component, err := resolvePolicyPackage(inventory, policy, policyPackageResolveRequest{
		SlotPrefix:         "bootstrap",
		OS:                 req.OS,
		Arch:               req.Arch,
		KubernetesVersion:  req.KubernetesVersion,
		KubeClipperVersion: req.KubeClipperVersion,
		Version:            req.Version,
		Candidates:         req.Candidates,
	})
	if err != nil {
		return ResolvedComponent{}, err
	}
	if len(req.Contents) == 0 {
		return component, nil
	}
	contents := make([]ArtifactContent, 0, len(req.Contents))
	contentSet := make(map[string]ArtifactContent, len(component.Contents))
	for _, content := range component.Contents {
		contentSet[content.Name] = content
	}
	for _, name := range req.Contents {
		content, ok := contentSet[name]
		if !ok {
			return ResolvedComponent{}, fmt.Errorf("bootstrap package %s/%s:%s missing content %q", component.Kind, component.Name, component.Version, name)
		}
		contents = append(contents, content)
	}
	component.Contents = contents
	return component, nil
}
