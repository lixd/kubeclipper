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
}

func ResolveBootstrapBinary(inventory *PackageInventory, policy *SupportPolicy, req BootstrapBinaryResolveRequest) (ResolvedComponent, error) {
	if len(req.Candidates) == 0 {
		return ResolvedComponent{}, fmt.Errorf("bootstrap binary resolve candidates are required")
	}
	return resolvePolicyPackage(inventory, policy, policyPackageResolveRequest{
		SlotPrefix:         "bootstrap",
		OS:                 req.OS,
		Arch:               req.Arch,
		KubernetesVersion:  req.KubernetesVersion,
		KubeClipperVersion: req.KubeClipperVersion,
		Version:            req.Version,
		Candidates:         req.Candidates,
	})
}
