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

type PackageCandidate struct {
	Kind string
	Name string
}

type SelectPackageRequest struct {
	Candidates []PackageCandidate
	Version    string
	OS         string
	Arch       string
}

type MatchPackageRequest struct {
	Candidates []PackageCandidate
	Version    string
	OS         string
	Arch       string
}

type packageMatchReport struct {
	Matches        []PackageEntry
	FoundOtherArch bool
}

func SelectBestPackage(inventory *PackageInventory, req SelectPackageRequest) (PackageEntry, error) {
	if err := inventory.Validate(); err != nil {
		return PackageEntry{}, err
	}
	report, err := analyzePackages(inventory, MatchPackageRequest{
		Candidates: req.Candidates,
		Version:    req.Version,
		OS:         req.OS,
		Arch:       req.Arch,
	})
	if err != nil {
		return PackageEntry{}, err
	}

	var (
		selected PackageEntry
		best     = -1
		found    bool
	)
	for _, pkg := range report.Matches {
		score := packagePreferenceScore(pkg)
		if !found || score > best {
			selected = pkg
			best = score
			found = true
		}
	}
	if !found {
		return PackageEntry{}, &ResolverError{
			Code:    ErrArtifactNotPublished,
			Message: fmt.Sprintf("matching package is not published for arch %q", req.Arch),
		}
	}
	return selected, nil
}

func MatchUniquePackage(inventory *PackageInventory, req MatchPackageRequest) (PackageEntry, error) {
	if err := inventory.Validate(); err != nil {
		return PackageEntry{}, err
	}
	report, err := analyzePackages(inventory, req)
	if err != nil {
		return PackageEntry{}, err
	}
	matches := report.Matches
	if len(matches) == 0 {
		return PackageEntry{}, &ResolverError{
			Code:    ErrArtifactNotPublished,
			Message: fmt.Sprintf("matching package is not published for arch %q", req.Arch),
		}
	}
	if len(matches) > 1 {
		return PackageEntry{}, fmt.Errorf("multiple matching packages found")
	}
	return matches[0], nil
}

func analyzePackages(inventory *PackageInventory, req MatchPackageRequest) (packageMatchReport, error) {
	if inventory == nil {
		return packageMatchReport{}, fmt.Errorf("inventory is nil")
	}
	if len(req.Candidates) == 0 {
		return packageMatchReport{}, fmt.Errorf("package candidates are required")
	}
	report := packageMatchReport{}
	reqOS := packageOS(req.OS)
	for _, pkg := range inventory.Spec.Packages {
		if !matchesPackageCandidate(pkg, req.Candidates) {
			continue
		}
		if req.Version != "" && pkg.Version != req.Version {
			continue
		}
		if packageOS(pkg.OS) != reqOS {
			continue
		}
		if req.Arch != "" && pkg.Arch != req.Arch {
			report.FoundOtherArch = true
			continue
		}
		report.Matches = append(report.Matches, pkg)
	}
	return report, nil
}

func matchesPackageCandidate(pkg PackageEntry, candidates []PackageCandidate) bool {
	for _, candidate := range candidates {
		if pkg.Name != candidate.Name {
			continue
		}
		if candidate.Kind == "" || pkg.Kind == candidate.Kind {
			return true
		}
	}
	return false
}

func packagePreferenceScore(pkg PackageEntry) int {
	score := 0
	if pkg.Transport.Type == TransportOCI {
		score += 20
	}
	return score
}
