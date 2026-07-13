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
	"regexp"
	"sort"
	"strings"
)

var digestRegexp = regexp.MustCompile(`^sha256:[a-fA-F0-9]{64}$`)

func NewPackageInventory(name string) *PackageInventory {
	return &PackageInventory{
		APIVersion: APIVersion,
		Kind:       KindPackageInventory,
		Metadata: ObjectMeta{
			Name: name,
		},
	}
}

func NewSupportPolicy(name string) *SupportPolicy {
	return &SupportPolicy{
		APIVersion: APIVersion,
		Kind:       KindSupportPolicy,
		Metadata: ObjectMeta{
			Name: name,
		},
	}
}

func (c *PackageInventory) Validate() error {
	if c == nil {
		return fmt.Errorf("package inventory is nil")
	}
	seen := make(map[string]struct{}, len(c.Spec.Packages))
	for i := range c.Spec.Packages {
		pkg := c.Spec.Packages[i]
		key := PackageKey(pkg)
		if key == "" {
			return fmt.Errorf("package[%d] identity is incomplete", i)
		}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("duplicate package %q", key)
		}
		seen[key] = struct{}{}
		if err := validatePackageEntry(pkg); err != nil {
			return fmt.Errorf("package[%d] %s/%s/%s/%s/%s: %w", i, pkg.Kind, pkg.Name, pkg.Version, packageOS(pkg.OS), pkg.Arch, err)
		}
	}
	return nil
}

func PackageKey(pkg PackageEntry) string {
	if pkg.Kind == "" || pkg.Name == "" || pkg.Version == "" || pkg.Arch == "" {
		return ""
	}
	return fmt.Sprintf("%s|%s|%s|%s|%s", pkg.Kind, pkg.Name, pkg.Version, packageOS(pkg.OS), pkg.Arch)
}

func packageOS(os string) string {
	if strings.TrimSpace(os) == "" {
		return DefaultPackageOS
	}
	return os
}

func validatePackageEntry(pkg PackageEntry) error {
	if err := validatePackageVersion(pkg.Version); err != nil && !isMutableKubeClipperBootstrap(pkg) {
		return err
	}
	if pkg.Transport.Type == "" {
		return fmt.Errorf("transport type is required")
	}
	if pkg.Transport.Type != TransportOCI {
		return fmt.Errorf("unsupported transport type %q", pkg.Transport.Type)
	}
	if pkg.Transport.Ref == "" {
		return fmt.Errorf("transport ref is required")
	}
	if !digestRegexp.MatchString(pkg.Transport.Digest) {
		return fmt.Errorf("oci transport digest must be sha256:<64 hex>")
	}
	if strings.Contains(pkg.Transport.Ref, "@") {
		_, digest, _ := strings.Cut(pkg.Transport.Ref, "@")
		if digest != pkg.Transport.Digest {
			return fmt.Errorf("oci transport ref digest %q must match digest %q", digest, pkg.Transport.Digest)
		}
	}
	return validateContentProfile(pkg.ContentProfile, pkg.Contents)
}

func isMutableKubeClipperBootstrap(pkg PackageEntry) bool {
	return pkg.Kind == "bootstrap" && pkg.Name == "kubeclipper" && strings.EqualFold(strings.TrimSpace(pkg.Version), "latest")
}

func validatePackageVersion(version string) error {
	if strings.EqualFold(strings.TrimSpace(version), "latest") {
		return fmt.Errorf("version %q is not supported", version)
	}
	return nil
}

func validateContentProfile(profile string, contents []ArtifactContent) error {
	if profile == "" {
		return nil
	}
	if profile == ContentProfileBinary && len(contents) == 0 {
		return fmt.Errorf("content profile %q requires at least one content", profile)
	}
	contentSet := make(map[string]struct{}, len(contents))
	for _, content := range contents {
		if content.Name == "" {
			return fmt.Errorf("content name is required")
		}
		if content.Name == "images" || content.File == "images.tar.gz" {
			return fmt.Errorf("embedded runtime image archives are not supported; publish runtime images as standard images")
		}
		if _, ok := contentSet[content.Name]; ok {
			return fmt.Errorf("duplicate content %q", content.Name)
		}
		if content.File == "" {
			return fmt.Errorf("content %q file is required", content.Name)
		}
		if content.Digest != "" && !digestRegexp.MatchString(content.Digest) {
			return fmt.Errorf("content %q digest must be sha256:<64 hex>", content.Name)
		}
		if err := validateContentTransport(content); err != nil {
			return err
		}
		if !contentAllowedForProfile(profile, content.Name) {
			return fmt.Errorf("content profile %q does not support %q", profile, content.Name)
		}
		contentSet[content.Name] = struct{}{}
	}
	for _, required := range requiredContents(profile) {
		if _, ok := contentSet[required]; !ok {
			return fmt.Errorf("content profile %q requires %q", profile, required)
		}
	}
	if profile == ContentProfileAddon {
		if _, hasConfigs := contentSet[ContentConfigs]; hasConfigs {
			return nil
		}
		if _, hasCharts := contentSet[ContentCharts]; hasCharts {
			return nil
		}
		return fmt.Errorf("content profile %q requires %q or %q", profile, ContentConfigs, ContentCharts)
	}
	return nil
}

func contentAllowedForProfile(profile, name string) bool {
	switch profile {
	case ContentProfileK8s, ContentProfileRuntime, ContentProfileExtension:
		return name == ContentConfigs
	case ContentProfileAddon:
		return name == ContentConfigs || name == ContentCharts
	case ContentProfileBinary:
		return name != ""
	default:
		return false
	}
}

func validateContentTransport(content ArtifactContent) error {
	if content.Transport.Type == "" {
		return nil
	}
	switch content.Transport.Type {
	case TransportOCI, TransportHelmOCI:
	default:
		return fmt.Errorf("content %q unsupported transport type %q", content.Name, content.Transport.Type)
	}
	if content.Transport.Ref == "" {
		return fmt.Errorf("content %q transport ref is required", content.Name)
	}
	if !digestRegexp.MatchString(content.Transport.Digest) {
		return fmt.Errorf("content %q transport digest must be sha256:<64 hex>", content.Name)
	}
	return nil
}

func requiredContents(profile string) []string {
	switch profile {
	case ContentProfileK8s:
		return []string{ContentConfigs}
	case ContentProfileRuntime:
		return []string{ContentConfigs}
	case ContentProfileAddon:
		return nil
	case ContentProfileExtension:
		return []string{ContentConfigs}
	case ContentProfileBinary:
		return nil
	default:
		return []string{"<unknown-profile:" + profile + ">"}
	}
}

func (p *SupportPolicy) Validate() error {
	if p == nil {
		return fmt.Errorf("support policy is nil")
	}
	if p.APIVersion != "" && p.APIVersion != APIVersion {
		return fmt.Errorf("support policy apiVersion must be %q", APIVersion)
	}
	if p.Kind != "" && p.Kind != KindSupportPolicy {
		return fmt.Errorf("support policy kind must be %q", KindSupportPolicy)
	}
	seenPolicies := make(map[string]struct{}, len(p.Spec.Policies))
	for i, policy := range p.Spec.Policies {
		if policy.Name == "" {
			return fmt.Errorf("policy[%d] name is required", i)
		}
		if _, ok := seenPolicies[policy.Name]; ok {
			return fmt.Errorf("duplicate policy %q", policy.Name)
		}
		seenPolicies[policy.Name] = struct{}{}
		if policy.Match.KubernetesVersion == "" {
			return fmt.Errorf("policy[%s] kubernetesVersion is required", policy.Name)
		}
		if err := validateKubernetesVersionPattern(policy.Match.KubernetesVersion); err != nil {
			return fmt.Errorf("policy[%s] kubernetesVersion: %w", policy.Name, err)
		}
		for j := 0; j < i; j++ {
			if policyMatchesOverlap(p.Spec.Policies[j].Match, policy.Match) {
				return fmt.Errorf("policy[%s] match overlaps policy[%s]", policy.Name, p.Spec.Policies[j].Name)
			}
		}
		seenSlots := make(map[string]struct{}, len(policy.ComponentSlots))
		for j, slot := range policy.ComponentSlots {
			if err := validateComponentSlot(slot); err != nil {
				return fmt.Errorf("policy[%s].componentSlots[%d]: %w", policy.Name, j, err)
			}
			if _, ok := seenSlots[slot.Slot]; ok {
				return fmt.Errorf("policy[%s] duplicate component slot %q", policy.Name, slot.Slot)
			}
			seenSlots[slot.Slot] = struct{}{}
		}
	}
	return nil
}

func validateKubernetesVersionPattern(pattern string) error {
	if strings.Count(pattern, "*") > 1 {
		return fmt.Errorf("wildcard is only supported as a single trailing *")
	}
	if strings.Contains(pattern, "*") && !strings.HasSuffix(pattern, "*") {
		return fmt.Errorf("wildcard is only supported as a trailing *")
	}
	return nil
}

func policyMatchesOverlap(left, right PolicyMatch) bool {
	return kubernetesVersionPatternsOverlap(left.KubernetesVersion, right.KubernetesVersion) &&
		kubeClipperVersionsOverlap(left.KubeClipperVersion, right.KubeClipperVersion)
}

func kubernetesVersionPatternsOverlap(left, right string) bool {
	leftPrefix, leftWildcard := versionPatternPrefix(left)
	rightPrefix, rightWildcard := versionPatternPrefix(right)
	switch {
	case leftWildcard && rightWildcard:
		return strings.HasPrefix(leftPrefix, rightPrefix) || strings.HasPrefix(rightPrefix, leftPrefix)
	case leftWildcard:
		return strings.HasPrefix(right, leftPrefix)
	case rightWildcard:
		return strings.HasPrefix(left, rightPrefix)
	default:
		return left == right
	}
}

func versionPatternPrefix(pattern string) (string, bool) {
	if strings.HasSuffix(pattern, "*") {
		return strings.TrimSuffix(pattern, "*"), true
	}
	return pattern, false
}

func kubeClipperVersionsOverlap(left, right string) bool {
	return left == "" || right == "" || left == right
}

func validateComponentSlot(slot ComponentSlotRule) error {
	if slot.Slot == "" {
		return fmt.Errorf("slot is required")
	}
	if !validSelection(slot.Selection) {
		return fmt.Errorf("unsupported selection %q", slot.Selection)
	}
	if slot.Selection == SelectionZeroOrOne && slot.Required {
		return fmt.Errorf("zeroOrOne slot cannot be required")
	}
	if len(slot.Options) == 0 {
		return fmt.Errorf("options are required")
	}
	optionMap := make(map[string]ComponentOption, len(slot.Options))
	for _, option := range slot.Options {
		if option.Name == "" || option.Kind == "" {
			return fmt.Errorf("option name and kind are required")
		}
		if _, ok := optionMap[option.Name]; ok {
			return fmt.Errorf("duplicate option %q", option.Name)
		}
		if len(option.AllowedVersions) == 0 {
			return fmt.Errorf("option %q allowedVersions are required", option.Name)
		}
		seenVersions := make(map[string]struct{}, len(option.AllowedVersions))
		for _, version := range option.AllowedVersions {
			if version == "" {
				return fmt.Errorf("option %q allowedVersions cannot contain empty version", option.Name)
			}
			if err := validatePackageVersion(version); err != nil {
				return fmt.Errorf("option %q allowedVersion: %w", option.Name, err)
			}
			if _, ok := seenVersions[version]; ok {
				return fmt.Errorf("option %q duplicate allowedVersion %q", option.Name, version)
			}
			seenVersions[version] = struct{}{}
		}
		optionMap[option.Name] = option
	}
	if slot.Required && (slot.Default.Name == "" || slot.Default.Version == "") {
		return fmt.Errorf("required slot must have default choice")
	}
	if slot.Default.Name != "" || slot.Default.Version != "" {
		option, ok := optionMap[slot.Default.Name]
		if !ok {
			return fmt.Errorf("default option %q is not declared", slot.Default.Name)
		}
		if !hasString(option.AllowedVersions, slot.Default.Version) {
			allowed := append([]string(nil), option.AllowedVersions...)
			sort.Strings(allowed)
			return fmt.Errorf("default version %q is not allowed for option %q, allowed: %s", slot.Default.Version, slot.Default.Name, strings.Join(allowed, ","))
		}
	}
	return nil
}

func validSelection(selection string) bool {
	switch selection {
	case SelectionOneOf, SelectionZeroOrOne:
		return true
	default:
		return false
	}
}

func hasString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
