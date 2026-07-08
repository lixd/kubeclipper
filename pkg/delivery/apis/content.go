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

package apis

import (
	"strconv"
	"strings"
)

func ContentProfileForKind(kind string) string {
	switch kind {
	case "k8s":
		return ContentProfileK8s
	case "cri":
		return ContentProfileRuntime
	case "binary", "bootstrap":
		return ContentProfileBinary
	case "extension", "k8s-extension":
		return ContentProfileExtension
	case "cni", "csi", "app":
		return ContentProfileAddon
	default:
		return ContentProfileAddon
	}
}

func ContentsForProfile(profile string) []ArtifactContent {
	if profile == ContentProfileAddon {
		return []ArtifactContent{{
			Name:      ContentCharts,
			File:      ContentFile(ContentCharts),
			Digest:    "",
			MediaType: MediaTypeForContent(ContentCharts),
		}}
	}
	var contents []ArtifactContent
	for _, name := range requiredContents(profile) {
		contents = append(contents, ArtifactContent{
			Name:      name,
			File:      ContentFile(name),
			Digest:    "",
			MediaType: MediaTypeForContent(name),
		})
	}
	return contents
}

func ContentFile(name string) string {
	switch name {
	case ContentConfigs:
		return "configs.tar.gz"
	case ContentImages:
		return "images.tar.gz"
	case ContentCharts:
		return "charts.tgz"
	case ContentBinary:
		return "binary"
	default:
		return name
	}
}

func MediaTypeForContent(name string) string {
	switch name {
	case ContentConfigs:
		return MediaTypeConfigsLayer
	case ContentImages:
		return MediaTypeImagesLayer
	case ContentCharts:
		return MediaTypeChartsLayer
	case ContentBinary:
		return MediaTypeBinaryLayer
	default:
		return ""
	}
}

func CompareVersions(left, right string) (int, bool) {
	leftParts, ok := parseVersionParts(left)
	if !ok {
		return 0, false
	}
	rightParts, ok := parseVersionParts(right)
	if !ok {
		return 0, false
	}
	maxLen := len(leftParts)
	if len(rightParts) > maxLen {
		maxLen = len(rightParts)
	}
	for i := 0; i < maxLen; i++ {
		var l, r int
		if i < len(leftParts) {
			l = leftParts[i]
		}
		if i < len(rightParts) {
			r = rightParts[i]
		}
		if l < r {
			return -1, true
		}
		if l > r {
			return 1, true
		}
	}
	return 0, true
}

func parseVersionParts(version string) ([]int, bool) {
	version = strings.TrimPrefix(version, "v")
	version = strings.TrimPrefix(version, "V")
	version = strings.Split(version, "-")[0]
	version = strings.Split(version, "+")[0]
	if version == "" {
		return nil, false
	}
	parts := strings.Split(version, ".")
	nums := make([]int, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			return nil, false
		}
		n, err := strconv.Atoi(part)
		if err != nil {
			return nil, false
		}
		nums = append(nums, n)
	}
	return nums, true
}
