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

const (
	APIVersion = "delivery.kubeclipper.io/v1alpha1"

	KindPackageInventory = "PackageInventory"
	KindSupportPolicy    = "SupportPolicy"

	TransportOCI     = "oci"
	TransportHelmOCI = "helm-oci"

	DefaultPackageOS = "linux"

	ContentProfileK8s       = "k8s"
	ContentProfileRuntime   = "runtime"
	ContentProfileAddon     = "addon"
	ContentProfileExtension = "extension"
	ContentProfileBinary    = "binary"

	ContentConfigs = "configs"
	ContentCharts  = "charts"
	ContentBinary  = "binary"

	MediaTypeConfigsLayer   = "application/vnd.kubeclipper.configs.layer.v1.tar+gzip"
	MediaTypeChartsLayer    = "application/vnd.kubeclipper.charts.layer.v1.tgz"
	MediaTypeBinaryLayer    = "application/vnd.kubeclipper.binary.layer.v1"
	MediaTypeHelmConfig     = "application/vnd.cncf.helm.config.v1+json"
	MediaTypeHelmChartLayer = "application/vnd.cncf.helm.chart.content.v1.tar+gzip"

	SelectionOneOf     = "oneOf"
	SelectionZeroOrOne = "zeroOrOne"
	SelectionMany      = "many"
)

type ObjectMeta struct {
	Name string `json:"name"`
}

type PackageInventory struct {
	APIVersion string               `json:"apiVersion"`
	Kind       string               `json:"kind"`
	Metadata   ObjectMeta           `json:"metadata"`
	Spec       PackageInventorySpec `json:"spec"`
}

type PackageInventorySpec struct {
	Registry string         `json:"registry,omitempty"`
	Packages []PackageEntry `json:"packages"`
}

type PackageEntry struct {
	Name           string            `json:"name"`
	Kind           string            `json:"kind"`
	Version        string            `json:"version"`
	SourceRevision string            `json:"sourceRevision,omitempty"`
	OS             string            `json:"os,omitempty"`
	Arch           string            `json:"arch"`
	ContentProfile string            `json:"contentProfile,omitempty"`
	Transport      TransportRef      `json:"transport"`
	Contents       []ArtifactContent `json:"contents"`
}

type TransportRef struct {
	Type   string `json:"type"`
	Ref    string `json:"ref"`
	Digest string `json:"digest"`
}

type ArtifactContent struct {
	Name      string       `json:"name"`
	File      string       `json:"file"`
	Digest    string       `json:"digest,omitempty"`
	MediaType string       `json:"mediaType,omitempty"`
	Transport TransportRef `json:"transport,omitempty"`
}

type SupportPolicy struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Metadata   ObjectMeta        `json:"metadata"`
	Spec       SupportPolicySpec `json:"spec"`
}

type SupportPolicySpec struct {
	Policies []KubernetesSupportPolicy `json:"policies"`
}

type KubernetesSupportPolicy struct {
	Name           string              `json:"name"`
	Match          PolicyMatch         `json:"match"`
	ComponentSlots []ComponentSlotRule `json:"componentSlots"`
	Constraints    []VersionRelation   `json:"constraints,omitempty"`
}

type PolicyMatch struct {
	KubeClipperVersion string `json:"kubeclipperVersion,omitempty"`
	KubernetesVersion  string `json:"kubernetesVersion"`
}

type ComponentSlotRule struct {
	Slot      string            `json:"slot"`
	Selection string            `json:"selection"`
	Required  bool              `json:"required"`
	Default   ComponentChoice   `json:"default,omitempty"`
	Options   []ComponentOption `json:"options"`
}

type ComponentChoice struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type ComponentOption struct {
	Name            string   `json:"name"`
	Kind            string   `json:"kind"`
	AllowedVersions []string `json:"allowedVersions"`
}

type VersionRelation struct {
	When     RelationSelector   `json:"when"`
	Requires []RelationSelector `json:"requires,omitempty"`
	Forbids  []RelationSelector `json:"forbids,omitempty"`
}

type RelationSelector struct {
	Kind      string   `json:"kind"`
	Name      string   `json:"name"`
	Version   string   `json:"version,omitempty"`
	VersionIn []string `json:"versionIn,omitempty"`
}

type ResolveRequest struct {
	KubernetesVersion  string                     `json:"kubernetesVersion"`
	OS                 string                     `json:"os,omitempty"`
	Arch               string                     `json:"arch"`
	KubeClipperVersion string                     `json:"kubeclipperVersion,omitempty"`
	Components         map[string]ComponentChoice `json:"components,omitempty"`
}

type ResolvedArtifactPlan struct {
	KubernetesVersion string              `json:"kubernetesVersion"`
	OS                string              `json:"os,omitempty"`
	Arch              string              `json:"arch"`
	Components        []ResolvedComponent `json:"components"`
}

type ResolvedComponent struct {
	Slot      string            `json:"slot"`
	Kind      string            `json:"kind"`
	Name      string            `json:"name"`
	Version   string            `json:"version"`
	OS        string            `json:"os,omitempty"`
	Arch      string            `json:"arch,omitempty"`
	Required  bool              `json:"required"`
	Transport TransportRef      `json:"transport"`
	Contents  []ArtifactContent `json:"contents"`
}
