/*
 *
 *  * Copyright 2022 KubeClipper Authors.
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

package v1

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"

	"github.com/kubeclipper/kubeclipper/pkg/cli/printer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const DefaultImageRegistry = "registry.k8s.io"

// RegistrySeedConfigDigestAnnotation identifies the last deployed registry configuration.
const RegistrySeedConfigDigestAnnotation = "kubeclipper.io/package-registry-config-digest"

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:openapi-gen=false

type Registry struct {
	metav1.TypeMeta `json:",inline"`
	// Standard object's metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	RegistrySpec      `json:",inline"`
}

type RegistrySpec struct {
	// RepositoryPrefix is the optional image repository path beneath Host.
	RepositoryPrefix string        `json:"repositoryPrefix,omitempty"`
	Scheme           string        `json:"scheme,omitempty"`
	Host             string        `json:"host,omitempty"`
	SkipVerify       bool          `json:"skipVerify,omitempty"`
	CA               string        `json:"ca,omitempty"`
	RegistryAuth     *RegistryAuth `json:"auth,omitempty"`
}

// ImageRepository returns the image namespace, independently of the CRI endpoint.
func (r *RegistrySpec) ImageRepository() string {
	if r.RepositoryPrefix == "" {
		return r.Host
	}
	return r.Host + "/" + r.RepositoryPrefix
}

// ValidateImageRepository checks the endpoint and optional repository path without rewriting them.
func (r *RegistrySpec) ValidateImageRepository() error {
	if _, err := name.NewRegistry(r.Host, name.StrictValidation); err != nil {
		return fmt.Errorf("invalid registry host %q: %w", r.Host, err)
	}
	if r.RepositoryPrefix == "" {
		return nil
	}
	for part := range strings.SplitSeq(r.RepositoryPrefix, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid registry repository prefix %q", r.RepositoryPrefix)
		}
	}
	if _, err := name.NewRepository("registry.invalid/" + r.RepositoryPrefix + "/kc-validation"); err != nil {
		return fmt.Errorf("invalid registry repository prefix %q: %w", r.RepositoryPrefix, err)
	}
	return nil
}

// RegistryAuth holds credentials for authenticating with a container registry.
//
// TODO: registry.configs.*.auth is deprecated since containerd 2.1. The Transfer Service
// (default since containerd 2.1) does not support inline auth in config.toml and will
// automatically fall back to Local Pull Mode with a warning. hosts.toml has no auth field,
// and containerd recommends using Kubernetes ImagePullSecrets instead. There is no removal
// timeline yet — containerd says it won't be removed "until a suitable secret management
// alternative is available as a plugin". Once we migrate auth to the Kubernetes layer
// (ImagePullSecrets), this struct and all code paths that write registry.configs.*.auth
// should be removed.
// Ref: https://github.com/containerd/containerd/blob/main/docs/cri/registry.md
type RegistryAuth struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// RegistryList is a resource containing a list of RegistryList objects.
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type RegistryList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items is the list of registry.
	Items []Registry `json:"items"`
}

func (n *RegistryList) YAMLPrint() ([]byte, error) {
	if len(n.Items) == 1 {
		return printer.YAMLPrinter(n.Items[0])
	}
	return printer.YAMLPrinter(n)
}

func (n *RegistryList) TablePrint() ([]string, [][]string) {
	headers := []string{"name", "scheme", "host", "skip-tls-verify", "ca", "auth"}
	var data [][]string
	for _, registry := range n.Items {
		data = append(data, []string{registry.Name,
			registry.Scheme,
			registry.Host,
			strconv.FormatBool(registry.SkipVerify),
			strconv.FormatBool(registry.CA != ""),
			strconv.FormatBool(registry.RegistryAuth != nil),
		})
	}
	return headers, data
}

func (n *RegistryList) JSONPrint() ([]byte, error) {
	if len(n.Items) == 1 {
		return printer.JSONPrinter(n.Items[0])
	}
	return printer.JSONPrinter(n)
}
