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

package imageproxy

import (
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"

	"github.com/spf13/pflag"
)

type Options struct {
	KcImageRepoMirror string `json:"kcImageRepoMirror" yaml:"kcImageRepoMirror"`
}

func NewOptions() *Options {
	return &Options{
		KcImageRepoMirror: "",
	}
}

func (s *Options) Validate() []error {
	mirror := s.KcImageRepoMirror
	if mirror == "" {
		return nil
	}
	// Validate a complete repository below the prefix, so a host-only
	// mirror (including host:port) is interpreted as a registry authority.
	if _, err := name.NewRepository(mirror + "/kc-image-mirror-validation"); err != nil {
		return []error{fmt.Errorf("invalid kc image repo mirror: %w", err)}
	}
	// NewRepository validates characters but allows empty and dot segments.
	// They are not valid repository namespaces and must not be normalized.
	for part := range strings.SplitSeq(mirror, "/") {
		if part == "" || part == "." || part == ".." {
			return []error{fmt.Errorf("invalid kc image repo mirror: empty or dot path segment")}
		}
	}
	return nil
}

func (s *Options) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&s.KcImageRepoMirror, "kc-image-repo-mirror", s.KcImageRepoMirror, "K8s image repository mirror")
}
