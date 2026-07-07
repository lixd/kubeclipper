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

package client

import (
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/random"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

func TestDeleteRefValidation(t *testing.T) {
	tests := []struct {
		name    string
		ref     string
		digest  string
		wantErr bool
	}{
		{name: "empty", wantErr: true},
		{name: "invalid ref", ref: "%%%invalid%%%", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := DeleteRef(tt.ref, tt.digest)
			if (err != nil) != tt.wantErr {
				t.Fatalf("DeleteRef() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRemoveManifestFromIndex(t *testing.T) {
	amd64Image, err := random.Image(128, 1)
	if err != nil {
		t.Fatalf("random amd64 image: %+v", err)
	}
	arm64Image, err := random.Image(256, 1)
	if err != nil {
		t.Fatalf("random arm64 image: %+v", err)
	}
	amd64Digest, err := amd64Image.Digest()
	if err != nil {
		t.Fatalf("amd64 digest: %+v", err)
	}
	index := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{
			Add: amd64Image,
			Descriptor: v1.Descriptor{
				MediaType: types.OCIManifestSchema1,
				Platform:  &v1.Platform{OS: "linux", Architecture: "amd64"},
			},
		},
		mutate.IndexAddendum{
			Add: arm64Image,
			Descriptor: v1.Descriptor{
				MediaType: types.OCIManifestSchema1,
				Platform:  &v1.Platform{OS: "linux", Architecture: "arm64"},
			},
		},
	)
	updated, removed, remaining, err := RemoveManifestFromIndex(index, amd64Digest.String())
	if err != nil {
		t.Fatalf("RemoveManifestFromIndex() error: %+v", err)
	}
	if !removed {
		t.Fatalf("RemoveManifestFromIndex() removed = false")
	}
	if remaining != 1 {
		t.Fatalf("remaining = %d, want 1", remaining)
	}
	manifest, err := updated.IndexManifest()
	if err != nil {
		t.Fatalf("updated IndexManifest() error: %+v", err)
	}
	if manifest.Manifests[0].Platform == nil || manifest.Manifests[0].Platform.Architecture != "arm64" {
		t.Fatalf("remaining descriptor = %+v", manifest.Manifests[0])
	}
}

func TestRemoveManifestFromIndexMissingDigest(t *testing.T) {
	image, err := random.Image(128, 1)
	if err != nil {
		t.Fatalf("random image: %+v", err)
	}
	index := mutate.AppendManifests(empty.Index, mutate.IndexAddendum{
		Add: image,
		Descriptor: v1.Descriptor{
			MediaType: types.OCIManifestSchema1,
			Platform:  &v1.Platform{OS: "linux", Architecture: "amd64"},
		},
	})
	_, removed, remaining, err := RemoveManifestFromIndex(index, "sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
	if err != nil {
		t.Fatalf("RemoveManifestFromIndex() error: %+v", err)
	}
	if removed {
		t.Fatalf("RemoveManifestFromIndex() removed = true")
	}
	if remaining != 1 {
		t.Fatalf("remaining = %d, want 1", remaining)
	}
}
