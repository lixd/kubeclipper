/*
 *
 *  * Copyright 2024 KubeClipper Authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package apis

import "testing"

const testDigest = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

func TestParsePackageRepository(t *testing.T) {
	kind, name, ok := ParsePackageRepository("kubeclipper/packages/cri/containerd")
	if !ok {
		t.Fatalf("ParsePackageRepository() ok = false")
	}
	if kind != "cri" || name != "containerd" {
		t.Fatalf("ParsePackageRepository() = %q, %q", kind, name)
	}
	if _, _, ok = ParsePackageRepository("library/nginx"); ok {
		t.Fatalf("ParsePackageRepository() accepted non-kubeclipper repository")
	}
	if _, _, ok = ParsePackageRepository("kubeclipper/packages/cri"); ok {
		t.Fatalf("ParsePackageRepository() accepted incomplete repository")
	}
}

func TestDerivePackageEntryFromManifest(t *testing.T) {
	entry, err := DerivePackageEntryFromManifest(PackageRef{
		Registry:   "registry.local:5000",
		Repository: "kubeclipper/packages/cri/containerd",
		Tag:        "2.1.0",
		Digest:     testDigest,
	}, PackageManifest{
		SchemaVersion:  1,
		Kind:           "cri",
		Name:           "containerd",
		Version:        "2.1.0",
		ContentProfile: ContentProfileRuntime,
		Platform: PackageManifestPlatform{
			OS:   "linux",
			Arch: "amd64",
		},
		Contents: []PackageManifestFile{
			{Name: ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
		},
	})
	if err != nil {
		t.Fatalf("DerivePackageEntryFromManifest() error: %+v", err)
	}
	if entry.Kind != "cri" || entry.Name != "containerd" || entry.Version != "2.1.0" {
		t.Fatalf("entry identity = %+v", entry)
	}
	if entry.OS != "linux" || entry.Arch != "amd64" {
		t.Fatalf("entry platform = %s/%s", entry.OS, entry.Arch)
	}
	if entry.Transport.Ref != "registry.local:5000/kubeclipper/packages/cri/containerd:2.1.0" {
		t.Fatalf("transport ref = %q", entry.Transport.Ref)
	}
	if entry.Transport.Digest != testDigest {
		t.Fatalf("transport digest = %q", entry.Transport.Digest)
	}
	if len(entry.Contents) != 1 {
		t.Fatalf("content count = %d, want 1", len(entry.Contents))
	}
	if entry.Contents[0].MediaType != MediaTypeConfigsLayer {
		t.Fatalf("content mediaType = %q", entry.Contents[0].MediaType)
	}
	if entry.Contents[0].Digest != testDigest {
		t.Fatalf("content digest = %q", entry.Contents[0].Digest)
	}
}

func TestDerivePackageEntryFromManifestRejectsIdentityMismatch(t *testing.T) {
	_, err := DerivePackageEntryFromManifest(PackageRef{
		Registry:   "registry.local",
		Repository: "kubeclipper/packages/cri/containerd",
		Tag:        "2.1.0",
		Digest:     testDigest,
	}, PackageManifest{
		SchemaVersion: 1,
		Kind:          "cri",
		Name:          "docker",
		Version:       "2.1.0",
		Platform:      PackageManifestPlatform{Arch: "amd64"},
		Contents: []PackageManifestFile{
			{Name: ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
			{Name: ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
		},
	})
	if err == nil {
		t.Fatalf("DerivePackageEntryFromManifest() expected identity mismatch error")
	}
}

func TestDerivePackageEntryFromManifestRejectsMissingArch(t *testing.T) {
	_, err := DerivePackageEntryFromManifest(PackageRef{
		Registry:   "registry.local",
		Repository: "kubeclipper/packages/cri/containerd",
		Tag:        "2.1.0",
		Digest:     testDigest,
	}, PackageManifest{
		SchemaVersion: 1,
		Kind:          "cri",
		Name:          "containerd",
		Version:       "2.1.0",
		Contents: []PackageManifestFile{
			{Name: ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
			{Name: ContentConfigs, File: "configs.tar.gz", Digest: testDigest},
		},
	})
	if err == nil {
		t.Fatalf("DerivePackageEntryFromManifest() expected missing arch error")
	}
}
