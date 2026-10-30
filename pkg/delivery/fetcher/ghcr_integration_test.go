/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fetcher

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/crane"

	apis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
)

// Explicit opt-in: this transfers a real public bootstrap layer, without seeding it.
func TestRealGHCRPackage(t *testing.T) {
	ref := os.Getenv("KC_TEST_GHCR_REF")
	if ref == "" {
		t.Skip("set KC_TEST_GHCR_REF to a pinned public bootstrap manifest")
	}
	t.Setenv(downloader.CacheDirEnv, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	image, err := crane.Pull(ref, crane.WithContext(ctx))
	if err != nil {
		t.Fatal(safeNetworkError(err))
	}
	config, err := image.ConfigFile()
	if err != nil {
		t.Fatal(safeNetworkError(err))
	}
	version := config.Config.Labels["org.opencontainers.image.version"]
	if version == "" {
		t.Fatal("package version metadata missing")
	}
	_, dgst, ok := strings.Cut(ref, "@")
	if !ok {
		t.Fatal("integration reference must be digest pinned")
	}
	plan := &apis.ResolvedArtifactPlan{OS: "linux", Arch: "amd64", Components: []apis.ResolvedComponent{{Kind: "bootstrap", Name: "kubeclipper", Version: version, Transport: apis.TransportRef{Type: apis.TransportOCI, Ref: ref, Digest: dgst}, Contents: []apis.ArtifactContent{{Name: "kubeclipper-server", File: "kubeclipper-server"}, {Name: "kubeclipper-agent", File: "kubeclipper-agent"}}}}}
	f := NewOCIArtifactFetcher(false)
	start := time.Now()
	result, err := f.Fetch(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	for name, p := range result.Components[0].Files {
		info, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("verified extracted asset %s: %d bytes", name, info.Size())
	}
	t.Logf("verified real GHCR transfer version=%s manifest=%s elapsed=%s", version, dgst, time.Since(start))
	if err = result.Release(); err != nil {
		t.Fatal(err)
	}
	start = time.Now()
	result, err = f.Fetch(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()
	t.Logf("verified warm-cache fetch elapsed=%s", time.Since(start))
}
