/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package releasemanifest

import (
	"bytes"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	registryserver "github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	registryconfig "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
)

type rewriteTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (t rewriteTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.URL.Scheme = t.target.Scheme
	clone.URL.Host = t.target.Host
	clone.Host = t.target.Host
	return t.base.RoundTrip(clone)
}

func TestSyncMultiArchAuthenticatedTarget(t *testing.T) { //nolint:gocyclo,funlen // One Registry fixture verifies the complete copy lifecycle.
	sourceServer := httptest.NewTLSServer(registryserver.New())
	defer sourceServer.Close()
	sourceURL, err := url.Parse(sourceServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	sourceTransport := rewriteTransport{target: sourceURL, base: sourceServer.Client().Transport}
	sourceOptions := []crane.Option{crane.WithAuth(authn.Anonymous), crane.WithTransport(sourceTransport)}

	amd64 := revisionImage(t, "release-revision")
	arm64 := revisionImage(t, "release-revision")
	amd64Digest, err := amd64.Digest()
	if err != nil {
		t.Fatal(err)
	}
	arm64Digest, err := arm64.Digest()
	if err != nil {
		t.Fatal(err)
	}
	index := mutate.AppendManifests(empty.Index,
		mutate.IndexAddendum{Add: amd64, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "amd64"}}},
		mutate.IndexAddendum{Add: arm64, Descriptor: v1.Descriptor{Platform: &v1.Platform{OS: "linux", Architecture: "arm64"}}},
	)
	indexDigest, err := index.Digest()
	if err != nil {
		t.Fatal(err)
	}
	source := OfficialSourcePrefix + "/kubeclipper/packages/bootstrap/kubeclipper:v2.0.0"
	sourceRef, err := name.ParseReference(source)
	if err != nil {
		t.Fatal(err)
	}
	if err = remote.WriteIndex(sourceRef, index, remote.WithAuth(authn.Anonymous), remote.WithTransport(sourceTransport)); err != nil {
		t.Fatalf("publish source index: %v", err)
	}
	runtimeSource := OfficialSourcePrefix + "/pause:3.10"
	runtimeRef, err := name.ParseReference(runtimeSource)
	if err != nil {
		t.Fatal(err)
	}
	if err = remote.WriteIndex(runtimeRef, index, remote.WithAuth(authn.Anonymous), remote.WithTransport(sourceTransport)); err != nil {
		t.Fatalf("publish runtime source index: %v", err)
	}

	targetHandler := registryserver.New()
	targetServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || username != "robot" || password != "secret" {
			w.Header().Set("WWW-Authenticate", `Basic realm="test"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		targetHandler.ServeHTTP(w, r)
	}))
	defer targetServer.Close()
	targetRegistry := strings.TrimPrefix(targetServer.URL, "https://") + "/project"
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: targetServer.Certificate().Raw})
	config := &registryconfig.Config{Registry: targetRegistry, Scheme: registryconfig.SchemeHTTPS, Username: "robot", Password: "secret", CA: string(ca)}

	manifest := &Manifest{
		APIVersion: APIVersion,
		Kind:       Kind,
		Metadata:   Metadata{Name: "kubeclipper-resources", Version: "v2.0.0", SourceRevision: "release-revision"},
		Registries: Registries{Package: OfficialSourcePrefix, Image: OfficialSourcePrefix},
		Artifacts: []Artifact{{
			Type: "package-image", Component: Component{Kind: "bootstrap", Name: "kubeclipper", Version: "v2.0.0"},
			Source: source, Target: "kubeclipper/packages/bootstrap/kubeclipper:v2.0.0",
			Platforms: []string{"linux/amd64", "linux/arm64"}, Digest: indexDigest.String(), SourceRevision: "release-revision",
		}, {
			Type: "runtime-image", Component: Component{Kind: "k8s", Name: "k8s", Version: "v1.36.1"},
			Source: runtimeSource, Target: "pause:3.10", Platforms: []string{"linux/amd64"}, Digest: amd64Digest.String(),
		}, {
			Type: "runtime-image", Component: Component{Kind: "k8s", Name: "k8s", Version: "v1.36.1"},
			Source: runtimeSource, Target: "pause:3.10", Platforms: []string{"linux/arm64"}, Digest: arm64Digest.String(),
		}},
	}

	for _, test := range []struct {
		name       string
		arch       string
		wantDigest string
	}{
		{name: "all", arch: "all", wantDigest: indexDigest.String()},
		{name: "amd64", arch: "amd64", wantDigest: amd64Digest.String()},
		{name: "arm64", arch: "arm64", wantDigest: arm64Digest.String()},
	} {
		t.Run(test.name, func(t *testing.T) {
			artifact := manifest.Artifacts[0]
			artifact.Target = "kubeclipper/packages/bootstrap/kubeclipper:" + test.arch
			runtimeAMD64 := manifest.Artifacts[1]
			runtimeARM64 := manifest.Artifacts[2]
			runtimeAMD64.Target = "pause:" + test.arch
			runtimeARM64.Target = runtimeAMD64.Target
			copyManifest := *manifest
			copyManifest.Artifacts = []Artifact{artifact, runtimeAMD64, runtimeARM64}
			var output bytes.Buffer
			result, syncErr := Sync(t.Context(), &copyManifest, &SyncOptions{
				Registry: targetRegistry, Arch: test.arch, Config: config, Out: &output, SourceOptions: sourceOptions,
			})
			if syncErr != nil {
				t.Fatalf("Sync() error = %v", syncErr)
			}
			if result.Copied != 2 || !strings.Contains(output.String(), "copy:") {
				t.Fatalf("Sync() result=%+v output=%q", result, output.String())
			}
			destinationOptions, optionErr := config.CraneOptions(t.Context())
			if optionErr != nil {
				t.Fatal(optionErr)
			}
			got, headErr := crane.Head(targetRegistry+"/"+artifact.Target, destinationOptions...)
			if headErr != nil || got.Digest.String() != test.wantDigest {
				t.Fatalf("target digest=%v err=%v, want %s", got, headErr, test.wantDigest)
			}
			runtimeGot, runtimeHeadErr := crane.Head(targetRegistry+"/"+runtimeAMD64.Target, destinationOptions...)
			if runtimeHeadErr != nil || runtimeGot.Digest.String() != test.wantDigest {
				t.Fatalf("runtime target digest=%v err=%v, want %s", runtimeGot, runtimeHeadErr, test.wantDigest)
			}
			result, syncErr = Sync(t.Context(), &copyManifest, &SyncOptions{
				Registry: targetRegistry, Arch: test.arch, Config: config, SourceOptions: sourceOptions,
			})
			if syncErr != nil || result.Skipped != 2 {
				t.Fatalf("second Sync() result=%+v error=%v", result, syncErr)
			}
		})
	}

	conflictManifest := *manifest
	conflictArtifact := manifest.Artifacts[0]
	conflictArtifact.Target = "kubeclipper/packages/bootstrap/kubeclipper:conflict"
	conflictManifest.Artifacts = []Artifact{conflictArtifact}
	destinationOptions, optionErr := config.CraneOptions(t.Context())
	if optionErr != nil {
		t.Fatal(optionErr)
	}
	if err = crane.Push(empty.Image, targetRegistry+"/"+conflictArtifact.Target, destinationOptions...); err != nil {
		t.Fatalf("push conflict image: %v", err)
	}
	if _, err = Sync(t.Context(), &conflictManifest, &SyncOptions{
		Registry: targetRegistry, Arch: "all", Config: config, SourceOptions: sourceOptions,
	}); err == nil || !strings.Contains(err.Error(), "target tag conflict") {
		t.Fatalf("Sync() conflict error = %v", err)
	}

	badConfig := &registryconfig.Config{Registry: targetRegistry, Scheme: registryconfig.SchemeHTTPS, Username: "robot", Password: "wrong", CA: string(ca)}
	if _, err = Sync(t.Context(), &conflictManifest, &SyncOptions{
		Registry: targetRegistry, Arch: "all", Config: badConfig, SourceOptions: sourceOptions,
	}); err == nil || !strings.Contains(err.Error(), "401 Unauthorized") {
		t.Fatalf("Sync() authentication error = %v", err)
	}
}

func revisionImage(t *testing.T, revision string) v1.Image {
	t.Helper()
	image, err := mutate.Config(empty.Image, v1.Config{Labels: map[string]string{"org.opencontainers.image.revision": revision}})
	if err != nil {
		t.Fatal(err)
	}
	return image
}
