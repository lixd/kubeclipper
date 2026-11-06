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

package registry

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	containerv1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/tarball"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

func TestRegistryCommandsUseUnifiedDelivery(t *testing.T) {
	cmd := NewCmdRegistryDeploy(NewRegistryOptions(options.IOStreams{}))
	for _, name := range []string{"pkg", "registry-image", "registry-image-archive", "registry-binary", "component-registry", "version"} {
		if cmd.Flags().Lookup(name) != nil {
			t.Fatalf("obsolete flag --%s remains", name)
		}
	}
	for _, name := range []string{"bundle", "package-registry"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Fatalf("missing --%s", name)
		}
	}
	root := NewCmdRegistry(options.IOStreams{})
	commands := make(map[string]bool)
	for _, child := range root.Commands() {
		commands[child.Name()] = true
		for _, alias := range child.Aliases {
			if alias == "sync" || alias == "push" {
				t.Fatalf("obsolete registry alias %s remains", alias)
			}
		}
	}
	if !commands["export"] || !commands["import"] || commands["sync"] || commands["push"] {
		t.Fatalf("unexpected registry commands: %v", commands)
	}
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	for _, name := range []string{"sync", "push"} {
		root.SetArgs([]string{name})
		if err := root.Execute(); err == nil || !strings.Contains(err.Error(), "unknown command") {
			t.Fatalf("removed command %s returned %v", name, err)
		}
	}
	root.SetArgs([]string{})
	if err := root.Execute(); err != nil {
		t.Fatalf("registry help returned %v", err)
	}
}

func TestValidateRegistryDeploySources(t *testing.T) {
	o := NewRegistryOptions(options.IOStreams{})
	o.Node = "10.0.0.1"
	o.SSHConfig.PkFile = "/tmp/id_rsa"
	if err := o.ValidateArgsDeploy(); err != nil {
		t.Fatal(err)
	}
	o.BundlePath = "registry.tar.gz"
	if err := o.ValidateArgsDeploy(); err != nil {
		t.Fatal(err)
	}
	o.PackageRegistry = "registry.example/kubeclipper"
	if err := o.ValidateArgsDeploy(); err == nil {
		t.Fatal("mixed online and offline sources accepted")
	}
	o.PackageRegistry = ""
	o.Arch = "riscv64"
	if err := o.ValidateArgsDeploy(); err == nil {
		t.Fatal("unsupported target architecture accepted")
	}
}

func TestExtractRegistryBinaryRejectsUnexpectedPath(t *testing.T) {
	img := testRegistryImage(t, "/tmp/registry", "registry-binary")
	dst := filepath.Join(t.TempDir(), "registry")

	err := extractRegistryBinary(img, dst)
	if err == nil {
		t.Fatalf("extractRegistryBinary() expected error")
	}
	if !errors.Is(err, io.EOF) && !contains(err.Error(), "registry binary not found") {
		t.Fatalf("extractRegistryBinary() error = %+v", err)
	}
}

func TestExtractRegistryBinaryFromPublishedPackage(t *testing.T) {
	img := testRegistryImage(t, "/opt/kubeclipper/resource/registry", "registry-binary")
	dst := filepath.Join(t.TempDir(), "registry")
	if err := extractRegistryBinary(img, dst); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(dst)
	if err != nil || string(data) != "registry-binary" {
		t.Fatalf("extracted binary = %q, error = %v", data, err)
	}
}

func testRegistryImage(t *testing.T, binaryPath, content string) containerv1.Image {
	t.Helper()
	layerTar := filepath.Join(t.TempDir(), "layer.tar")
	file, err := os.Create(layerTar)
	if err != nil {
		t.Fatalf("Create() error = %+v", err)
	}
	tw := tar.NewWriter(file)
	data := []byte(content)
	if err = tw.WriteHeader(&tar.Header{
		Name: binaryPath,
		Mode: 0755,
		Size: int64(len(data)),
	}); err != nil {
		t.Fatalf("WriteHeader() error = %+v", err)
	}
	if _, err = io.Copy(tw, bytes.NewReader(data)); err != nil {
		t.Fatalf("Copy() error = %+v", err)
	}
	if err = tw.Close(); err != nil {
		t.Fatalf("Close tar writer error = %+v", err)
	}
	if err = file.Close(); err != nil {
		t.Fatalf("Close file error = %+v", err)
	}
	layer, err := tarball.LayerFromFile(layerTar)
	if err != nil {
		t.Fatalf("LayerFromFile() error = %+v", err)
	}
	img, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		t.Fatalf("AppendLayers() error = %+v", err)
	}
	return img
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

func TestRegistryPackageChecksPlatformAndBinaryDigest(t *testing.T) {
	for _, test := range []struct {
		name, arch, digest string
		valid              bool
	}{
		{"valid", "amd64", fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("binary"))), true},
		{"wrong architecture", "arm64", fmt.Sprintf("sha256:%x", sha256.Sum256([]byte("binary"))), false},
		{"wrong payload", "amd64", "sha256:" + strings.Repeat("0", 64), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			image := testRegistryImage(t, "/opt/kubeclipper/resource/registry", "binary")
			manifest := deliveryapis.PackageManifest{SchemaVersion: 1, Kind: "bootstrap", Name: "registry", Version: "3.1.1",
				Platform: deliveryapis.PackageManifestPlatform{OS: "linux", Arch: test.arch},
				Contents: []deliveryapis.PackageManifestFile{{Name: "registry", File: "registry", Digest: test.digest}},
			}
			data, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			var buf bytes.Buffer
			tw := tar.NewWriter(&buf)
			if headerErr := tw.WriteHeader(&tar.Header{Name: "opt/kubeclipper/resource/kc-package-manifest.json", Mode: 0600, Size: int64(len(data))}); headerErr != nil {
				t.Fatal(headerErr)
			}
			if _, err = tw.Write(data); err != nil {
				t.Fatal(err)
			}
			_ = tw.Close()
			layer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buf.Bytes())), nil })
			if err != nil {
				t.Fatal(err)
			}
			image, err = mutate.AppendLayers(image, layer)
			if err != nil {
				t.Fatal(err)
			}
			_, cleanup, err := extractRegistryPackageBinary(image, "amd64")
			if cleanup != nil {
				defer cleanup()
			}
			if (err == nil) != test.valid {
				t.Fatalf("extract package error=%v, valid=%v", err, test.valid)
			}
		})
	}
}
