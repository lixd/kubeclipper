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
	"errors"
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
)

func TestNewCmdRegistryDeployDoesNotExposeLegacyPackageFlag(t *testing.T) {
	cmd := NewCmdRegistryDeploy(NewRegistryOptions(options.IOStreams{}))
	if cmd.Flags().Lookup("pkg") != nil {
		t.Fatalf("registry deploy should not expose package flag")
	}
	for _, name := range []string{"registry-image", "registry-image-archive", "registry-binary", "package-registry"} {
		if cmd.Flags().Lookup(name) == nil {
			t.Fatalf("registry deploy missing --%s flag", name)
		}
	}
	if cmd.Flags().Lookup("component-registry") != nil {
		t.Fatalf("registry deploy should not expose --component-registry")
	}
}

func TestNewCmdRegistryPushDoesNotExposePackageFlag(t *testing.T) {
	cmd := NewCmdRegistryPush(NewRegistryOptions(options.IOStreams{}))
	if cmd.Flags().Lookup("pkg") != nil {
		t.Fatalf("registry push should not expose package flag")
	}
	if cmd.Flags().Lookup("image-archive") == nil {
		t.Fatalf("registry push missing --image-archive flag")
	}
}

func TestValidateArgsPushRequiresImageArchive(t *testing.T) {
	o := NewRegistryOptions(options.IOStreams{})
	o.Node = "10.0.0.1"

	err := o.ValidateArgsPush()
	if err == nil {
		t.Fatalf("ValidateArgsPush() expected error")
	}
	if got, want := err.Error(), "--image-archive must be specified"; !contains(got, want) {
		t.Fatalf("ValidateArgsPush() error = %q, want containing %q", got, want)
	}

	o.ImageArchive = "images.tar.gz"
	if err = o.ValidateArgsPush(); err != nil {
		t.Fatalf("ValidateArgsPush() error = %+v", err)
	}
}

func TestValidateArgsDeployAcceptsDefaultRegistryImage(t *testing.T) {
	o := NewRegistryOptions(options.IOStreams{})
	o.Node = "10.0.0.1"
	o.SSHConfig.PkFile = "/tmp/id_rsa"
	o.Arch = "amd64"

	if err := o.ValidateArgsDeploy(); err != nil {
		t.Fatalf("ValidateArgsDeploy() error = %+v", err)
	}
}

func TestValidateArgsDeployRejectsMultipleSources(t *testing.T) {
	o := NewRegistryOptions(options.IOStreams{})
	o.Node = "10.0.0.1"
	o.SSHConfig.PkFile = "/tmp/id_rsa"
	o.Arch = "amd64"
	o.RegistryImage = "ghcr.io/lixd/kubeclipper/kubeclipper/packages/bootstrap/registry:3.1.1"
	o.RegistryBinary = "./registry"

	err := o.ValidateArgsDeploy()
	if err == nil {
		t.Fatalf("ValidateArgsDeploy() expected error")
	}
	if got, want := err.Error(), "mutually exclusive"; !contains(got, want) {
		t.Fatalf("ValidateArgsDeploy() error = %q, want containing %q", got, want)
	}
}

func TestResolveRegistryImage(t *testing.T) {
	o := NewRegistryOptions(options.IOStreams{})
	ref, err := o.resolveRegistryImage()
	if err != nil {
		t.Fatalf("resolveRegistryImage() error = %+v", err)
	}
	if ref != "ghcr.io/lixd/kubeclipper/kubeclipper/packages/bootstrap/registry:3.1.1" {
		t.Fatalf("resolveRegistryImage() = %q", ref)
	}

	o.PackageRegistry = "harbor.example.com/kubeclipper/"
	o.Version = "v1.8.0"
	ref, err = o.resolveRegistryImage()
	if err != nil {
		t.Fatalf("resolveRegistryImage() error = %+v", err)
	}
	if ref != "harbor.example.com/kubeclipper/kubeclipper/packages/bootstrap/registry:v1.8.0" {
		t.Fatalf("resolveRegistryImage() = %q", ref)
	}
}

func TestNormalizeRegistryBinaryAlwaysNamesFileRegistry(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "registry-linux-amd64")
	if err := os.WriteFile(src, []byte("registry"), 0644); err != nil {
		t.Fatalf("WriteFile() error = %+v", err)
	}

	path, cleanup, err := normalizeRegistryBinary(src)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		t.Fatalf("normalizeRegistryBinary() error = %+v", err)
	}
	if filepath.Base(path) != "registry" {
		t.Fatalf("normalizeRegistryBinary() path = %q, want basename registry", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %+v", err)
	}
	if string(data) != "registry" {
		t.Fatalf("normalized binary content = %q", string(data))
	}
}

func TestExtractRegistryBinaryUsesExpectedPath(t *testing.T) {
	for _, binaryPath := range []string{packageRegistryBinaryPath, standardRegistryBinaryPath} {
		t.Run(binaryPath, func(t *testing.T) {
			img := testRegistryImage(t, binaryPath, "registry-binary")
			dst := filepath.Join(t.TempDir(), "registry")

			if err := extractRegistryBinary(img, dst, binaryPath); err != nil {
				t.Fatalf("extractRegistryBinary() error = %+v", err)
			}
			data, err := os.ReadFile(dst)
			if err != nil {
				t.Fatalf("ReadFile() error = %+v", err)
			}
			if string(data) != "registry-binary" {
				t.Fatalf("extracted binary = %q", string(data))
			}
		})
	}
}

func TestExtractRegistryBinaryRejectsUnexpectedPath(t *testing.T) {
	img := testRegistryImage(t, "/tmp/registry", "registry-binary")
	dst := filepath.Join(t.TempDir(), "registry")

	err := extractRegistryBinary(img, dst, standardRegistryBinaryPath)
	if err == nil {
		t.Fatalf("extractRegistryBinary() expected error")
	}
	if !errors.Is(err, io.EOF) && !contains(err.Error(), "registry binary not found") {
		t.Fatalf("extractRegistryBinary() error = %+v", err)
	}
}

func TestNormalizeRegistryArchitecture(t *testing.T) {
	tests := map[string]string{
		"x86_64\n":  "amd64",
		"amd64":     "amd64",
		"aarch64\n": "arm64",
		"arm64":     "arm64",
	}
	for machine, want := range tests {
		got, err := normalizeRegistryArchitecture(machine)
		if err != nil {
			t.Fatalf("normalizeRegistryArchitecture(%q): %v", machine, err)
		}
		if got != want {
			t.Fatalf("normalizeRegistryArchitecture(%q) = %q, want %q", machine, got, want)
		}
	}
	if _, err := normalizeRegistryArchitecture("riscv64"); err == nil {
		t.Fatal("unsupported architecture must return an error")
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
