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

package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryfetcher "github.com/kubeclipper/kubeclipper/pkg/delivery/fetcher"
	deliveryindexer "github.com/kubeclipper/kubeclipper/pkg/delivery/indexer"
	deliverypublisher "github.com/kubeclipper/kubeclipper/pkg/delivery/publisher"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
)

const (
	archAMD64              = "amd64"
	archARM64              = "arm64"
	kindCNI                = "cni"
	kindCRI                = "cri"
	kindK8s                = "k8s"
	kindBinary             = "binary"
	kindExtension          = "extension"
	nameCalico             = "calico"
	nameContainerd         = "containerd"
	nameEtcdctl            = "etcdctl"
	nameKubeClipperAgent   = "kubeclipper-agent"
	nameKubectlTerminal    = "kubectl-terminal"
	versionCalico          = "v3.30.0"
	versionContainerd      = "2.1.0"
	versionEtcdctl         = "v3.5.15"
	versionK8s             = "v1.36.0"
	versionKubeClipper     = "v1.8.0"
	versionKubectlTerminal = "v1.0.0"
	imagesArchive          = "images.tar.gz"
	verificationComponents = 6
	directoryMode          = 0755
	fileMode               = 0644
	executableFileMode     = 0755
)

type packageFixture struct {
	kind    string
	name    string
	version string
	profile string
	files   []string
}

type verifier struct {
	total    int
	passed   int
	failures []string
}

func main() {
	if len(os.Args) != 2 || strings.TrimSpace(os.Args[1]) == "" {
		fmt.Fprintf(os.Stderr, "usage: %s <registry-host:port>\n", filepath.Base(os.Args[0]))
		os.Exit(2)
	}
	if err := run(context.Background(), strings.TrimSpace(os.Args[1])); err != nil {
		fmt.Fprintf(os.Stderr, "oci verification failed: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, registry string) error {
	v := &verifier{}
	workdir, err := os.MkdirTemp("", "kc-oci-verify-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(workdir)
	_ = os.RemoveAll(downloader.BaseDstDir)

	fixtures := verificationFixtures()
	if publishErr := publishFixtures(workdir, registry, fixtures, v); publishErr != nil {
		return publishErr
	}

	inventory, err := deliveryindexer.NewRegistryPackageInventoryIndexer(nil).Refresh(ctx, registry)
	if err != nil {
		return err
	}
	verifyInventory(inventory, fixtures, registry, v)
	plan, err := resolveVerificationPlan(inventory, v)
	if err != nil {
		return err
	}
	if err := verifyFetchedPlan(ctx, plan, v); err != nil {
		return err
	}

	if len(v.failures) > 0 {
		return fmt.Errorf("verification checks failed: %s", strings.Join(v.failures, "; "))
	}
	fmt.Printf("OCI verification passed: %d/%d checks\n", v.passed, v.total)
	return nil
}

func verificationFixtures() []packageFixture {
	return []packageFixture{
		{
			kind: kindK8s, name: kindK8s, version: versionK8s,
			profile: deliveryapis.ContentProfileK8s, files: []string{"configs.tar.gz", imagesArchive},
		},
		{
			kind: kindCRI, name: nameContainerd, version: versionContainerd,
			profile: deliveryapis.ContentProfileRuntime, files: []string{"configs.tar.gz", imagesArchive},
		},
		{
			kind: kindCNI, name: nameCalico, version: versionCalico,
			profile: deliveryapis.ContentProfileAddon, files: []string{imagesArchive, "charts.tgz"},
		},
		{
			kind: kindBinary, name: nameKubeClipperAgent, version: versionKubeClipper,
			profile: deliveryapis.ContentProfileBinary, files: []string{nameKubeClipperAgent},
		},
		{
			kind: kindBinary, name: nameEtcdctl, version: versionEtcdctl,
			profile: deliveryapis.ContentProfileBinary, files: []string{nameEtcdctl},
		},
		{
			kind: kindExtension, name: nameKubectlTerminal, version: versionKubectlTerminal,
			profile: deliveryapis.ContentProfileExtension, files: []string{imagesArchive},
		},
	}
}

func publishFixtures(workdir, registry string, fixtures []packageFixture, v *verifier) error {
	publisher := deliverypublisher.NewOCIArtifactPublisher()
	for i := range fixtures {
		fixture := &fixtures[i]
		archive, err := writePackageArchive(workdir, fixture)
		if err != nil {
			return err
		}
		result, err := publisher.Publish(deliverypublisher.PublishRequest{
			PackagePath: archive, Kind: fixture.kind, Name: fixture.name, Version: fixture.version,
			Arch: archAMD64, Registry: registry, ContentProfile: fixture.profile,
		})
		if err != nil {
			return fmt.Errorf("publish %s/%s:%s: %w", fixture.kind, fixture.name, fixture.version, err)
		}
		v.checkf(result.Transport.Type == deliveryapis.TransportOCI, "publish %s/%s uses OCI transport", fixture.kind, fixture.name)
		v.checkf(strings.HasPrefix(result.Transport.Digest, "sha256:"), "publish %s/%s records manifest digest", fixture.kind, fixture.name)
	}
	return nil
}

func verifyInventory(inventory *deliveryapis.PackageInventory, fixtures []packageFixture, registry string, v *verifier) {
	v.checkf(inventory.Spec.Registry == registry, "inventory records source registry")
	v.checkf(len(inventory.Spec.Packages) >= len(fixtures), "inventory contains all published packages")
	for i := range fixtures {
		fixture := &fixtures[i]
		pkg, ok := findPackage(inventory, fixture.kind, fixture.name, fixture.version)
		v.checkf(ok, "inventory has %s/%s:%s", fixture.kind, fixture.name, fixture.version)
		if ok {
			v.checkf(pkg.Arch == archAMD64, "inventory %s/%s arch comes from platform", fixture.kind, fixture.name)
			v.checkf(pkg.Transport.Type == deliveryapis.TransportOCI, "inventory %s/%s transport is OCI", fixture.kind, fixture.name)
			v.checkf(strings.HasPrefix(pkg.Transport.Digest, "sha256:"), "inventory %s/%s has digest", fixture.kind, fixture.name)
		}
	}
}

func resolveVerificationPlan(inventory *deliveryapis.PackageInventory, v *verifier) (*deliveryapis.ResolvedArtifactPlan, error) {
	policy := supportPolicy()
	plan, err := deliveryapis.ResolveArtifacts(inventory, policy, deliveryapis.ResolveRequest{
		KubernetesVersion: versionK8s, OS: deliveryapis.DefaultPackageOS, Arch: archAMD64,
	})
	if err != nil {
		return nil, err
	}
	v.checkf(len(plan.Components) == 3, "cluster install plan contains k8s/cri/cni only")
	for i := range plan.Components {
		component := &plan.Components[i]
		v.checkf(component.Transport.Type == deliveryapis.TransportOCI, "plan %s/%s transport is OCI", component.Kind, component.Name)
		v.checkf(strings.HasPrefix(component.Transport.Digest, "sha256:"), "plan %s/%s is digest pinned", component.Kind, component.Name)
	}
	_, err = deliveryapis.ResolveArtifacts(inventory, policy, deliveryapis.ResolveRequest{
		KubernetesVersion: versionK8s, OS: deliveryapis.DefaultPackageOS, Arch: archARM64,
	})
	var resolverErr *deliveryapis.ResolverError
	v.checkf(
		errors.As(err, &resolverErr) && resolverErr.Code == deliveryapis.ErrArtifactArchUnavailable,
		"arm64 resolve fails with ArtifactArchUnavailable",
	)

	agent, err := deliveryapis.ResolveBootstrapBinary(inventory, policy, deliveryapis.BootstrapBinaryResolveRequest{
		Arch: archAMD64, KubernetesVersion: versionK8s,
		Candidates: []deliveryapis.PackageCandidate{{Kind: kindBinary, Name: nameKubeClipperAgent}},
	})
	if err != nil {
		return nil, err
	}
	etcdctl, err := deliveryapis.ResolveBootstrapBinary(inventory, policy, deliveryapis.BootstrapBinaryResolveRequest{
		Arch: archAMD64, KubernetesVersion: versionK8s,
		Candidates: []deliveryapis.PackageCandidate{{Kind: kindBinary, Name: nameEtcdctl}},
	})
	if err != nil {
		return nil, err
	}
	extension, err := deliveryapis.ResolveExtensionArtifact(inventory, policy, deliveryapis.ExtensionResolveRequest{
		Arch: archAMD64, KubernetesVersion: versionK8s,
		Candidates: []deliveryapis.ExtensionCandidate{{Kind: kindExtension, Name: nameKubectlTerminal}},
	})
	if err != nil {
		return nil, err
	}
	v.checkf(agent.Kind == kindBinary && agent.Name == nameKubeClipperAgent, "bootstrap agent resolves from binary package")
	v.checkf(etcdctl.Kind == kindBinary && etcdctl.Name == nameEtcdctl, "bootstrap etcdctl resolves from binary package")
	v.checkf(extension.Kind == kindExtension && extension.Name == nameKubectlTerminal, "kubectl terminal resolves from extension package")
	_, err = deliveryapis.ResolveExtensionArtifact(inventory, policy, deliveryapis.ExtensionResolveRequest{
		Arch: archAMD64, KubernetesVersion: versionK8s,
		Candidates: []deliveryapis.ExtensionCandidate{{Kind: kindExtension, Name: "inventory-only-extension"}},
	})
	v.checkf(
		errors.As(err, &resolverErr) && resolverErr.Code == deliveryapis.ErrUnsupportedComponentChoice,
		"extension inventory-only candidate is rejected by policy",
	)

	plan.Components = append(plan.Components, agent, etcdctl, extension)
	return plan, nil
}

func verifyFetchedPlan(ctx context.Context, plan *deliveryapis.ResolvedArtifactPlan, v *verifier) error {
	fetchResult, err := deliveryfetcher.NewOCIArtifactFetcher(false).Fetch(ctx, plan)
	if err != nil {
		return err
	}
	v.checkf(len(fetchResult.Components) == verificationComponents, "fetcher materializes all resolved components")
	for i := range fetchResult.Components {
		component := &fetchResult.Components[i]
		v.checkf(component.Transport.Type == deliveryapis.TransportOCI, "fetched %s/%s transport remains OCI", component.Kind, component.Name)
		v.checkf(component.ManifestPath != "", "fetched %s/%s manifest is recorded", component.Kind, component.Name)
		for name, path := range component.Files {
			v.checkf(path != "", "fetched %s/%s content %s is recorded", component.Kind, component.Name, name)
		}
	}
	return nil
}

func supportPolicy() *deliveryapis.SupportPolicy {
	policy := deliveryapis.NewSupportPolicy("default")
	policy.Spec.Policies = []deliveryapis.KubernetesSupportPolicy{{
		Name:  "k8s-v1.36",
		Match: deliveryapis.PolicyMatch{KubernetesVersion: "v1.36.*"},
		ComponentSlots: []deliveryapis.ComponentSlotRule{
			{
				Slot:      "cri",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: nameContainerd, Version: versionContainerd},
				Options: []deliveryapis.ComponentOption{{
					Kind:            kindCRI,
					Name:            nameContainerd,
					AllowedVersions: []string{versionContainerd},
				}},
			},
			{
				Slot:      "cni",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: nameCalico, Version: versionCalico},
				Options: []deliveryapis.ComponentOption{{
					Kind:            kindCNI,
					Name:            nameCalico,
					AllowedVersions: []string{versionCalico},
				}},
			},
			{
				Slot:      "bootstrap-kubeclipper-agent",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: nameKubeClipperAgent, Version: versionKubeClipper},
				Options: []deliveryapis.ComponentOption{{
					Kind:            kindBinary,
					Name:            nameKubeClipperAgent,
					AllowedVersions: []string{versionKubeClipper},
				}},
			},
			{
				Slot:      "bootstrap-etcdctl",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: nameEtcdctl, Version: versionEtcdctl},
				Options: []deliveryapis.ComponentOption{{
					Kind:            kindBinary,
					Name:            nameEtcdctl,
					AllowedVersions: []string{versionEtcdctl},
				}},
			},
			{
				Slot:      "extension",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: nameKubectlTerminal, Version: versionKubectlTerminal},
				Options: []deliveryapis.ComponentOption{{
					Kind:            kindExtension,
					Name:            nameKubectlTerminal,
					AllowedVersions: []string{versionKubectlTerminal},
				}},
			},
		},
	}}
	return policy
}

func writePackageArchive(workdir string, fixture *packageFixture) (string, error) {
	root := filepath.Join(workdir, "archives", fixture.kind, fixture.name, fixture.version)
	if err := os.MkdirAll(root, directoryMode); err != nil {
		return "", err
	}
	entries := make([]tarEntry, 0, len(fixture.files))
	for _, file := range fixture.files {
		contents := []byte(fixture.kind + "/" + fixture.name + "/" + fixture.version + "/" + file + "\n")
		mode := int64(fileMode)
		if fixture.profile == deliveryapis.ContentProfileBinary {
			mode = executableFileMode
		} else {
			var err error
			contents, err = tarGzPayload(contents, fileMode)
			if err != nil {
				return "", err
			}
		}
		entries = append(entries, tarEntry{
			name: filepath.ToSlash(filepath.Join(fixture.name, fixture.version, archAMD64, file)),
			data: contents,
			mode: mode,
		})
	}
	archive := filepath.Join(root, fixture.name+".tar.gz")
	return archive, writeTarGz(archive, entries)
}

type tarEntry struct {
	name string
	data []byte
	mode int64
}

func tarGzPayload(data []byte, mode int64) ([]byte, error) {
	buffer := &bytes.Buffer{}
	writer := gzip.NewWriter(buffer)
	tarWriter := tar.NewWriter(writer)
	if err := tarWriter.WriteHeader(&tar.Header{Name: "payload.txt", Mode: mode, Size: int64(len(data))}); err != nil {
		return nil, err
	}
	if _, err := tarWriter.Write(data); err != nil {
		return nil, err
	}
	if err := tarWriter.Close(); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func writeTarGz(archive string, entries []tarEntry) error {
	file, err := os.Create(archive)
	if err != nil {
		return err
	}
	defer file.Close()
	gw := gzip.NewWriter(file)
	tw := tar.NewWriter(gw)
	for _, entry := range entries {
		if err := tw.WriteHeader(&tar.Header{Name: entry.name, Mode: entry.mode, Size: int64(len(entry.data))}); err != nil {
			return err
		}
		if _, err := tw.Write(entry.data); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gw.Close()
}

func findPackage(inventory *deliveryapis.PackageInventory, kind, name, version string) (deliveryapis.PackageEntry, bool) {
	for i := range inventory.Spec.Packages {
		pkg := inventory.Spec.Packages[i]
		if pkg.Kind == kind && pkg.Name == name && pkg.Version == version && pkg.Arch == archAMD64 {
			return pkg, true
		}
	}
	return deliveryapis.PackageEntry{}, false
}

func (v *verifier) checkf(ok bool, format string, args ...any) {
	v.total++
	message := fmt.Sprintf(format, args...)
	if !ok {
		v.failures = append(v.failures, message)
		fmt.Printf("not ok %02d - %s\n", v.total, message)
		return
	}
	v.passed++
	fmt.Printf("ok %02d - %s\n", v.total, message)
}
