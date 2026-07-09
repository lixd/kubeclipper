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
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryfetcher "github.com/kubeclipper/kubeclipper/pkg/delivery/fetcher"
	deliveryindexer "github.com/kubeclipper/kubeclipper/pkg/delivery/indexer"
	deliverypublisher "github.com/kubeclipper/kubeclipper/pkg/delivery/publisher"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
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

	fixtures := []packageFixture{
		{kind: "k8s", name: "k8s", version: "v1.36.0", profile: deliveryapis.ContentProfileK8s, files: []string{"configs.tar.gz", "images.tar.gz"}},
		{kind: "cri", name: "containerd", version: "2.1.0", profile: deliveryapis.ContentProfileRuntime, files: []string{"configs.tar.gz", "images.tar.gz"}},
		{kind: "cni", name: "calico", version: "v3.30.0", profile: deliveryapis.ContentProfileAddon, files: []string{"images.tar.gz", "charts.tgz"}},
		{kind: "bootstrap", name: "kubeclipper", version: "v1.8.0", profile: deliveryapis.ContentProfileBinary, files: []string{"kubeclipper-server", "kubeclipper-agent"}},
		{kind: "bootstrap", name: "etcd", version: "v3.5.15", profile: deliveryapis.ContentProfileBinary, files: []string{"etcd", "etcdctl", "etcdutl"}},
		{kind: "extension", name: "kubectl-terminal", version: "v1.0.0", profile: deliveryapis.ContentProfileExtension, files: []string{"images.tar.gz"}},
	}

	publisher := deliverypublisher.NewOCIArtifactPublisher()
	for _, fixture := range fixtures {
		archive, err := writePackageArchive(workdir, fixture)
		if err != nil {
			return err
		}
		result, err := publisher.Publish(deliverypublisher.PublishRequest{
			PackagePath:    archive,
			Kind:           fixture.kind,
			Name:           fixture.name,
			Version:        fixture.version,
			Arch:           "amd64",
			Registry:       registry,
			ContentProfile: fixture.profile,
		})
		if err != nil {
			return fmt.Errorf("publish %s/%s:%s: %w", fixture.kind, fixture.name, fixture.version, err)
		}
		v.check(result.Transport.Type == deliveryapis.TransportOCI, "publish %s/%s uses OCI transport", fixture.kind, fixture.name)
		v.check(strings.HasPrefix(result.Transport.Digest, "sha256:"), "publish %s/%s records manifest digest", fixture.kind, fixture.name)
	}

	inventory, err := deliveryindexer.NewRegistryPackageInventoryIndexer(nil).Refresh(ctx, registry)
	if err != nil {
		return err
	}
	v.check(inventory.Spec.Registry == registry, "inventory records source registry")
	v.check(len(inventory.Spec.Packages) >= len(fixtures), "inventory contains all published packages")
	for _, fixture := range fixtures {
		pkg, ok := findPackage(inventory, fixture.kind, fixture.name, fixture.version)
		v.check(ok, "inventory has %s/%s:%s", fixture.kind, fixture.name, fixture.version)
		if ok {
			v.check(pkg.Arch == "amd64", "inventory %s/%s arch comes from platform", fixture.kind, fixture.name)
			v.check(pkg.Transport.Type == deliveryapis.TransportOCI, "inventory %s/%s transport is OCI", fixture.kind, fixture.name)
			v.check(strings.HasPrefix(pkg.Transport.Digest, "sha256:"), "inventory %s/%s has digest", fixture.kind, fixture.name)
		}
	}

	policy := supportPolicy()
	plan, err := deliveryapis.ResolveArtifacts(inventory, policy, deliveryapis.ResolveRequest{
		KubernetesVersion: "v1.36.0",
		OS:                deliveryapis.DefaultPackageOS,
		Arch:              "amd64",
	})
	if err != nil {
		return err
	}
	v.check(len(plan.Components) == 3, "cluster install plan contains k8s/cri/cni only")
	for _, component := range plan.Components {
		v.check(component.Transport.Type == deliveryapis.TransportOCI, "plan %s/%s transport is OCI", component.Kind, component.Name)
		v.check(strings.HasPrefix(component.Transport.Digest, "sha256:"), "plan %s/%s is digest pinned", component.Kind, component.Name)
	}

	_, err = deliveryapis.ResolveArtifacts(inventory, policy, deliveryapis.ResolveRequest{
		KubernetesVersion: "v1.36.0",
		OS:                deliveryapis.DefaultPackageOS,
		Arch:              "arm64",
	})
	var resolverErr *deliveryapis.ResolverError
	v.check(errors.As(err, &resolverErr) && resolverErr.Code == deliveryapis.ErrArtifactArchUnavailable, "arm64 resolve fails with ArtifactArchUnavailable")

	agent, err := deliveryapis.ResolveBootstrapBinary(inventory, policy, deliveryapis.BootstrapBinaryResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates:        []deliveryapis.PackageCandidate{{Kind: "bootstrap", Name: "kubeclipper"}},
		Contents:          []string{"kubeclipper-agent"},
	})
	if err != nil {
		return err
	}
	etcdctl, err := deliveryapis.ResolveBootstrapBinary(inventory, policy, deliveryapis.BootstrapBinaryResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates:        []deliveryapis.PackageCandidate{{Kind: "bootstrap", Name: "etcd"}},
		Contents:          []string{"etcdctl"},
	})
	if err != nil {
		return err
	}
	extension, err := deliveryapis.ResolveExtensionArtifact(inventory, policy, deliveryapis.ExtensionResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates:        []deliveryapis.ExtensionCandidate{{Kind: "extension", Name: "kubectl-terminal"}},
	})
	if err != nil {
		return err
	}
	v.check(agent.Kind == "bootstrap" && agent.Name == "kubeclipper" && len(agent.Contents) == 1 && agent.Contents[0].Name == "kubeclipper-agent", "bootstrap agent resolves from kubeclipper package")
	v.check(etcdctl.Kind == "bootstrap" && etcdctl.Name == "etcd" && len(etcdctl.Contents) == 1 && etcdctl.Contents[0].Name == "etcdctl", "bootstrap etcdctl resolves from etcd package")
	v.check(extension.Kind == "extension" && extension.Name == "kubectl-terminal", "kubectl terminal resolves from extension package")
	_, err = deliveryapis.ResolveExtensionArtifact(inventory, policy, deliveryapis.ExtensionResolveRequest{
		Arch:              "amd64",
		KubernetesVersion: "v1.36.0",
		Candidates:        []deliveryapis.ExtensionCandidate{{Kind: "extension", Name: "inventory-only-extension"}},
	})
	v.check(errors.As(err, &resolverErr) && resolverErr.Code == deliveryapis.ErrUnsupportedComponentChoice, "extension inventory-only candidate is rejected by policy")

	fullPlan := *plan
	fullPlan.Components = append(append([]deliveryapis.ResolvedComponent{}, plan.Components...), agent, etcdctl, extension)
	fetcher := deliveryfetcher.NewOCIArtifactFetcher(false)
	fetchResult, err := fetcher.Fetch(ctx, &fullPlan)
	if err != nil {
		return err
	}
	v.check(len(fetchResult.Components) == 6, "fetcher materializes all resolved components")
	for _, component := range fetchResult.Components {
		v.check(component.Transport.Type == deliveryapis.TransportOCI, "fetched %s/%s transport remains OCI", component.Kind, component.Name)
		v.check(fileExists(component.ManifestPath), "fetched %s/%s manifest exists", component.Kind, component.Name)
		for name, path := range component.Files {
			v.check(fileExists(path), "fetched %s/%s content %s exists", component.Kind, component.Name, name)
		}
	}

	if len(v.failures) > 0 {
		return fmt.Errorf("verification checks failed: %s", strings.Join(v.failures, "; "))
	}
	fmt.Printf("OCI verification passed: %d/%d checks\n", v.passed, v.total)
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
				Default:   deliveryapis.ComponentChoice{Name: "containerd", Version: "2.1.0"},
				Options: []deliveryapis.ComponentOption{{
					Kind:            "cri",
					Name:            "containerd",
					AllowedVersions: []string{"2.1.0"},
				}},
			},
			{
				Slot:      "cni",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: "calico", Version: "v3.30.0"},
				Options: []deliveryapis.ComponentOption{{
					Kind:            "cni",
					Name:            "calico",
					AllowedVersions: []string{"v3.30.0"},
				}},
			},
			{
				Slot:      "bootstrap-kubeclipper",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: "kubeclipper", Version: "v1.8.0"},
				Options: []deliveryapis.ComponentOption{{
					Kind:            "bootstrap",
					Name:            "kubeclipper",
					AllowedVersions: []string{"v1.8.0"},
				}},
			},
			{
				Slot:      "bootstrap-etcd",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: "etcd", Version: "v3.5.15"},
				Options: []deliveryapis.ComponentOption{{
					Kind:            "bootstrap",
					Name:            "etcd",
					AllowedVersions: []string{"v3.5.15"},
				}},
			},
			{
				Slot:      "extension",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: "kubectl-terminal", Version: "v1.0.0"},
				Options: []deliveryapis.ComponentOption{{
					Kind:            "extension",
					Name:            "kubectl-terminal",
					AllowedVersions: []string{"v1.0.0"},
				}},
			},
		},
	}}
	return policy
}

func writePackageArchive(workdir string, fixture packageFixture) (string, error) {
	root := filepath.Join(workdir, "archives", fixture.kind, fixture.name, fixture.version)
	base := filepath.Join(root, fixture.name, fixture.version, "amd64")
	if err := os.MkdirAll(base, 0755); err != nil {
		return "", err
	}
	for _, file := range fixture.files {
		mode := os.FileMode(0644)
		if fixture.profile == deliveryapis.ContentProfileBinary {
			mode = 0755
			if err := os.WriteFile(filepath.Join(base, file), []byte(fixture.kind+"/"+fixture.name+"/"+fixture.version+"/"+file+"\n"), mode); err != nil {
				return "", err
			}
			continue
		}
		if err := writePayloadTarGz(filepath.Join(base, file), fixture, file, mode); err != nil {
			return "", err
		}
	}
	archive := filepath.Join(root, fixture.name+".tar.gz")
	return archive, writeTarGz(archive, root)
}

func writePayloadTarGz(path string, fixture packageFixture, file string, mode os.FileMode) error {
	payloadRoot, err := os.MkdirTemp(filepath.Dir(path), "payload-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(payloadRoot)
	payloadFile := filepath.Join(payloadRoot, "payload.txt")
	if err = os.WriteFile(payloadFile, []byte(fixture.kind+"/"+fixture.name+"/"+fixture.version+"/"+file+"\n"), mode); err != nil {
		return err
	}
	return writeTarGz(path, payloadRoot)
}

func writeTarGz(archive, root string) error {
	file, err := os.Create(archive)
	if err != nil {
		return err
	}
	defer file.Close()
	gw := gzip.NewWriter(file)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()
	return filepath.Walk(root, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Clean(path) == filepath.Clean(archive) {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(rel)
		if err = tw.WriteHeader(header); err != nil {
			return err
		}
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()
		_, err = io.Copy(tw, src)
		return err
	})
}

func findPackage(inventory *deliveryapis.PackageInventory, kind, name, version string) (deliveryapis.PackageEntry, bool) {
	for _, pkg := range inventory.Spec.Packages {
		if pkg.Kind == kind && pkg.Name == name && pkg.Version == version && pkg.Arch == "amd64" {
			return pkg, true
		}
	}
	return deliveryapis.PackageEntry{}, false
}

func fileExists(path string) bool {
	if path == "" {
		return false
	}
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

func (v *verifier) check(ok bool, format string, args ...interface{}) {
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
