/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

type buildManifest struct {
	Bootstrap struct {
		KubeClipperVersion string `json:"kubeclipperVersion"`
		EtcdVersion        string `json:"etcdVersion"`
		ConsoleVersion     string `json:"consoleVersion"`
		RegistryVersion    string `json:"registryVersion"`
	} `json:"bootstrap"`
	Resources struct {
		K8s struct {
			Versions []string `json:"versions"`
		} `json:"k8s"`
		K8sExtension struct {
			Versions []string `json:"versions"`
		} `json:"k8sExtension"`
		KCRuntime struct {
			Versions []string `json:"versions"`
		} `json:"kcRuntime"`
		RuntimeImageSets map[string]struct {
			Versions []string `json:"versions"`
		} `json:"runtimeImageSets"`
		CRI struct {
			Containerd struct {
				Versions []string `json:"versions"`
			} `json:"containerd"`
		} `json:"cri"`
		CNI struct {
			Calico struct {
				Versions []string `json:"versions"`
			} `json:"calico"`
		} `json:"cni"`
	} `json:"resources"`
}

func main() {
	manifestPath := flag.String("manifest", "packaging/resources.yaml", "offline resource build manifest")
	publishMatrix := flag.Bool("publish-matrix", false, "print the GitHub Actions OCI publish matrix as JSON")
	flag.Parse()
	data, err := os.ReadFile(*manifestPath)
	if err != nil {
		fatal(err)
	}
	var manifest buildManifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		fatal(fmt.Errorf("decode %s: %w", *manifestPath, err))
	}
	if err := verifyPolicyCoverage(manifest, deliveryapis.DefaultSupportPolicy()); err != nil {
		fatal(err)
	}
	if *publishMatrix {
		data, err := json.Marshal(buildPublishMatrix(&manifest))
		if err != nil {
			fatal(err)
		}
		fmt.Println(string(data))
		return
	}
	fmt.Printf("release artifact/support policy consistency verified: %s\n", *manifestPath)
}

type publishMatrixEntry struct {
	Component string `json:"component"`
	Version   string `json:"version"`
}

func buildPublishMatrix(manifest *buildManifest) []publishMatrixEntry {
	entries := []publishMatrixEntry{
		{Component: "bootstrap-kubeclipper", Version: manifest.Bootstrap.KubeClipperVersion},
		{Component: "bootstrap-etcd", Version: manifest.Bootstrap.EtcdVersion},
		{Component: "bootstrap-console", Version: manifest.Bootstrap.ConsoleVersion},
		{Component: "bootstrap-registry", Version: manifest.Bootstrap.RegistryVersion},
	}
	appendVersions := func(component string, versions []string) {
		for _, version := range versions {
			entries = append(entries, publishMatrixEntry{Component: component, Version: version})
		}
	}
	appendVersions("resource-k8s", manifest.Resources.K8s.Versions)
	appendVersions("resource-containerd", manifest.Resources.CRI.Containerd.Versions)
	appendVersions("resource-k8s-extension", manifest.Resources.K8sExtension.Versions)
	appendVersions("resource-calico", manifest.Resources.CNI.Calico.Versions)
	appendVersions("resource-kc-runtime", manifest.Resources.KCRuntime.Versions)
	for _, component := range []string{"nfs", "metallb"} {
		appendVersions("resource-"+component, manifest.Resources.RuntimeImageSets[component].Versions)
	}
	return entries
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func verifyPolicyCoverage(manifest buildManifest, policy *deliveryapis.SupportPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	advertised := map[string][]string{
		"cri/containerd":              manifest.Resources.CRI.Containerd.Versions,
		"cni/calico":                  manifest.Resources.CNI.Calico.Versions,
		"k8s-extension/k8s-extension": manifest.Resources.K8sExtension.Versions,
		"bootstrap/kubeclipper":       {manifest.Bootstrap.KubeClipperVersion},
		"bootstrap/etcd":              {manifest.Bootstrap.EtcdVersion},
	}
	referenced := make(map[string]map[string]struct{})
	for _, support := range policy.Spec.Policies {
		for _, slot := range support.ComponentSlots {
			for _, option := range slot.Options {
				key := option.Kind + "/" + option.Name
				if referenced[key] == nil {
					referenced[key] = make(map[string]struct{})
				}
				for _, version := range option.AllowedVersions {
					referenced[key][version] = struct{}{}
				}
			}
			key := slotKey(slot)
			if !contains(advertised[key], slot.Default.Version) {
				return fmt.Errorf("policy %s default %s:%s is not advertised by the release manifest", support.Name, key, slot.Default.Version)
			}
		}
	}
	for key, versions := range advertised {
		for _, version := range versions {
			if _, ok := referenced[key][version]; !ok {
				return fmt.Errorf("release artifact %s:%s is not referenced by the default support policy", key, version)
			}
		}
	}
	for _, version := range manifest.Resources.K8s.Versions {
		if !kubernetesVersionReferenced(version, policy) {
			return fmt.Errorf("release artifact k8s/k8s:%s is not referenced by the default support policy", version)
		}
	}
	return nil
}

func slotKey(slot deliveryapis.ComponentSlotRule) string {
	for _, option := range slot.Options {
		if option.Name == slot.Default.Name {
			return option.Kind + "/" + option.Name
		}
	}
	return "unknown/" + slot.Default.Name
}

func kubernetesVersionReferenced(version string, policy *deliveryapis.SupportPolicy) bool {
	for _, support := range policy.Spec.Policies {
		pattern := support.Match.KubernetesVersion
		if strings.HasSuffix(pattern, "*") && strings.HasPrefix(version, strings.TrimSuffix(pattern, "*")) || version == pattern {
			return true
		}
	}
	return false
}

func contains(values []string, wanted string) bool {
	sorted := sortedCopy(values)
	i := sort.SearchStrings(sorted, wanted)
	return i < len(sorted) && sorted[i] == wanted
}

func sortedCopy(values []string) []string {
	result := append([]string(nil), values...)
	sort.Strings(result)
	return result
}
