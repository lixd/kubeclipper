/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package main

import (
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
	} `json:"bootstrap"`
	Resources struct {
		K8s struct {
			Versions []string `json:"versions"`
		} `json:"k8s"`
		K8sExtension struct {
			Versions []string `json:"versions"`
		} `json:"k8sExtension"`
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
	fmt.Printf("release artifact/support policy consistency verified: %s\n", *manifestPath)
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
