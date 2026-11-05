/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package source

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
)

const CNPackageRegistry = "registry.cn-hangzhou.aliyuncs.com/kubeclipper-release"

// Release builds bind these defaults to their published artifacts and OSS URL.
var (
	GlobalPackageRegistry = deliveryregistry.DefaultPackageRegistry
	GlobalReleaseBaseURL  = "https://github.com/kubeclipper/kubeclipper/releases/download"
	CNReleaseBaseURL      = "https://kubeclipper.oss-ap-southeast-1.aliyuncs.com/kc"
)

func domestic() (bool, error) {
	switch os.Getenv("KCZONE") {
	case "", "global":
		return false, nil
	case "cn":
		return true, nil
	default:
		return false, fmt.Errorf("KCZONE must be global or cn")
	}
}

// RegistryOverride selects an explicit registry or zone. Empty keeps a manifest's sources.
func RegistryOverride(configured string) (string, error) {
	cn, err := domestic()
	if err != nil {
		return "", err
	}
	if configured != "" {
		config := &deliveryregistry.Config{Registry: configured}
		if err := config.Validate(); err != nil {
			return "", err
		}
		return config.Registry, nil
	}
	if cn {
		return CNPackageRegistry, nil
	}
	if os.Getenv("KCZONE") == "global" {
		return GlobalPackageRegistry, nil
	}
	return "", nil
}

func Registry(configured string) (string, error) {
	registry, err := RegistryOverride(configured)
	if err != nil || registry != "" {
		return registry, err
	}
	return GlobalPackageRegistry, nil
}

func ReleaseBaseURL(explicit string) (string, error) {
	cn, err := domestic()
	if err != nil {
		return "", err
	}
	base := explicit
	if base == "" {
		base = os.Getenv("KC_DOWNLOAD_URL")
	}
	if base == "" {
		base = GlobalReleaseBaseURL
		if cn {
			base = CNReleaseBaseURL
		}
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", fmt.Errorf("release download source must be an HTTP or HTTPS base URL; configure KC_DOWNLOAD_URL")
	}
	return strings.TrimRight(base, "/"), nil
}
