/*
 *
 *  * Copyright 2026 KubeClipper Authors.
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
	"flag"
	"fmt"
	"os"

	deliverypublisher "github.com/kubeclipper/kubeclipper/pkg/delivery/publisher"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
)

type options struct {
	ChartPath        string
	Registry         string
	RepositoryPrefix string
	Name             string
	RegistryFiles    deliveryregistry.FileOptions
}

func main() {
	opts := parseFlags()
	registryConfig, err := resolveRegistryConfig(opts.Registry, opts.RegistryFiles)
	if err != nil {
		fmt.Fprintf(os.Stderr, "configure package registry failed: %v\n", err)
		os.Exit(1)
	}
	result, err := deliverypublisher.PublishHelmChart(deliverypublisher.HelmChartPublishRequest{
		ChartPath:        opts.ChartPath,
		Registry:         opts.Registry,
		RepositoryPrefix: opts.RepositoryPrefix,
		Name:             opts.Name,
		RegistryConfig:   registryConfig,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "publish helm chart failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("published helm chart %s:%s\n", result.Name, result.Version)
	fmt.Printf("ref: %s\n", result.Ref)
	fmt.Printf("digest: %s\n", result.Digest)
	fmt.Printf("chartDigest: %s\n", result.ChartHash)
}

func parseFlags() options {
	var opts options
	flag.StringVar(&opts.ChartPath, "chart", "", "path to a Helm chart archive")
	flag.StringVar(&opts.Registry, "registry", "", "OCI registry host:port")
	flag.StringVar(&opts.RepositoryPrefix, "repository-prefix", "kubeclipper/charts", "Helm chart repository prefix")
	flag.StringVar(&opts.Name, "name", "", "optional chart name validation")
	addRegistryFlags(&opts.RegistryFiles)
	flag.Parse()
	if opts.ChartPath == "" || opts.Registry == "" || opts.RepositoryPrefix == "" {
		fmt.Fprintf(os.Stderr, "usage: %s --chart <chart.tgz> --registry <host:port> [--repository-prefix kubeclipper/charts] [--name <chart-name>]\n", os.Args[0])
		os.Exit(2)
	}
	return opts
}

func addRegistryFlags(opts *deliveryregistry.FileOptions) {
	flag.StringVar(&opts.Scheme, "registry-scheme", opts.Scheme, "registry transport scheme: https or http (default https)")
	flag.StringVar(&opts.Username, "registry-username", opts.Username, "registry username or robot account")
	flag.StringVar(&opts.PasswordFile, "registry-password-file", opts.PasswordFile, "file containing the registry password or token")
	flag.StringVar(&opts.CAFile, "registry-ca-file", opts.CAFile, "PEM CA file used to verify the registry")
	flag.BoolVar(&opts.SkipTLSVerify, "registry-skip-tls-verify", opts.SkipTLSVerify, "skip registry TLS verification (not recommended)")
}

func resolveRegistryConfig(registry string, opts deliveryregistry.FileOptions) (*deliveryregistry.Config, error) {
	if opts.Specified() {
		return opts.Resolve(registry)
	}
	return deliveryregistry.Resolve(registry)
}
