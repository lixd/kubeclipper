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
)

type options struct {
	ChartPath        string
	Registry         string
	RepositoryPrefix string
	Name             string
}

func main() {
	opts := parseFlags()
	result, err := deliverypublisher.PublishHelmChart(deliverypublisher.HelmChartPublishRequest{
		ChartPath:        opts.ChartPath,
		Registry:         opts.Registry,
		RepositoryPrefix: opts.RepositoryPrefix,
		Name:             opts.Name,
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
	flag.Parse()
	if opts.ChartPath == "" || opts.Registry == "" || opts.RepositoryPrefix == "" {
		fmt.Fprintf(os.Stderr,
			"usage: %s --chart <chart.tgz> --registry <host:port> [--repository-prefix kubeclipper/charts] [--name <chart-name>]\n",
			os.Args[0])
		os.Exit(2)
	}
	return opts
}
