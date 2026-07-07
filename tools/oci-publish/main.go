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
	"strings"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliverypublisher "github.com/kubeclipper/kubeclipper/pkg/delivery/publisher"
)

type options struct {
	PackagePath      string
	Kind             string
	Name             string
	Version          string
	Arch             string
	Registry         string
	Profile          string
	ExternalContents externalContentFlags
}

type externalContentFlags []deliveryapis.ArtifactContent

func (f *externalContentFlags) String() string {
	return fmt.Sprint([]deliveryapis.ArtifactContent(*f))
}

func (f *externalContentFlags) Set(value string) error {
	fields := map[string]string{}
	for _, part := range strings.Split(value, ",") {
		key, val, ok := strings.Cut(part, "=")
		if !ok {
			return fmt.Errorf("external content part %q must be key=value", part)
		}
		fields[strings.TrimSpace(key)] = strings.TrimSpace(val)
	}
	content := deliveryapis.ArtifactContent{
		Name:      fields["name"],
		File:      fields["file"],
		MediaType: fields["mediaType"],
		Digest:    fields["digest"],
		Transport: deliveryapis.TransportRef{
			Type:   fields["transport"],
			Ref:    fields["ref"],
			Digest: fields["transportDigest"],
		},
	}
	if content.Transport.Digest == "" {
		content.Transport.Digest = fields["digest"]
	}
	if content.Name == "" || content.File == "" || content.Transport.Type == "" || content.Transport.Ref == "" || content.Transport.Digest == "" {
		return fmt.Errorf("external content requires name,file,transport,ref,digest or transportDigest")
	}
	*f = append(*f, content)
	return nil
}

func main() {
	opts := parseFlags()
	result, err := deliverypublisher.NewOCIArtifactPublisher().Publish(deliverypublisher.PublishRequest{
		PackagePath:      opts.PackagePath,
		Kind:             opts.Kind,
		Name:             opts.Name,
		Version:          opts.Version,
		Arch:             opts.Arch,
		Registry:         opts.Registry,
		ContentProfile:   opts.Profile,
		ExternalContents: []deliveryapis.ArtifactContent(opts.ExternalContents),
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "publish package failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("published %s/%s:%s\n", opts.Kind, opts.Name, opts.Version)
	fmt.Printf("ref: %s\n", result.Transport.Ref)
	fmt.Printf("digest: %s\n", result.Transport.Digest)
	for _, content := range result.Contents {
		fmt.Printf("content: %s file=%s digest=%s\n", content.Name, content.File, content.Digest)
	}
}

func parseFlags() options {
	var opts options
	flag.StringVar(&opts.PackagePath, "package", "", "path to offline package tar.gz")
	flag.StringVar(&opts.Kind, "kind", "", "package kind, e.g. k8s/cri/cni/binary/extension")
	flag.StringVar(&opts.Name, "name", "", "package name")
	flag.StringVar(&opts.Version, "version", "", "package version")
	flag.StringVar(&opts.Arch, "arch", "amd64", "package architecture")
	flag.StringVar(&opts.Registry, "registry", "", "OCI registry host:port")
	flag.StringVar(&opts.Profile, "profile", "", "optional content profile override")
	flag.Var(&opts.ExternalContents, "external-content", "external content descriptor: name=<name>,file=<file>,transport=<oci|helm-oci>,ref=<ref>,digest=<sha256>,mediaType=<type>")
	flag.Parse()

	if (opts.PackagePath == "" && len(opts.ExternalContents) == 0) || opts.Kind == "" || opts.Name == "" || opts.Version == "" || opts.Arch == "" || opts.Registry == "" {
		fmt.Fprintf(os.Stderr, "usage: %s [--package <package.tar.gz>] --kind <kind> --name <name> --version <version> --arch <arch> --registry <host:port> [--profile <profile>] [--external-content <descriptor>]\n", os.Args[0])
		os.Exit(2)
	}
	return opts
}
