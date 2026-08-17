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
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"sigs.k8s.io/yaml"

	deliverypublisher "github.com/kubeclipper/kubeclipper/pkg/delivery/publisher"
)

const workdirMode = 0755

type manifest struct {
	Registry string         `json:"registry"`
	Packages []packageEntry `json:"packages"`
}

type packageEntry struct {
	Source  string `json:"source"`
	Kind    string `json:"kind"`
	Name    string `json:"name"`
	Version string `json:"version"`
	Arch    string `json:"arch"`
	Profile string `json:"profile,omitempty"`
}

type options struct {
	File    string
	WorkDir string
}

func main() {
	opts := parseFlags()
	if err := run(opts); err != nil {
		fmt.Fprintf(os.Stderr, "migrate legacy packages to OCI failed: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() options {
	var opts options
	pflag.StringVarP(&opts.File, "file", "f", "", "migration manifest yaml path")
	pflag.StringVar(&opts.WorkDir, "workdir", "", "optional download workspace")
	pflag.Parse()

	if strings.TrimSpace(opts.File) == "" {
		fmt.Fprintf(os.Stderr, "usage: %s --file <manifest.yaml> [--workdir <dir>]\n", os.Args[0])
		os.Exit(2)
	}
	return opts
}

func run(opts options) error {
	data, err := os.ReadFile(opts.File)
	if err != nil {
		return err
	}
	var cfg manifest
	if unmarshalErr := yaml.Unmarshal(data, &cfg); unmarshalErr != nil {
		return unmarshalErr
	}
	if strings.TrimSpace(cfg.Registry) == "" {
		return fmt.Errorf("manifest registry is required")
	}
	if len(cfg.Packages) == 0 {
		return fmt.Errorf("manifest packages are required")
	}

	workdir := opts.WorkDir
	if workdir == "" {
		workdir, err = os.MkdirTemp("", "kc-oci-migrate-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(workdir)
	} else if err := os.MkdirAll(workdir, workdirMode); err != nil {
		return err
	}

	client := &http.Client{Timeout: 30 * time.Minute}
	publisher := deliverypublisher.NewOCIArtifactPublisher()
	for i := range cfg.Packages {
		normalized, err := normalizeEntry(&cfg.Packages[i])
		if err != nil {
			return err
		}
		localPath, err := materializeSource(client, workdir, &normalized)
		if err != nil {
			return err
		}
		result, err := publisher.Publish(deliverypublisher.PublishRequest{
			PackagePath:    localPath,
			Kind:           normalized.Kind,
			Name:           normalized.Name,
			Version:        normalized.Version,
			Arch:           normalized.Arch,
			Registry:       cfg.Registry,
			ContentProfile: normalized.Profile,
		})
		if err != nil {
			return fmt.Errorf("publish %s/%s:%s: %w", normalized.Kind, normalized.Name, normalized.Version, err)
		}
		fmt.Printf("published %s/%s:%s\n", normalized.Kind, normalized.Name, normalized.Version)
		fmt.Printf("  source: %s\n", normalized.Source)
		fmt.Printf("  ref: %s\n", result.Transport.Ref)
		fmt.Printf("  digest: %s\n", result.Transport.Digest)
	}
	return nil
}

func normalizeEntry(input *packageEntry) (packageEntry, error) {
	pkg := *input
	pkg.Source = strings.TrimSpace(pkg.Source)
	pkg.Kind = strings.TrimSpace(pkg.Kind)
	pkg.Name = strings.TrimSpace(pkg.Name)
	pkg.Version = strings.TrimSpace(pkg.Version)
	pkg.Arch = strings.TrimSpace(pkg.Arch)
	pkg.Profile = strings.TrimSpace(pkg.Profile)
	if pkg.Source == "" {
		return packageEntry{}, fmt.Errorf("package source is required")
	}
	if pkg.Kind == "" || pkg.Name == "" || pkg.Version == "" {
		return packageEntry{}, fmt.Errorf("package kind, name and version are required")
	}
	if pkg.Arch == "" {
		pkg.Arch = "amd64"
	}
	return pkg, nil
}

func materializeSource(client *http.Client, workdir string, pkg *packageEntry) (string, error) {
	source := strings.TrimSpace(pkg.Source)
	if isRemoteURL(source) {
		return downloadToWorkdir(client, workdir, pkg, source)
	}
	if _, err := os.Stat(source); err != nil {
		return "", err
	}
	return source, nil
}

func isRemoteURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https")
}

func downloadToWorkdir(client *http.Client, workdir string, pkg *packageEntry, source string) (string, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, source, http.NoBody)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("download %s failed: http %d", source, resp.StatusCode)
	}
	filename := filepath.Base(req.URL.Path)
	if filename == "" || filename == "." || filename == "/" {
		filename = fmt.Sprintf("%s-%s-%s-%s.tar.gz", pkg.Kind, pkg.Name, pkg.Version, pkg.Arch)
	}
	filename = fmt.Sprintf("%s-%s-%s-%s-%s", pkg.Kind, pkg.Name, pkg.Version, pkg.Arch, filename)
	target := filepath.Join(workdir, filename)
	file, err := os.Create(target)
	if err != nil {
		return "", err
	}
	defer file.Close()
	if _, err = io.Copy(file, resp.Body); err != nil {
		return "", err
	}
	return target, nil
}
