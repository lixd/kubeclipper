/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package registry

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/component-base/version"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	registryconfig "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/delivery/releasemanifest"
)

const registrySyncExample = `
  # Sync all release platforms to an authenticated Harbor project
  kcctl registry sync --registry harbor.example.com/kubeclipper \
    --registry-username 'robot$kubeclipper-writer' \
    --registry-password-file harbor-token \
    --registry-ca-file harbor-ca.pem

  # Use a locally downloaded release manifest
  kcctl registry sync --manifest release-manifest-v2.0.0.yaml \
    --registry 10.0.0.10:5000 --registry-scheme http --arch amd64`

type RegistrySyncOptions struct {
	options.IOStreams

	Manifest      string
	Registry      string
	Username      string
	PasswordFile  string
	CAFile        string
	Scheme        string
	SkipTLSVerify bool
	Arch          string

	Version    string
	Downloader releasemanifest.Downloader
}

func NewRegistrySyncOptions(streams options.IOStreams) *RegistrySyncOptions {
	return &RegistrySyncOptions{
		IOStreams: streams,
		Scheme:    registryconfig.SchemeHTTPS,
		Arch:      releasemanifest.ArchAll,
		Version:   version.Get().GitVersion,
	}
}

func NewCmdRegistrySync(streams options.IOStreams) *cobra.Command {
	o := NewRegistrySyncOptions(streams)
	cmd := &cobra.Command{
		Use:                   "sync --registry <registry/project> [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "Sync KubeClipper release OCI artifacts to a Registry",
		Example:               registrySyncExample,
		Args:                  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return o.Run(cmd.Context())
		},
	}
	cmd.Flags().StringVar(
		&o.Manifest, "manifest", "", "Local release manifest. By default, download the manifest matching this kcctl version.",
	)
	cmd.Flags().StringVar(&o.Registry, "registry", "", "Target Registry/project prefix.")
	cmd.Flags().StringVar(&o.Username, "registry-username", "", "Target Registry username.")
	cmd.Flags().StringVar(&o.PasswordFile, "registry-password-file", "", "File containing the target Registry password or token.")
	cmd.Flags().StringVar(&o.CAFile, "registry-ca-file", "", "PEM CA file for the target Registry.")
	cmd.Flags().StringVar(&o.Scheme, "registry-scheme", registryconfig.SchemeHTTPS, "Target Registry scheme: https or http.")
	cmd.Flags().BoolVar(&o.SkipTLSVerify, "registry-skip-tls-verify", false, "Skip target Registry TLS certificate verification.")
	cmd.Flags().StringVar(&o.Arch, "arch", "all", "Architecture to sync: amd64, arm64, or all.")
	if err := cmd.MarkFlagRequired("registry"); err != nil {
		panic(err)
	}
	return cmd
}

func (o *RegistrySyncOptions) Run(ctx context.Context) error {
	if strings.TrimSpace(o.Registry) == "" {
		return fmt.Errorf("--registry is required")
	}
	if o.Arch != releasemanifest.ArchAMD64 && o.Arch != releasemanifest.ArchARM64 && o.Arch != releasemanifest.ArchAll {
		return fmt.Errorf("--arch must be amd64, arm64, or all")
	}
	config, err := (registryconfig.FileOptions{
		Scheme:        o.Scheme,
		Username:      o.Username,
		PasswordFile:  o.PasswordFile,
		CAFile:        o.CAFile,
		SkipTLSVerify: o.SkipTLSVerify,
	}).Resolve(o.Registry)
	if err != nil {
		return err
	}
	manifestData, err := o.loadManifest(ctx)
	if err != nil {
		return err
	}
	manifest, err := releasemanifest.Parse(manifestData)
	if err != nil {
		return err
	}
	if o.Manifest == "" && manifest.Metadata.Version != o.Version {
		return fmt.Errorf("downloaded release manifest version %s does not match kcctl version %s", manifest.Metadata.Version, o.Version)
	}
	out := o.Out
	if out == nil {
		out = io.Discard
	}
	result, err := releasemanifest.Sync(ctx, manifest, &releasemanifest.SyncOptions{
		Registry: o.Registry,
		Arch:     o.Arch,
		Config:   config,
		Out:      out,
	})
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "synced %d artifact(s); skipped %d matching artifact(s)\n", result.Copied, result.Skipped)
	return nil
}

func (o *RegistrySyncOptions) loadManifest(ctx context.Context) ([]byte, error) {
	if o.Manifest != "" {
		data, err := os.ReadFile(o.Manifest)
		if err != nil {
			return nil, fmt.Errorf("read release manifest: %w", err)
		}
		return data, nil
	}
	return o.Downloader.Download(ctx, o.Version)
}
