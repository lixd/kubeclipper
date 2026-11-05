/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package registry

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/pkg/delivery/releasemanifest"
)

func NewCmdRegistryExport(o *RegistryOptions) *cobra.Command {
	var manifestPath, output, username, password string
	cmd := &cobra.Command{
		Use:   "export [--manifest <release-manifest.yaml>] [--output <directory>]",
		Short: "export a complete multi-platform release and a separate Registry bootstrap bundle",
		Long: `Export all artifacts and every platform referenced by their original image indexes.
Release binaries use their embedded release manifest unless --manifest is provided.
The output directory receives kubeclipper-offline-<version>.tar.gz and
registry-<version>.tar.gz for bootstrapping
the offline Registry. Neither export nor import requires a container runtime.`,
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
			manifest, err := releasemanifest.Load(manifestPath)
			if err != nil {
				return err
			}
			source, err := sourceRegistryOptions(ctx, username, password)
			if err != nil {
				return err
			}
			result, err := releasemanifest.Export(ctx, manifest, releasemanifest.ExportOptions{
				Output: output, SourceOptions: source, Out: o.Out,
			})
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(o.Out, "release bundle: %s\nRegistry bootstrap bundle: %s\n", result.Bundle, result.RegistryBundle)
			return err
		},
	}
	cmd.Flags().StringVar(&manifestPath, "manifest", "", "release manifest file overriding the embedded release inventory")
	cmd.Flags().StringVar(&output, "output", "./offline", "output directory for the release and Registry bootstrap archives")
	cmd.Flags().StringVar(&username, "source-username", "", "source registry username (default: anonymous)")
	cmd.Flags().StringVar(&password, "source-password", "", "source registry password or token")
	return cmd
}
