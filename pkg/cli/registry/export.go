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

	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	"github.com/kubeclipper/kubeclipper/pkg/delivery/releasemanifest"
)

func NewCmdRegistryExport(o *RegistryOptions) *cobra.Command {
	var manifestPath, output, username, passwordFile string
	cmd := &cobra.Command{
		Use:   "export --manifest <release-manifest.yaml> --output <release.tar.gz>",
		Short: "export a complete multi-platform release and a separate Registry bootstrap bundle",
		Long: `Export all artifacts and every platform referenced by their original image indexes.
The output directory also receives registry-<version>.tar.gz for bootstrapping
the offline Registry. Neither export nor import requires a container runtime.`,
		Args: cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
			data, err := os.ReadFile(manifestPath)
			if err != nil {
				return err
			}
			manifest, err := releasemanifest.Parse(data)
			if err != nil {
				return err
			}
			source, err := sourceRegistryOptions(ctx, username, passwordFile)
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
	cmd.Flags().StringVar(&manifestPath, "manifest", "", "release manifest selecting the artifacts to export")
	cmd.Flags().StringVar(&output, "output", "", "release .tar.gz output; the separate Registry bundle is written alongside it")
	cmd.Flags().StringVar(&username, "source-username", "", "source registry username (default: anonymous)")
	cmd.Flags().StringVar(&passwordFile, "source-password-file", "", "file containing the source registry password or token")
	utils.CheckErr(cmd.MarkFlagRequired("manifest"))
	utils.CheckErr(cmd.MarkFlagRequired("output"))
	return cmd
}
