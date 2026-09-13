/*
 *
 *  * Copyright 2024 KubeClipper Authors.
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

package registry

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/delivery/releasemanifest"
)

const syncLongDescription = `
 Copy every artifact of a release manifest (bootstrap and resource package
 images, Helm OCI charts, runtime images) from the registries recorded in the
 manifest into a target registry, verifying digests and refusing to overwrite
 a tag with different content.

 This is the supported way to mirror official KubeClipper releases into an
 internal or air-gapped registry for use with 'kcctl deploy --package-registry'.`

const syncExample = `
  # Mirror a release into a private registry
  kcctl registry sync --manifest release-manifest.yaml --target harbor.internal/kubeclipper

  # Mirror with credentials on both sides and a single architecture
  kcctl registry sync --manifest release-manifest.yaml --target harbor.internal/kubeclipper \
    --target-username robot --target-password-file /tmp/harbor.pw \
    --source-username user --source-password-file /tmp/src.pw --arch amd64

  # Show what would be copied without touching any registry
  kcctl registry sync --manifest release-manifest.yaml --target harbor.internal/kubeclipper --dry-run`

type SyncOptions struct {
	options.IOStreams

	ManifestPath  string
	Target        string
	Arch          string
	DryRun        bool
	TargetFiles   deliveryregistry.FileOptions
	SourceUser    string
	SourcePassFil string
}

func NewCmdRegistrySync(o *RegistryOptions) *cobra.Command {
	opts := &SyncOptions{IOStreams: o.IOStreams}
	cmd := &cobra.Command{
		Use:     "sync (--manifest <release-manifest.yaml>) (--target <registry>) [flags]",
		Short:   "mirror a release manifest into another registry",
		Long:    syncLongDescription,
		Example: syncExample,
		Args:    cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(opts.ValidateArgs())
			utils.CheckErr(opts.Run(cmd))
		},
	}

	cmd.Flags().StringVar(&opts.ManifestPath, "manifest", "", "release manifest YAML to sync. Required.")
	cmd.Flags().StringVar(&opts.Target, "target", "", "target registry prefix, e.g. harbor.internal/kubeclipper. Required.")
	cmd.Flags().StringVar(&opts.Arch, "arch", "all", "architecture to sync: amd64, arm64 or all")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "print the copy plan without contacting registries")
	cmd.Flags().StringVar(&opts.TargetFiles.Scheme, "target-scheme", "https", "target registry transport scheme: https or http")
	cmd.Flags().StringVar(&opts.TargetFiles.Username, "target-username", "", "target registry username or robot account")
	cmd.Flags().StringVar(&opts.TargetFiles.PasswordFile, "target-password-file", "", "file containing the target registry password or token")
	cmd.Flags().StringVar(&opts.TargetFiles.CAFile, "target-ca-file", "", "PEM CA file used to verify the target registry")
	cmd.Flags().BoolVar(&opts.TargetFiles.SkipTLSVerify, "target-skip-tls-verify", false, "skip target TLS verification (not recommended)")
	cmd.Flags().StringVar(&opts.SourceUser, "source-username", "", "source registry username (default: anonymous)")
	cmd.Flags().StringVar(&opts.SourcePassFil, "source-password-file", "", "file containing the source registry password or token")

	utils.CheckErr(cmd.MarkFlagRequired("manifest"))
	utils.CheckErr(cmd.MarkFlagRequired("target"))
	return cmd
}

func (o *SyncOptions) ValidateArgs() error {
	if o.ManifestPath == "" {
		return fmt.Errorf("--manifest is required")
	}
	if o.Target == "" {
		return fmt.Errorf("--target is required")
	}
	switch o.Arch {
	case "amd64", "arm64", "all":
	default:
		return fmt.Errorf("--arch must be amd64, arm64 or all")
	}
	if _, err := os.Stat(o.ManifestPath); err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	return nil
}

func (o *SyncOptions) Run(_ *cobra.Command) error {
	data, err := os.ReadFile(o.ManifestPath)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	manifest, err := releasemanifest.Parse(data)
	if err != nil {
		return err
	}
	if o.DryRun {
		return printSyncPlan(o.IOStreams.Out, manifest, o.Target, o.Arch)
	}

	targetConfig, err := o.resolveTargetConfig()
	if err != nil {
		return err
	}
	sourceOptions, err := o.sourceCraneOptions()
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	result, err := releasemanifest.Sync(ctx, manifest, &releasemanifest.SyncOptions{
		Registry:      o.Target,
		Arch:          o.Arch,
		Config:        targetConfig,
		Out:           o.IOStreams.Out,
		SourceOptions: sourceOptions,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(o.IOStreams.Out, "sync complete: %d copied, %d skipped (already present with matching digest)\n", result.Copied, result.Skipped)
	return nil
}

func (o *SyncOptions) resolveTargetConfig() (*deliveryregistry.Config, error) {
	if o.TargetFiles.Specified() {
		return o.TargetFiles.Resolve(o.Target)
	}
	return deliveryregistry.Resolve(o.Target)
}

func (o *SyncOptions) sourceCraneOptions() ([]crane.Option, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	opts := []crane.Option{crane.WithContext(ctx), crane.WithAuth(authn.Anonymous)}
	if o.SourceUser != "" {
		password, err := os.ReadFile(o.SourcePassFil)
		if err != nil {
			return nil, fmt.Errorf("read source password file: %w", err)
		}
		opts = []crane.Option{crane.WithContext(ctx),
			crane.WithAuth(&authn.Basic{Username: o.SourceUser, Password: string(password)})}
	} else if o.SourcePassFil != "" {
		return nil, fmt.Errorf("--source-password-file requires --source-username")
	}
	return opts, nil
}

func printSyncPlan(out io.Writer, manifest *releasemanifest.Manifest, target, arch string) error {
	for _, artifact := range manifest.Artifacts {
		if arch != "all" && !artifactSupportsArch(artifact, arch) {
			continue
		}
		fmt.Fprintf(out, "would copy %s -> %s/%s\n", artifact.Source, target, artifact.Target)
	}
	return nil
}

func artifactSupportsArch(artifact releasemanifest.Artifact, arch string) bool {
	if len(artifact.Platforms) == 0 {
		return true
	}
	for _, platform := range artifact.Platforms {
		if platform == "linux/"+arch {
			return true
		}
	}
	return false
}
