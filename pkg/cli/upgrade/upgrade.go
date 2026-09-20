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

package upgrade

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/util/sets"

	cmdoptions "github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/deploy"
	"github.com/kubeclipper/kubeclipper/pkg/cli/logger"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryfetcher "github.com/kubeclipper/kubeclipper/pkg/delivery/fetcher"
	deliveryindexer "github.com/kubeclipper/kubeclipper/pkg/delivery/indexer"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/delivery/releasemanifest"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"

	"github.com/spf13/cobra"
)

const (
	longDescription = `
  Upgrade kubeclipper platform components from a release manifest served by an
  OCI Package Registry.

  The upgrade consumes the same artifacts the platform is deployed from: a
  ReleaseManifest plus the bootstrap package images it pins. kcctl resolves the
  target revision from the manifest, downloads and digest-verifies every
  artifact for every target node architecture BEFORE stopping any service, and
  then replaces kubeclipper-server and kubeclipper-agent one node at a time
  with a health check between nodes. A failed node is restored from its backup
  and the rollout stops there; already upgraded nodes stay upgraded.

  --version downloads the release manifest of a stable vX.Y.Z release with
  checksum verification; --manifest reads a local manifest file, which is the
  supported path for offline environments after the release bundle has been
  synced into an internal Package Registry. The two flags are mutually
  exclusive and exactly one of them is required.`
	upgradeExample = `
  # Upgrade server and agent of the whole platform to a released version
  kcctl upgrade all --version v2.0.4

  # Upgrade only the server from a local release manifest (offline Registry)
  kcctl upgrade server --manifest release-manifest-v2.0.4.yaml

  # Plain-HTTP internal Registry (or persist the scheme in the package
  # registry config file / KC_PACKAGE_REGISTRY_CONFIG instead)
  kcctl upgrade all --manifest release-manifest.yaml --package-registry-scheme http`
)

const (
	bootstrapPackageKind = "bootstrap"
	bootstrapPackageName = "kubeclipper"

	serverBinaryName = "kubeclipper-server"
	agentBinaryName  = "kubeclipper-agent"

	roleServer = "server"
	roleAgent  = "agent"

	upgradeStagingDir = "/tmp/kubeclipper-upgrade"

	nodeHealthzTimeout = 3 * time.Second
	nodeHealthzWait    = 180 * time.Second
	nodeHealthzTick    = 3 * time.Second
	minFreeDiskMB      = 256
)

type UpgradeOptions struct {
	deployConfig *cmdoptions.DeployConfig
	CliOpts      *cmdoptions.CliOptions
	client       *kc.Client
	SSHConfig    *sshutils.SSH
	cmdoptions.IOStreams

	component    string
	version      string
	manifestPath string
	registryOpts deliveryregistry.FileOptions

	manifest       *releasemanifest.Manifest
	artifact       *releasemanifest.Artifact
	registry       string
	targetRef      string
	registryConfig *deliveryregistry.Config

	platformVersion string
	platformRev     string
	targetVersion   string
	targetRevision  string
}

func NewUpgradeOptions(stream cmdoptions.IOStreams) *UpgradeOptions {
	return &UpgradeOptions{
		CliOpts:      cmdoptions.NewCliOptions(),
		client:       nil,
		SSHConfig:    sshutils.NewSSH(),
		IOStreams:    stream,
		deployConfig: cmdoptions.NewDeployOptions(),
	}
}

func NewCmdUpgrade(stream cmdoptions.IOStreams) *cobra.Command {
	o := NewUpgradeOptions(stream)
	cmd := &cobra.Command{
		Use:                   "upgrade ( component ) ( --version vX.Y.Z | --manifest <file> ) [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "upgrade kubeclipper platform or components from a release manifest",
		Long:                  longDescription,
		Example:               upgradeExample,
		Args:                  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete())
			utils.CheckErr(o.Validate(cmd, args))
			utils.CheckErr(o.RunUpgrade())
		},
	}
	cmd.Flags().StringVar(&o.version, "version", o.version, "Stable release version to upgrade to, e.g. v2.0.4; the release manifest is downloaded and checksum-verified")
	cmd.Flags().StringVar(&o.manifestPath, "manifest", o.manifestPath, "Path to a local release manifest file; the supported path for offline Package Registries")
	cmd.Flags().StringVar(&o.registryOpts.Scheme, "package-registry-scheme", o.registryOpts.Scheme,
		"Package Registry transport scheme: https or http (default https)")
	cmd.Flags().StringVar(&o.registryOpts.Username, "package-registry-username", o.registryOpts.Username,
		"Package Registry username or robot account")
	cmd.Flags().StringVar(&o.registryOpts.PasswordFile, "package-registry-password-file", o.registryOpts.PasswordFile,
		"File containing the Package Registry password or token")
	cmd.Flags().StringVar(&o.registryOpts.CAFile, "package-registry-ca-file", o.registryOpts.CAFile,
		"PEM CA file used to verify the Package Registry")
	cmd.Flags().BoolVar(&o.registryOpts.SkipTLSVerify, "package-registry-skip-tls-verify", o.registryOpts.SkipTLSVerify,
		"Skip Package Registry TLS verification (not recommended)")

	return cmd
}

func (o *UpgradeOptions) Complete() error {
	var err error
	if err = o.CliOpts.Complete(); err != nil {
		return err
	}
	o.client, err = kc.FromConfig(o.CliOpts.ToRawConfig())
	if err != nil {
		return err
	}

	o.deployConfig, err = deploy.GetDeployConfig(context.Background(), o.client, true)
	if err != nil {
		return errors.WithMessage(err, "get online deploy-config failed")
	}
	// All node operations go through o.SSHConfig; without this wiring the
	// empty default config makes sshutils fall back to running every command
	// locally on the kcctl host while logging the target host names.
	o.SSHConfig = o.deployConfig.SSHConfig
	return nil
}

func (o *UpgradeOptions) Validate(cmd *cobra.Command, args []string) error {
	if o.deployConfig.SSHConfig.PkFile == "" && o.deployConfig.SSHConfig.Password == "" {
		return utils.UsageErrorf(cmd, "one of pkfile or password must be specified, please config it in %s", o.deployConfig.Config)
	}
	o.component = args[0]
	switch o.component {
	case cmdoptions.UpgradeServer, cmdoptions.UpgradeAgent, cmdoptions.UpgradeAll:
	case cmdoptions.UpgradeConsole, cmdoptions.UpgradeKcctl:
		return utils.UsageErrorf(cmd, "component %q upgrade is not supported yet; supported components are [ all | server | agent ]", o.component)
	default:
		return utils.UsageErrorf(cmd, "unsupported upgrade component %q, support [ all | server | agent ] now", o.component)
	}

	if o.version != "" && o.manifestPath != "" {
		return utils.UsageErrorf(cmd, "--version and --manifest are mutually exclusive")
	}
	if o.version == "" && o.manifestPath == "" {
		return utils.UsageErrorf(cmd, "exactly one of --version or --manifest must be specified")
	}

	if err := o.loadManifest(); err != nil {
		return err
	}
	artifact := o.manifest.BootstrapKubeClipperArtifact()
	if artifact == nil {
		return fmt.Errorf("release manifest contains no bootstrap/kubeclipper package artifact")
	}
	o.artifact = artifact
	o.registry = strings.TrimRight(o.manifest.Registries.Package, "/")
	o.targetRef = o.registry + "/" + artifact.Target
	o.targetVersion = artifact.Component.Version
	o.targetRevision = o.manifest.Metadata.SourceRevision

	if err := o.checkVersionPolicy(); err != nil {
		return err
	}

	registryConfig, err := o.resolveRegistryConfig()
	if err != nil {
		return err
	}
	o.registryConfig = registryConfig

	if len(o.deployConfig.ServerIPs)%2 == 0 {
		return fmt.Errorf("server node must be odd number")
	}
	return nil
}

func (o *UpgradeOptions) loadManifest() error {
	if o.manifestPath != "" {
		data, err := os.ReadFile(o.manifestPath)
		if err != nil {
			return fmt.Errorf("read release manifest: %w", err)
		}
		manifest, err := releasemanifest.Parse(data)
		if err != nil {
			return err
		}
		o.manifest = manifest
		return nil
	}
	if !releasemanifest.IsStableVersion(o.version) {
		return fmt.Errorf("--version %q is not a stable vX.Y.Z release; download the release manifest and use --manifest instead", o.version)
	}
	downloader := releasemanifest.Downloader{}
	data, err := downloader.Download(context.Background(), o.version)
	if err != nil {
		return fmt.Errorf("download release manifest for %s: %w (offline environments must sync the release bundle and use --manifest)", o.version, err)
	}
	manifest, err := releasemanifest.Parse(data)
	if err != nil {
		return err
	}
	o.manifest = manifest
	return nil
}

func (o *UpgradeOptions) checkVersionPolicy() error {
	versionInfo, err := o.client.Version(context.TODO())
	if err != nil {
		return fmt.Errorf("query platform version: %w", err)
	}
	o.platformVersion = versionInfo.GitVersion
	o.platformRev = versionInfo.GitCommit
	return evaluateVersionPolicy(o.platformVersion, o.platformRev, o.targetVersion, o.targetRevision)
}

// evaluateVersionPolicy implements the version contract of the upgrade:
// identical revisions are an idempotent no-op, implicit downgrades are
// refused, and same-version/different-revision re-installs are allowed.
// The downgrade check runs BEFORE the revision idempotency short-circuit:
// a manifest that pins the current revision but claims an older version
// must not slip through as an idempotent re-run.
func evaluateVersionPolicy(platformVersion, platformRevision, targetVersion, targetRevision string) error {
	cmp, comparable := deliveryapis.CompareVersions(platformVersion, targetVersion)
	if comparable && cmp > 0 {
		return fmt.Errorf("refusing implicit downgrade: platform %s is newer than target %s", platformVersion, targetVersion)
	}
	if platformRevision != "" && platformRevision == targetRevision {
		logger.Infof("platform API already reports revision %s; nodes are verified individually", platformRevision)
		return nil
	}
	if comparable {
		return nil
	}
	if platformVersion == targetVersion {
		// Same version, different revision: a re-install of the same release.
		return nil
	}
	logger.Warnf("cannot compare platform version %q with target %q; proceeding with upgrade", platformVersion, targetVersion)
	return nil
}

func (o *UpgradeOptions) resolveRegistryConfig() (*deliveryregistry.Config, error) {
	if o.registryOpts.Specified() {
		return o.registryOpts.Resolve(o.registry)
	}
	return deliveryregistry.Resolve(o.registry)
}

type nodePlan struct {
	role            string
	host            string
	arch            string
	currentRevision string
}

type rolloutPlan struct {
	nodes []nodePlan
}

type archArtifacts struct {
	serverPath string
	agentPath  string
}

func (o *UpgradeOptions) targetRoles() []string {
	switch o.component {
	case cmdoptions.UpgradeServer:
		return []string{roleServer}
	case cmdoptions.UpgradeAgent:
		return []string{roleAgent}
	default:
		return []string{roleServer, roleAgent}
	}
}

func (o *UpgradeOptions) RunUpgrade() error {
	plan, err := o.buildRolloutPlan()
	if err != nil {
		return err
	}
	if len(plan.nodes) == 0 {
		logger.Infof("no %s nodes to upgrade", o.component)
		return nil
	}
	o.printPlan(plan)

	fetched, err := o.preflight(plan)
	if err != nil {
		return err
	}
	if err = o.runRollout(plan, fetched); err != nil {
		return err
	}
	return o.cleanup(plan)
}

func (o *UpgradeOptions) buildRolloutPlan() (*rolloutPlan, error) {
	roles := sets.NewString(o.targetRoles()...)
	plan := &rolloutPlan{}
	for _, host := range o.deployConfig.ServerIPs {
		if !roles.Has(roleServer) {
			break
		}
		if err := o.appendNode(plan, roleServer, host); err != nil {
			return nil, err
		}
	}
	for _, host := range o.deployConfig.Agents.ListIP() {
		if !roles.Has(roleAgent) {
			break
		}
		if err := o.appendNode(plan, roleAgent, host); err != nil {
			return nil, err
		}
	}
	plan.nodes = dedupNodes(plan.nodes)
	return plan, nil
}

// dedupNodes drops (role, host) duplicates from the rollout plan. A node may
// legitimately appear once per role (a combined server+agent host), but a
// repeated (role, host) entry would stop and replace the same service twice.
func dedupNodes(nodes []nodePlan) []nodePlan {
	type roleHost struct{ role, host string }
	seen := make(map[roleHost]struct{}, len(nodes))
	kept := nodes[:0]
	for _, node := range nodes {
		key := roleHost{node.role, node.host}
		if _, dup := seen[key]; dup {
			logger.Warnf("drop duplicate %s %s from the upgrade plan", node.role, node.host)
			continue
		}
		seen[key] = struct{}{}
		kept = append(kept, node)
	}
	return kept
}

func (o *UpgradeOptions) appendNode(plan *rolloutPlan, role, host string) error {
	arch, err := o.probeNodeArch(host)
	if err != nil {
		return err
	}
	plan.nodes = append(plan.nodes, nodePlan{
		role:            role,
		host:            host,
		arch:            arch,
		currentRevision: o.probeNodeRevision(host, role),
	})
	return nil
}

func (o *UpgradeOptions) probeNodeArch(host string) (string, error) {
	result, err := sshutils.SSHCmdWithSudo(o.SSHConfig, host, "uname -m")
	if err != nil {
		return "", fmt.Errorf("probe architecture on %s: %w", host, err)
	}
	if result.ExitCode != 0 {
		return "", fmt.Errorf("probe architecture on %s: exit %d: %s", host, result.ExitCode, strings.TrimSpace(result.Stderr))
	}
	return normalizeUnameArch(strings.TrimSpace(result.Stdout))
}

func normalizeUnameArch(arch string) (string, error) {
	switch arch {
	case "x86_64":
		return "amd64", nil
	case "aarch64":
		return "arm64", nil
	default:
		return "", fmt.Errorf("unsupported node architecture %q", arch)
	}
}

func (o *UpgradeOptions) probeNodeRevision(host, role string) string {
	result, err := sshutils.SSHCmdWithSudo(o.SSHConfig, host, fmt.Sprintf("/usr/local/bin/%s version -o json", binaryForRole(role)))
	if err != nil || result.ExitCode != 0 {
		return ""
	}
	var info nodeVersionInfo
	if err := json.Unmarshal([]byte(strings.TrimSpace(result.Stdout)), &info); err != nil {
		return ""
	}
	return info.GitCommit
}

type nodeVersionInfo struct {
	GitVersion string `json:"gitVersion"`
	GitCommit  string `json:"gitCommit"`
}

func binaryForRole(role string) string {
	if role == roleAgent {
		return agentBinaryName
	}
	return serverBinaryName
}

func (o *UpgradeOptions) printPlan(plan *rolloutPlan) {
	logger.Infof("upgrade plan: target %s (revision %s) from manifest %s, registry %s", o.targetVersion, shortRev(o.targetRevision), manifestName(o), o.registry)
	for _, node := range plan.nodes {
		logger.Infof("  node %s role=%s arch=%s current=%s", node.host, node.role, node.arch, nodeRevisionLabel(node.currentRevision))
	}
}

func manifestName(o *UpgradeOptions) string {
	if o.manifestPath != "" {
		return o.manifestPath
	}
	return "downloaded for " + o.version
}

func shortRev(rev string) string {
	if len(rev) > 12 {
		return rev[:12]
	}
	return rev
}

func nodeRevisionLabel(rev string) string {
	if rev == "" {
		return "unknown"
	}
	return shortRev(rev)
}

// preflight resolves and verifies every artifact for every target
// architecture and checks node connectivity before any service is stopped.
func (o *UpgradeOptions) preflight(plan *rolloutPlan) (map[string]archArtifacts, error) {
	ctx := context.Background()
	if err := o.verifyTagBinding(ctx); err != nil {
		return nil, err
	}
	arches := sets.NewString()
	for i := range plan.nodes {
		arches.Insert(plan.nodes[i].arch)
	}
	fetched := make(map[string]archArtifacts, arches.Len())
	for _, arch := range arches.List() {
		artifacts, err := o.fetchPlatformPackage(ctx, arch)
		if err != nil {
			return nil, err
		}
		fetched[arch] = *artifacts
	}
	for i := range plan.nodes {
		if err := o.checkNodeReadiness(&plan.nodes[i]); err != nil {
			return nil, err
		}
	}
	return fetched, nil
}

// verifyTagBinding proves the registry tag the upgrade is about to pull is the
// one the release manifest pinned: either the tag digest still matches the
// manifest artifact digest, or (after a per-arch re-tag of a synced bundle)
// the image carries the release source revision label. Pulling by digest alone
// is impossible for a tag whose child digest is unknown to this manifest.
func (o *UpgradeOptions) verifyTagBinding(ctx context.Context) error {
	opts, err := o.registryConfig.CraneOptions(ctx)
	if err != nil {
		return err
	}
	desc, err := crane.Get(o.targetRef, opts...)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", o.targetRef, err)
	}
	if desc.Digest.String() == o.artifact.Digest {
		return nil
	}
	image, err := desc.Image()
	if err != nil {
		return fmt.Errorf("resolve %s: read image: %w", o.targetRef, err)
	}
	configFile, err := image.ConfigFile()
	if err != nil {
		return fmt.Errorf("resolve %s: read image config: %w", o.targetRef, err)
	}
	if configFile.Config.Labels["org.opencontainers.image.revision"] == o.targetRevision {
		logger.Warnf("registry tag %s digest %s differs from manifest digest %s; source revision %s matches", o.targetRef, desc.Digest, o.artifact.Digest, o.targetRevision)
		return nil
	}
	return fmt.Errorf("registry tag %s digest %s does not match the release manifest digest %s; refusing to upgrade from a repointed tag", o.targetRef, desc.Digest, o.artifact.Digest)
}

func (o *UpgradeOptions) fetchPlatformPackage(ctx context.Context, arch string) (*archArtifacts, error) {
	repository, tag, ok := strings.Cut(o.artifact.Target, ":")
	if !ok || tag == "" {
		return nil, fmt.Errorf("release manifest artifact target %q carries no tag", o.artifact.Target)
	}
	if tag != o.targetVersion {
		return nil, fmt.Errorf("release manifest artifact tag %q does not match component version %q", tag, o.targetVersion)
	}
	indexer := deliveryindexer.NewRegistryPackageInventoryIndexerWithConfig(o.registryConfig)
	inventory, err := indexer.IndexPackageRepositories(ctx, o.registry, []string{repository})
	if err != nil {
		return nil, fmt.Errorf("index package repository %s/%s: %w", o.registry, repository, err)
	}
	entry := selectPackageEntry(inventory, o.targetVersion, arch)
	if entry == nil {
		return nil, fmt.Errorf("registry %s has no %s/%s package version %s for arch %s; sync the release bundle first", o.registry, bootstrapPackageKind, bootstrapPackageName, o.targetVersion, arch)
	}
	if entry.SourceRevision != "" && entry.SourceRevision != o.targetRevision {
		return nil, fmt.Errorf("package %s/%s:%s(%s) source revision %s does not match release manifest revision %s", entry.Kind, entry.Name, entry.Version, entry.Arch, entry.SourceRevision, o.targetRevision)
	}
	component := deliveryapis.ResolvedComponent{
		Slot:      "upgrade-" + bootstrapPackageName,
		Kind:      entry.Kind,
		Name:      entry.Name,
		Version:   entry.Version,
		OS:        entry.OS,
		Arch:      entry.Arch,
		Required:  true,
		Transport: entry.Transport,
		Contents:  entry.Contents,
	}
	result, err := deliveryfetcher.NewOCIArtifactFetcherWithConfig(false, o.registryConfig).Fetch(ctx, &deliveryapis.ResolvedArtifactPlan{
		OS:         deliveryapis.DefaultPackageOS,
		Arch:       arch,
		Components: []deliveryapis.ResolvedComponent{component},
	})
	if err != nil {
		return nil, fmt.Errorf("fetch %s/%s:%s for %s: %w", bootstrapPackageKind, bootstrapPackageName, o.targetVersion, arch, err)
	}
	if len(result.Components) != 1 {
		return nil, fmt.Errorf("fetch %s/%s:%s for %s: unexpected fetch result", bootstrapPackageKind, bootstrapPackageName, o.targetVersion, arch)
	}
	artifacts := &archArtifacts{}
	for name, filePath := range result.Components[0].Files {
		switch name {
		case serverBinaryName:
			artifacts.serverPath = filePath
		case agentBinaryName:
			artifacts.agentPath = filePath
		}
	}
	if o.component == cmdoptions.UpgradeAll || o.component == cmdoptions.UpgradeServer {
		if artifacts.serverPath == "" {
			return nil, fmt.Errorf("package %s:%s(%s) has no %s payload", bootstrapPackageName, o.targetVersion, arch, serverBinaryName)
		}
	}
	if o.component == cmdoptions.UpgradeAll || o.component == cmdoptions.UpgradeAgent {
		if artifacts.agentPath == "" {
			return nil, fmt.Errorf("package %s:%s(%s) has no %s payload", bootstrapPackageName, o.targetVersion, arch, agentBinaryName)
		}
	}
	return artifacts, nil
}

func selectPackageEntry(inventory *deliveryapis.PackageInventory, version, arch string) *deliveryapis.PackageEntry {
	if inventory == nil {
		return nil
	}
	for i := range inventory.Spec.Packages {
		pkg := &inventory.Spec.Packages[i]
		if pkg.Kind == bootstrapPackageKind && pkg.Name == bootstrapPackageName && pkg.Version == version && pkg.Arch == arch {
			return pkg
		}
	}
	return nil
}

func (o *UpgradeOptions) checkNodeReadiness(node *nodePlan) error {
	result, err := sshutils.SSHCmdWithSudo(o.SSHConfig, node.host, "df -Pm /usr/local | awk 'NR==2 {print $4}'")
	if err != nil {
		return fmt.Errorf("check disk space on %s: %w", node.host, err)
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("check disk space on %s: exit %d: %s", node.host, result.ExitCode, strings.TrimSpace(result.Stderr))
	}
	freeMB, parseErr := strconv.Atoi(strings.TrimSpace(result.Stdout))
	if parseErr != nil {
		return fmt.Errorf("check disk space on %s: unexpected df output %q", node.host, strings.TrimSpace(result.Stdout))
	}
	if freeMB < minFreeDiskMB {
		return fmt.Errorf("node %s has only %dMB free under /usr/local, at least %dMB is required", node.host, freeMB, minFreeDiskMB)
	}
	if node.role == roleServer && !o.nodeHealthzOK(node.host) {
		return fmt.Errorf("healthz endpoint of server node %s is unreachable; fix connectivity to %s:%v/healthz before upgrading", node.host, node.host, o.deployConfig.ServerPort)
	}
	return nil
}

func (o *UpgradeOptions) runRollout(plan *rolloutPlan, fetched map[string]archArtifacts) error {
	upgraded := make([]nodePlan, 0, len(plan.nodes))
	for i := range plan.nodes {
		node := plan.nodes[i]
		// Re-probe right before touching the node: the plan-time revision is
		// stale for long rollouts and for a second upgrade invocation racing
		// this one; both would otherwise replace an already-upgraded binary
		// and clobber its backup.
		if current := o.probeNodeRevision(node.host, node.role); current != "" {
			node.currentRevision = current
		}
		if node.currentRevision != "" && node.currentRevision == o.targetRevision {
			logger.Infof("skip %s %s: already at revision %s", node.role, node.host, shortRev(o.targetRevision))
			continue
		}
		artifacts := fetched[node.arch]
		var err error
		switch node.role {
		case roleServer:
			err = o.upgradeNodeBinary(node.host, roleServer, artifacts.serverPath)
			if err == nil {
				err = o.waitServerHealthy(node.host)
			}
		case roleAgent:
			err = o.upgradeNodeBinary(node.host, roleAgent, artifacts.agentPath)
			if err == nil {
				err = o.waitAgentActive(node.host)
			}
		default:
			err = fmt.Errorf("unsupported role %q", node.role)
		}
		if err != nil {
			logger.Errorf("upgrade %s on %s failed: %v", node.role, node.host, err)
			o.restoreNodeBinary(node.host, node.role)
			// Keep the staging dir (and the backup inside it) so the failure
			// scene stays inspectable; /tmp is cleared on reboot.
			return fmt.Errorf("upgrade stopped at %s %s: %w; the node was restored to the previous binary, already upgraded nodes were not rolled back", node.role, node.host, err)
		}
		upgraded = append(upgraded, node)
		logger.Infof("upgraded %s %s to %s (revision %s)", node.role, node.host, o.targetVersion, shortRev(o.targetRevision))
	}
	return o.verifyPlatform(plan)
}

func (o *UpgradeOptions) upgradeNodeBinary(host, role, localPath string) error {
	binaryName := binaryForRole(role)
	remoteBin := path.Join("/usr/local/bin", binaryName)
	backupPath := path.Join(upgradeStagingDir, "backup", binaryName)
	remotePath := path.Join(upgradeStagingDir, binaryName)
	service := "kc-" + role

	// Keep the first backup taken on this node: a repeated pass (a second
	// invocation that raced past the skip check) must not replace the
	// pre-upgrade binary with the already-upgraded one, or restore would
	// roll forward instead of back.
	steps := []string{
		fmt.Sprintf("mkdir -p %s", path.Join(upgradeStagingDir, "backup")),
		fmt.Sprintf("systemctl stop %s", service),
		fmt.Sprintf("[ -f %s ] || cp -a %s %s", backupPath, remoteBin, backupPath),
		fmt.Sprintf("install -m 0755 %s %s", remotePath, remoteBin),
		fmt.Sprintf("systemctl start %s", service),
	}
	if err := utils.SendPackageV2(o.SSHConfig, localPath, []string{host}, upgradeStagingDir, nil, nil); err != nil {
		return fmt.Errorf("upload %s to %s: %w", binaryName, host, err)
	}
	for i, cmd := range steps {
		result, err := sshutils.SSHCmdWithSudo(o.SSHConfig, host, cmd)
		if err == nil && result.ExitCode != 0 {
			err = fmt.Errorf("exit %d: %s", result.ExitCode, strings.TrimSpace(result.Stderr))
		}
		if err != nil {
			return fmt.Errorf("step %d/%d (%s): %w", i+1, len(steps), cmd, err)
		}
	}
	return nil
}

func (o *UpgradeOptions) restoreNodeBinary(host, role string) {
	binaryName := binaryForRole(role)
	backupPath := path.Join(upgradeStagingDir, "backup", binaryName)
	remoteBin := path.Join("/usr/local/bin", binaryName)
	service := "kc-" + role
	for _, cmd := range []string{
		fmt.Sprintf("install -m 0755 %s %s", backupPath, remoteBin),
		fmt.Sprintf("systemctl restart %s", service),
	} {
		result, err := sshutils.SSHCmdWithSudo(o.SSHConfig, host, cmd)
		if err == nil && result.ExitCode != 0 {
			err = fmt.Errorf("exit %d: %s", result.ExitCode, strings.TrimSpace(result.Stderr))
		}
		if err != nil {
			logger.Errorf("restore %s on %s failed: %v (backup kept at %s, restore manually)", binaryName, host, err, backupPath)
			return
		}
	}
	logger.Infof("restored previous %s on %s", binaryName, host)
}

func (o *UpgradeOptions) nodeHealthzOK(host string) bool {
	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //nolint:gosec // health probe mirrors kcctl deploy behavior for self-signed platform certs
	client := &http.Client{Transport: tr, Timeout: nodeHealthzTimeout}
	scheme := "https"
	if !o.deployConfig.TLS {
		scheme = "http"
	}
	resp, err := client.Get(fmt.Sprintf("%s://%s:%v/healthz", scheme, host, o.deployConfig.ServerPort))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64))
	return err == nil && strings.TrimSpace(string(body)) == "ok"
}

func (o *UpgradeOptions) waitServerHealthy(host string) error {
	deadline := time.Now().Add(nodeHealthzWait)
	ticker := time.NewTicker(nodeHealthzTick)
	defer ticker.Stop()
	for {
		if o.nodeHealthzOK(host) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("server on %s did not become healthy within %s", host, nodeHealthzWait)
		}
		<-ticker.C
	}
}

func (o *UpgradeOptions) waitAgentActive(host string) error {
	deadline := time.Now().Add(nodeHealthzWait)
	ticker := time.NewTicker(nodeHealthzTick)
	defer ticker.Stop()
	for {
		result, err := sshutils.SSHCmdWithSudo(o.SSHConfig, host, "systemctl is-active kc-agent")
		if err == nil && result.ExitCode == 0 && strings.TrimSpace(result.Stdout) == "active" {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("kc-agent on %s did not become active within %s", host, nodeHealthzWait)
		}
		<-ticker.C
	}
}

// verifyPlatform confirms the cluster API answers with the target revision
// after the rollout. Individual node results were already reported per node.
func (o *UpgradeOptions) verifyPlatform(_ *rolloutPlan) error {
	versionInfo, err := o.client.Version(context.TODO())
	if err != nil {
		return fmt.Errorf("verify upgrade via platform API: %w", err)
	}
	if o.targetRevision != "" && versionInfo.GitCommit != o.targetRevision {
		return fmt.Errorf("platform API reports revision %s after upgrade, expected %s", versionInfo.GitCommit, o.targetRevision)
	}
	logger.Infof("platform API reports %s (revision %s)", versionInfo.GitVersion, shortRev(versionInfo.GitCommit))
	return nil
}

func (o *UpgradeOptions) cleanup(plan *rolloutPlan) error {
	return o.cleanupNodes(plan.nodes)
}

func (o *UpgradeOptions) cleanupNodes(nodes []nodePlan) error {
	hosts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		hosts = append(hosts, node.host)
	}
	if len(hosts) == 0 {
		return nil
	}
	cmd := fmt.Sprintf("rm -rf %s", upgradeStagingDir)
	if err := sshutils.CmdBatchWithSudo(o.SSHConfig, hosts, cmd, sshutils.DefaultWalk); err != nil {
		return fmt.Errorf("clean staging dir on upgraded nodes: %w", err)
	}
	return nil
}
