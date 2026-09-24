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
  then replaces the platform components one node at a time with a health check
  between nodes. A failed node is restored from its backup and the rollout
  stops there; already upgraded nodes stay upgraded.

  Components: server and agent replace the kubeclipper-server/kubeclipper-agent
  binaries and restart the systemd services; console replaces the caddy binary
  and the kc-console web dist on the server nodes (a console re-install of the
  same version is allowed — the console carries no source revision); kcctl
  replaces the /usr/local/bin/kcctl binary (no service restart). The fixed
  rollout order is server -> agent -> console -> kcctl, and "all" rolls the
  four of them without ever touching kc-etcd or the Package Registry.

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

  # Upgrade the console (caddy + web dist) and the kcctl binary only
  kcctl upgrade console --manifest release-manifest.yaml
  kcctl upgrade kcctl --manifest release-manifest.yaml

  # Plain-HTTP internal Registry (or persist the scheme in the package
  # registry config file / KC_PACKAGE_REGISTRY_CONFIG instead)
  kcctl upgrade all --manifest release-manifest.yaml --package-registry-scheme http`
)

const (
	bootstrapPackageName = "kubeclipper"
	consolePackageName   = "console"

	serverBinaryName  = "kubeclipper-server"
	agentBinaryName   = "kubeclipper-agent"
	kcctlBinaryName   = "kcctl"
	consoleBinaryName = "caddy"
	consoleDistName   = "kc-console"

	roleServer  = "server"
	roleAgent   = "agent"
	roleConsole = "console"
	roleKcctl   = "kcctl"

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

	manifest          *releasemanifest.Manifest
	requiredArtifacts []*releasemanifest.Artifact
	registry          string
	registryConfig    *deliveryregistry.Config

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
	case cmdoptions.UpgradeServer, cmdoptions.UpgradeAgent, cmdoptions.UpgradeAll, cmdoptions.UpgradeConsole, cmdoptions.UpgradeKcctl:
	default:
		return utils.UsageErrorf(cmd, "unsupported upgrade component %q, support [ all | server | agent | console | kcctl ] now", o.component)
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
	required, err := requiredArtifactsFor(o.component, o.manifest)
	if err != nil {
		return err
	}
	o.requiredArtifacts = required
	o.registry = strings.TrimRight(o.manifest.Registries.Package, "/")
	// The primary artifact decides the reported target version: the console
	// version for a console-only upgrade, the platform version otherwise.
	o.targetVersion = required[0].Component.Version
	o.targetRevision = o.manifest.Metadata.SourceRevision

	if o.component == cmdoptions.UpgradeConsole {
		// The console dist versions independently of the platform (console
		// v1.6.0 ships with platform v2.0.3-rc.x), so a semver comparison
		// against the platform version would reject every console upgrade;
		// the console carries no source revision either — re-install of the
		// same console version is the idempotent path.
		logger.Infof("console upgrade target: bootstrap/console:%s from %s", o.targetVersion, o.registry)
	} else if err := o.checkVersionPolicy(); err != nil {
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

// requiredArtifactsFor decides which bootstrap package artifacts a component
// upgrade needs; "all" needs both packages. Missing artifacts fail here, in
// Validate, before anything is fetched or stopped.
func requiredArtifactsFor(component string, manifest *releasemanifest.Manifest) ([]*releasemanifest.Artifact, error) {
	kcArtifact := manifest.BootstrapKubeClipperArtifact()
	consoleArtifact := manifest.BootstrapConsoleArtifact()
	switch component {
	case cmdoptions.UpgradeConsole:
		if consoleArtifact == nil {
			return nil, fmt.Errorf("release manifest contains no bootstrap/console package artifact (console upgrade requires it)")
		}
		return []*releasemanifest.Artifact{consoleArtifact}, nil
	case cmdoptions.UpgradeAll:
		if kcArtifact == nil {
			return nil, fmt.Errorf("release manifest contains no bootstrap/kubeclipper package artifact")
		}
		if consoleArtifact == nil {
			return nil, fmt.Errorf("release manifest contains no bootstrap/console package artifact (upgrade all requires it)")
		}
		return []*releasemanifest.Artifact{kcArtifact, consoleArtifact}, nil
	default:
		if kcArtifact == nil {
			return nil, fmt.Errorf("release manifest contains no bootstrap/kubeclipper package artifact")
		}
		return []*releasemanifest.Artifact{kcArtifact}, nil
	}
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
	kcctlPath  string
	caddyPath  string
	distPath   string
}

func (o *UpgradeOptions) targetRoles() []string {
	switch o.component {
	case cmdoptions.UpgradeServer:
		return []string{roleServer}
	case cmdoptions.UpgradeAgent:
		return []string{roleAgent}
	case cmdoptions.UpgradeConsole:
		return []string{roleConsole}
	case cmdoptions.UpgradeKcctl:
		return []string{roleKcctl}
	default:
		return []string{roleServer, roleAgent, roleConsole, roleKcctl}
	}
}

func (o *UpgradeOptions) needsRole(role string) bool {
	return sets.NewString(o.targetRoles()...).Has(role)
}

// roleOrder: fixed rollout order — control-plane binaries first, then the web
// entrypoint, then the CLI the operator runs from these very nodes.
var roleOrder = []string{roleServer, roleAgent, roleConsole, roleKcctl}

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
	for _, role := range roleOrder {
		if !roles.Has(role) {
			continue
		}
		for _, host := range o.hostsForRole(role) {
			if err := o.appendNode(plan, role, host); err != nil {
				return nil, err
			}
		}
	}
	plan.nodes = dedupNodes(plan.nodes)
	return plan, nil
}

// hostsForRole: the agent runs on every agent node, while the server, the
// console and the kcctl binary live on the server nodes.
func (o *UpgradeOptions) hostsForRole(role string) []string {
	if role == roleAgent {
		return o.deployConfig.Agents.ListIP()
	}
	return o.deployConfig.ServerIPs
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
	info, err := parseVersionJSON(result.Stdout)
	if err != nil {
		return ""
	}
	return info.GitCommit
}

// parseVersionJSON extracts the version Info from command output. The platform
// binaries print plain JSON, while kcctl prefixes it with a "kcctl version:"
// banner line — and with a reachable platform config appends a second
// "kubeclipper-server version:" object. The client (first) object is the one
// that identifies the binary being probed.
func parseVersionJSON(out string) (nodeVersionInfo, error) {
	obj, ok := firstJSONObject(out)
	if !ok {
		return nodeVersionInfo{}, fmt.Errorf("no JSON object in version output: %q", strings.TrimSpace(out))
	}
	var info nodeVersionInfo
	if err := json.Unmarshal([]byte(obj), &info); err != nil {
		return nodeVersionInfo{}, err
	}
	return info, nil
}

// firstJSONObject returns the first balanced JSON object in s, honoring string
// literals so brace characters inside values cannot skew the matching.
func firstJSONObject(s string) (string, bool) {
	start := strings.Index(s, "{")
	if start < 0 {
		return "", false
	}
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(s); i++ {
		switch c := s[i]; {
		case escaped:
			escaped = false
		case inString:
			if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
		case c == '"':
			inString = true
		case c == '{':
			depth++
		case c == '}':
			depth--
			if depth == 0 {
				return s[start : i+1], true
			}
		}
	}
	return "", false
}

type nodeVersionInfo struct {
	GitVersion string `json:"gitVersion"`
	GitCommit  string `json:"gitCommit"`
}

func binaryForRole(role string) string {
	switch role {
	case roleAgent:
		return agentBinaryName
	case roleConsole:
		return consoleBinaryName
	case roleKcctl:
		return kcctlBinaryName
	default:
		return serverBinaryName
	}
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
	for _, artifact := range o.requiredArtifacts {
		ref := o.registry + "/" + artifact.Target
		desc, err := crane.Get(ref, opts...)
		if err != nil {
			return fmt.Errorf("resolve %s: %w", ref, err)
		}
		if desc.Digest.String() == artifact.Digest {
			continue
		}
		image, err := desc.Image()
		if err != nil {
			return fmt.Errorf("resolve %s: read image: %w", ref, err)
		}
		configFile, err := image.ConfigFile()
		if err != nil {
			return fmt.Errorf("resolve %s: read image config: %w", ref, err)
		}
		if configFile.Config.Labels["org.opencontainers.image.revision"] == o.targetRevision {
			logger.Warnf("registry tag %s digest %s differs from manifest digest %s; source revision %s matches", ref, desc.Digest, artifact.Digest, o.targetRevision)
			continue
		}
		return fmt.Errorf("registry tag %s digest %s does not match the release manifest digest %s; refusing to upgrade from a repointed tag", ref, desc.Digest, artifact.Digest)
	}
	return nil
}

func (o *UpgradeOptions) fetchPlatformPackage(ctx context.Context, arch string) (*archArtifacts, error) {
	artifacts := &archArtifacts{}
	for _, required := range o.requiredArtifacts {
		files, err := o.fetchPackageFiles(ctx, required, arch)
		if err != nil {
			return nil, err
		}
		switch required.Component.Name {
		case bootstrapPackageName:
			artifacts.serverPath = files[serverBinaryName]
			artifacts.agentPath = files[agentBinaryName]
			artifacts.kcctlPath = files[kcctlBinaryName]
			for _, need := range []struct {
				role   string
				binary string
				path   string
			}{
				{roleServer, serverBinaryName, artifacts.serverPath},
				{roleAgent, agentBinaryName, artifacts.agentPath},
				{roleKcctl, kcctlBinaryName, artifacts.kcctlPath},
			} {
				if o.needsRole(need.role) && need.path == "" {
					return nil, fmt.Errorf("package %s:%s(%s) has no %s payload", bootstrapPackageName, o.targetVersion, arch, need.binary)
				}
			}
		case consolePackageName:
			artifacts.caddyPath = files[consoleBinaryName]
			artifacts.distPath = files[consoleDistName]
			if o.needsRole(roleConsole) {
				if artifacts.caddyPath == "" {
					return nil, fmt.Errorf("package %s:%s(%s) has no %s payload", consolePackageName, required.Component.Version, arch, consoleBinaryName)
				}
				if artifacts.distPath == "" {
					return nil, fmt.Errorf("package %s:%s(%s) has no %s payload", consolePackageName, required.Component.Version, arch, consoleDistName)
				}
			}
		}
	}
	return artifacts, nil
}

// fetchPackageFiles downloads one bootstrap package for the given architecture
// and returns its contents as a file-name -> local-path map.
func (o *UpgradeOptions) fetchPackageFiles(ctx context.Context, artifact *releasemanifest.Artifact, arch string) (map[string]string, error) {
	repository, tag, ok := strings.Cut(artifact.Target, ":")
	if !ok || tag == "" {
		return nil, fmt.Errorf("release manifest artifact target %q carries no tag", artifact.Target)
	}
	if tag != artifact.Component.Version {
		return nil, fmt.Errorf("release manifest artifact tag %q does not match component version %q", tag, artifact.Component.Version)
	}
	indexer := deliveryindexer.NewRegistryPackageInventoryIndexerWithConfig(o.registryConfig)
	inventory, err := indexer.IndexPackageRepositories(ctx, o.registry, []string{repository})
	if err != nil {
		return nil, fmt.Errorf("index package repository %s/%s: %w", o.registry, repository, err)
	}
	entry := selectPackageEntry(inventory, artifact.Component.Kind, artifact.Component.Name, artifact.Component.Version, arch)
	if entry == nil {
		return nil, fmt.Errorf("registry %s has no %s/%s package version %s for arch %s; sync the release bundle first", o.registry, artifact.Component.Kind, artifact.Component.Name, artifact.Component.Version, arch)
	}
	// The platform package is built from the source tree the manifest pins, so
	// its sourceRevision must match. The console dist is not built from the
	// platform source tree — its revision label only records the publish-time
	// platform revision, and the manifest digest binding is the integrity
	// guarantee, so a mismatch there is informational.
	strictRevision := artifact.Component.Name == bootstrapPackageName
	if entry.SourceRevision == "" {
		if strictRevision {
			logger.Warnf("package %s/%s:%s(%s) carries no sourceRevision metadata; the revision expectation %s cannot be verified before rollout, republish the package with KC_SOURCE_REVISION set", entry.Kind, entry.Name, entry.Version, entry.Arch, o.targetRevision)
		}
	} else if entry.SourceRevision != o.targetRevision {
		if strictRevision {
			return nil, fmt.Errorf("package %s/%s:%s(%s) source revision %s does not match release manifest revision %s", entry.Kind, entry.Name, entry.Version, entry.Arch, entry.SourceRevision, o.targetRevision)
		}
		logger.Warnf("package %s/%s:%s(%s) was published at platform revision %s, release manifest pins %s; the console dist is not built from the platform source tree, the manifest digest binding is the integrity guarantee", entry.Kind, entry.Name, entry.Version, entry.Arch, entry.SourceRevision, o.targetRevision)
	}
	component := deliveryapis.ResolvedComponent{
		Slot:      "upgrade-" + artifact.Component.Name,
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
		return nil, fmt.Errorf("fetch %s/%s:%s for %s: %w", artifact.Component.Kind, artifact.Component.Name, artifact.Component.Version, arch, err)
	}
	if len(result.Components) != 1 {
		return nil, fmt.Errorf("fetch %s/%s:%s for %s: unexpected fetch result", artifact.Component.Kind, artifact.Component.Name, artifact.Component.Version, arch)
	}
	return result.Components[0].Files, nil
}

func selectPackageEntry(inventory *deliveryapis.PackageInventory, kind, name, version, arch string) *deliveryapis.PackageEntry {
	if inventory == nil {
		return nil
	}
	for i := range inventory.Spec.Packages {
		pkg := &inventory.Spec.Packages[i]
		if pkg.Kind == kind && pkg.Name == name && pkg.Version == version && pkg.Arch == arch {
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
		// and clobber its backup. The console has no revision to probe
		// (caddy version is caddy's, not the console's), so console nodes
		// always re-install — that re-install is the idempotent path.
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
		case roleConsole:
			err = o.upgradeConsoleNode(node.host, artifacts)
			if err == nil {
				err = o.waitConsoleHealthy(node.host)
			}
		case roleKcctl:
			err = o.upgradeKcctlNode(node.host, artifacts)
			if err == nil && o.targetRevision != "" {
				if got := o.probeNodeRevision(node.host, roleKcctl); got != o.targetRevision {
					err = fmt.Errorf("kcctl on %s reports revision %q after replace, expected %s", node.host, got, shortRev(o.targetRevision))
				}
			}
		default:
			err = fmt.Errorf("unsupported role %q", node.role)
		}
		if err != nil {
			logger.Errorf("upgrade %s on %s failed: %v", node.role, node.host, err)
			o.restoreNode(node.host, node.role)
			// Keep the staging dir (and the backup inside it) so the failure
			// scene stays inspectable; /tmp is cleared on reboot.
			return fmt.Errorf("upgrade stopped at %s %s: %w; the node was restored to the previous binary, already upgraded nodes were not rolled back", node.role, node.host, err)
		}
		upgraded = append(upgraded, node)
		logger.Infof("upgraded %s %s to %s", node.role, node.host, o.targetVersion)
	}
	return o.verifyPlatform(plan)
}

// verifiesPlatform: only the components that replace kubeclipper-server can
// change what the platform API reports, so only they are verified against the
// target revision afterwards.
func (o *UpgradeOptions) verifiesPlatform() bool {
	return o.component != cmdoptions.UpgradeConsole && o.component != cmdoptions.UpgradeKcctl
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
	return o.uploadAndRun(host, []string{localPath}, steps)
}

// upgradeConsoleNode replaces the caddy binary and the kc-console web dist on
// a server node and restarts kc-console. The dist travels as a tar archive
// whose top-level directory is named kc-console; it is extracted under
// dist-extract/ to avoid the archive/extracted-dir name clash in staging.
func (o *UpgradeOptions) upgradeConsoleNode(host string, artifacts archArtifacts) error {
	backupDir := path.Join(upgradeStagingDir, "backup")
	backupCaddy := path.Join(backupDir, consoleBinaryName)
	backupDist := path.Join(backupDir, "dist.tar")
	remoteCaddy := path.Join("/usr/local/bin", consoleBinaryName)
	extractDir := path.Join(upgradeStagingDir, "dist-extract")
	steps := []string{
		fmt.Sprintf("mkdir -p %s", backupDir),
		"systemctl stop kc-console",
		fmt.Sprintf("[ -f %s ] || cp -a %s %s", backupCaddy, remoteCaddy, backupCaddy),
		fmt.Sprintf("[ -f %s ] || tar -cf %s -C /etc/kc-console dist", backupDist, backupDist),
		fmt.Sprintf("install -m 0755 %s %s", path.Join(upgradeStagingDir, consoleBinaryName), remoteCaddy),
		fmt.Sprintf("rm -rf %s && mkdir -p %s && tar -xf %s -C %s", extractDir, extractDir, path.Join(upgradeStagingDir, consoleDistName), extractDir),
		fmt.Sprintf("rm -rf /etc/kc-console/dist && cp -a %s/%s /etc/kc-console/dist", extractDir, consoleDistName),
		"systemctl start kc-console",
	}
	return o.uploadAndRun(host, []string{artifacts.caddyPath, artifacts.distPath}, steps)
}

// upgradeKcctlNode swaps /usr/local/bin/kcctl in place; there is no service to
// restart — the next kcctl invocation picks the binary up.
func (o *UpgradeOptions) upgradeKcctlNode(host string, artifacts archArtifacts) error {
	backupPath := path.Join(upgradeStagingDir, "backup", kcctlBinaryName)
	remoteBin := path.Join("/usr/local/bin", kcctlBinaryName)
	steps := []string{
		fmt.Sprintf("mkdir -p %s", path.Join(upgradeStagingDir, "backup")),
		fmt.Sprintf("[ -f %s ] || cp -a %s %s", backupPath, remoteBin, backupPath),
		fmt.Sprintf("install -m 0755 %s %s", path.Join(upgradeStagingDir, kcctlBinaryName), remoteBin),
	}
	return o.uploadAndRun(host, []string{artifacts.kcctlPath}, steps)
}

// uploadAndRun uploads the fetched files into the staging dir and runs the
// remote steps, reporting the failing step with its command on error.
func (o *UpgradeOptions) uploadAndRun(host string, localPaths []string, steps []string) error {
	for _, localPath := range localPaths {
		if err := utils.SendPackageV2(o.SSHConfig, localPath, []string{host}, upgradeStagingDir, nil, nil); err != nil {
			return fmt.Errorf("upload %s to %s: %w", path.Base(localPath), host, err)
		}
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

func (o *UpgradeOptions) restoreNode(host, role string) {
	switch role {
	case roleConsole:
		o.restoreConsole(host)
	case roleKcctl:
		o.restoreKcctl(host)
	default:
		o.restoreNodeBinary(host, role)
	}
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

func (o *UpgradeOptions) restoreConsole(host string) {
	backupCaddy := path.Join(upgradeStagingDir, "backup", consoleBinaryName)
	backupDist := path.Join(upgradeStagingDir, "backup", "dist.tar")
	for _, cmd := range []string{
		// guarded: a failure before the backup steps left nothing to restore
		fmt.Sprintf("if [ -f %s ]; then install -m 0755 %s /usr/local/bin/%s; fi", backupCaddy, backupCaddy, consoleBinaryName),
		fmt.Sprintf("if [ -f %s ]; then rm -rf /etc/kc-console/dist && tar -xf %s -C /etc/kc-console; fi", backupDist, backupDist),
		"systemctl restart kc-console",
	} {
		result, err := sshutils.SSHCmdWithSudo(o.SSHConfig, host, cmd)
		if err == nil && result.ExitCode != 0 {
			err = fmt.Errorf("exit %d: %s", result.ExitCode, strings.TrimSpace(result.Stderr))
		}
		if err != nil {
			logger.Errorf("restore kc-console on %s failed: %v (backups kept at %s and %s, restore manually)", host, err, backupCaddy, backupDist)
			return
		}
	}
	logger.Infof("restored previous caddy and web dist on %s", host)
}

func (o *UpgradeOptions) restoreKcctl(host string) {
	backupPath := path.Join(upgradeStagingDir, "backup", kcctlBinaryName)
	cmd := fmt.Sprintf("if [ -f %s ]; then install -m 0755 %s /usr/local/bin/%s; fi", backupPath, backupPath, kcctlBinaryName)
	result, err := sshutils.SSHCmdWithSudo(o.SSHConfig, host, cmd)
	if err == nil && result.ExitCode != 0 {
		err = fmt.Errorf("exit %d: %s", result.ExitCode, strings.TrimSpace(result.Stderr))
	}
	if err != nil {
		logger.Errorf("restore %s on %s failed: %v (backup kept at %s, restore manually)", kcctlBinaryName, host, err, backupPath)
		return
	}
	logger.Infof("restored previous %s on %s", kcctlBinaryName, host)
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

// consoleOK probes the console entrypoint: caddy serves the web dist over
// plain HTTP on the console port, so any 2xx proves the service is up and
// serving the frontend.
func (o *UpgradeOptions) consoleOK(host string) bool {
	client := &http.Client{Timeout: nodeHealthzTimeout}
	resp, err := client.Get(fmt.Sprintf("http://%s:%v/", host, o.deployConfig.ConsolePort))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64))
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

func (o *UpgradeOptions) waitConsoleHealthy(host string) error {
	deadline := time.Now().Add(nodeHealthzWait)
	ticker := time.NewTicker(nodeHealthzTick)
	defer ticker.Stop()
	for {
		if o.consoleOK(host) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("console on %s did not become healthy within %s (probing http port %v)", host, nodeHealthzWait, o.deployConfig.ConsolePort)
		}
		<-ticker.C
	}
}

// verifyPlatform confirms the cluster API answers with the target revision
// after the rollout. Individual node results were already reported per node.
func (o *UpgradeOptions) verifyPlatform(_ *rolloutPlan) error {
	if !o.verifiesPlatform() {
		logger.Infof("%s upgrade complete; platform API revision not verified (component does not replace kubeclipper-server)", o.component)
		return nil
	}
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
