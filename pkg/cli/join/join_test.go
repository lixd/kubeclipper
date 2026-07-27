package join

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"
)

func TestJoinOptionsValidateArgsRequiresPackageRegistry(t *testing.T) {
	o := NewJoinOptions(options.IOStreams{})
	o.deployConfig.ServerIPs = []string{"10.0.0.1"}
	o.deployConfig.Config = "deploy-config.yaml"
	o.parseAgent = options.Agents{
		"10.0.0.2": {Region: "default"},
	}
	o.sshConfig = &sshutils.SSH{Password: "secret"}

	if err := o.ValidateArgs(&cobra.Command{}); err == nil {
		t.Fatalf("ValidateArgs() expected packageRegistry error")
	}

	o.deployConfig.PackageRegistry = "registry.local:5000"
	if err := o.ValidateArgs(&cobra.Command{}); err != nil {
		t.Fatalf("ValidateArgs() unexpected error: %+v", err)
	}
}

func TestJoinCommandExposesPackageRegistryFlag(t *testing.T) {
	cmd := NewCmdJoin(options.IOStreams{})
	if cmd.Flags().Lookup("package-registry") == nil {
		t.Fatalf("join command must expose --package-registry")
	}
}

func TestReadJoinConfigPackageRegistry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "join-config.yaml")
	if err := os.WriteFile(path, []byte("packageRegistry: registry.local:5000\nagents:\n  10.0.0.2: {}\n"), 0644); err != nil {
		t.Fatalf("write join config: %+v", err)
	}
	cfg, err := readJoinConfig(path)
	if err != nil {
		t.Fatalf("readJoinConfig() error: %+v", err)
	}
	if cfg.PackageRegistry != "registry.local:5000" {
		t.Fatalf("PackageRegistry = %q, want registry.local:5000", cfg.PackageRegistry)
	}
}

func TestCompleteServerSSHConfigKeepsServerOverrides(t *testing.T) {
	fallback := &sshutils.SSH{User: "deploy", Password: "deploy-secret", Port: 22, PkFile: "/deploy/key"}
	server := &sshutils.SSH{User: "server", Port: 2202, PkFile: "/server/key"}

	got := completeServerSSHConfig(server, fallback)
	if got.User != "server" || got.Port != 2202 || got.PkFile != "/server/key" {
		t.Fatalf("server overrides lost: %+v", got)
	}
	if got.Password != "deploy-secret" {
		t.Fatalf("fallback password = %q", got.Password)
	}
}

func TestFailJoinWithRollbackCleansEveryRequestedAgent(t *testing.T) {
	o := NewJoinOptions(options.IOStreams{})
	o.parseAgent = options.Agents{
		"10.0.0.1": {},
		"10.0.0.2": {},
	}
	called := map[string]bool{}
	o.sshRunner = func(_ *sshutils.SSH, host, _ string) (sshutils.Result, error) {
		called[host] = true
		return sshutils.Result{}, nil
	}
	err := o.failJoinWithRollback(errors.New("install failed"))
	if err == nil || len(called) != 2 || !called["10.0.0.1"] || !called["10.0.0.2"] {
		t.Fatalf("rollback error=%v called=%v", err, called)
	}
}

func TestCompleteServerSSHConfigFallsBackToDeployTransport(t *testing.T) {
	fallback := &sshutils.SSH{User: "deploy", Port: 2222, PrivateKey: "key-data"}
	got := completeServerSSHConfig(nil, fallback)
	if got.User != fallback.User || got.Port != fallback.Port || got.PrivateKey != fallback.PrivateKey {
		t.Fatalf("completed transport = %+v, want fallback %+v", got, fallback)
	}
}

func TestAgentSSHConfigForPersistenceDoesNotCopyCachedPrivateKey(t *testing.T) {
	source := &sshutils.SSH{
		User:       "root",
		PkFile:     "/root/agent-key",
		PrivateKey: "cached-private-key",
		Port:       22,
	}

	got := agentSSHConfigForPersistence(source)
	if got.PkFile != source.PkFile || got.PrivateKey != "" {
		t.Fatalf("persisted SSH config = %+v, want key path without cached private key", got)
	}
	if source.PrivateKey != "cached-private-key" {
		t.Fatal("source SSH config was mutated")
	}
}

func TestPlanJoinedAgentsPreservesTransportForSequentialJoins(t *testing.T) {
	config := options.NewDeployOptions()
	config.SSHConfig = &sshutils.SSH{User: "deploy", PkFile: "/keys/deploy"}
	config.ServerIPs = []string{"10.0.0.1"}
	config.Agents = options.Agents{
		"10.0.0.2": {AgentID: "initial"},
	}
	if err := config.RecordInitialAgentSSHTransport(); err != nil {
		t.Fatal(err)
	}

	first := NewJoinOptions(options.IOStreams{})
	first.deployConfig = config
	first.parseAgent = options.Agents{"10.0.0.3": {Region: "default"}}
	first.sshConfig = &sshutils.SSH{User: "agent-a", PkFile: "/keys/a", PrivateKey: "cached-a"}
	first.newSSHTransportID = func() string { return "join-a" }
	if err := first.planJoinedAgents(); err != nil {
		t.Fatalf("first planJoinedAgents() error = %v", err)
	}

	second := NewJoinOptions(options.IOStreams{})
	second.deployConfig = config
	second.parseAgent = options.Agents{"10.0.0.4": {Region: "default"}}
	second.sshConfig = &sshutils.SSH{User: "agent-b", PkFile: "/keys/b", PrivateKey: "cached-b"}
	second.newSSHTransportID = func() string { return "join-b" }
	if err := second.planJoinedAgents(); err != nil {
		t.Fatalf("second planJoinedAgents() error = %v", err)
	}

	for ip, wantID := range map[string]string{
		"10.0.0.2": options.SSHTransportIDDeploy,
		"10.0.0.3": "join-a",
		"10.0.0.4": "join-b",
	} {
		if got := config.Agents[ip].SSHTransportID; got != wantID {
			t.Fatalf("agent %s transport = %q, want %q", ip, got, wantID)
		}
	}
	for id, wantKey := range map[string]string{
		options.SSHTransportIDDeploy: "/keys/deploy",
		"join-a":                     "/keys/a",
		"join-b":                     "/keys/b",
	} {
		transport := config.SSHTransports[id]
		if transport == nil || transport.PkFile != wantKey || transport.PrivateKey != "" {
			t.Fatalf("transport %q = %+v, want key %q without cached key", id, transport, wantKey)
		}
	}
}

func TestFailedJoinRestoresAgentsAndSSHTransports(t *testing.T) {
	config := options.NewDeployOptions()
	config.SSHConfig = &sshutils.SSH{User: "deploy", PkFile: "/keys/deploy"}
	config.ServerIPs = []string{"10.0.0.1"}
	config.Agents = options.Agents{
		"10.0.0.2": {AgentID: "existing", SSHTransportID: "join-a"},
	}
	config.SSHTransports = options.SSHTransports{
		options.SSHTransportIDDeploy: {User: "deploy", PkFile: "/keys/deploy"},
		"join-a":                     {User: "agent-a", PkFile: "/keys/a"},
	}
	wantAgents := cloneAgents(config.Agents)
	wantTransports := cloneSSHTransports(config.SSHTransports)

	o := NewJoinOptions(options.IOStreams{})
	o.deployConfig = config
	o.parseAgent = options.Agents{"10.0.0.3": {Region: "default"}}
	o.sshConfig = &sshutils.SSH{User: "agent-b", PkFile: "/keys/b"}
	o.newSSHTransportID = func() string { return "join-b" }
	if err := o.planJoinedAgents(); err != nil {
		t.Fatalf("planJoinedAgents() error = %v", err)
	}
	rolledBack := false
	o.sshRunner = func(config *sshutils.SSH, host, _ string) (sshutils.Result, error) {
		if config.PkFile != "/keys/b" || host != "10.0.0.3" {
			t.Fatalf("rollback used key %q for host %q", config.PkFile, host)
		}
		rolledBack = true
		return sshutils.Result{}, nil
	}
	persisted := false
	o.updateDeployConfig = func(_ context.Context, _ *kc.Client, got *options.DeployConfig, dump bool) error {
		persisted = true
		if !dump || !reflect.DeepEqual(got.Agents, wantAgents) || !reflect.DeepEqual(got.SSHTransports, wantTransports) {
			t.Fatalf("restored deploy config = agents %+v transports %+v dump %t", got.Agents, got.SSHTransports, dump)
		}
		return nil
	}
	if err := o.failJoinWithRollbackAndConfig(errors.New("install failed")); err == nil {
		t.Fatal("failJoinWithRollbackAndConfig() error = nil")
	}
	if !rolledBack || !persisted {
		t.Fatalf("rollback called = %t, restored config persisted = %t", rolledBack, persisted)
	}
	if !reflect.DeepEqual(config.Agents, wantAgents) || !reflect.DeepEqual(config.SSHTransports, wantTransports) {
		t.Fatalf("failed join state was not restored: agents %+v transports %+v", config.Agents, config.SSHTransports)
	}
}

func TestPlannedInventoryPersistenceFailureRestoresPreJoinState(t *testing.T) {
	config := options.NewDeployOptions()
	config.SSHConfig = &sshutils.SSH{User: "deploy", PkFile: "/keys/deploy"}
	config.ServerIPs = []string{"10.0.0.1"}
	config.Agents = options.Agents{
		"10.0.0.2": {AgentID: "initial"},
	}
	if err := config.RecordInitialAgentSSHTransport(); err != nil {
		t.Fatal(err)
	}
	wantAgents := cloneAgents(config.Agents)
	wantTransports := cloneSSHTransports(config.SSHTransports)

	o := NewJoinOptions(options.IOStreams{})
	o.deployConfig = config
	o.parseAgent = options.Agents{"10.0.0.3": {Region: "default"}}
	o.sshConfig = &sshutils.SSH{User: "agent", PkFile: "/keys/agent"}
	o.newSSHTransportID = func() string { return "join-failed" }
	updates := 0
	o.updateDeployConfig = func(_ context.Context, _ *kc.Client, got *options.DeployConfig, _ bool) error {
		updates++
		if updates == 1 {
			if got.Agents["10.0.0.3"].SSHTransportID != "join-failed" {
				t.Fatalf("planned inventory was not passed to first update: %+v", got.Agents)
			}
			return errors.New("injected persist failure")
		}
		if !reflect.DeepEqual(got.Agents, wantAgents) || !reflect.DeepEqual(got.SSHTransports, wantTransports) {
			t.Fatalf("restore update = agents %+v transports %+v", got.Agents, got.SSHTransports)
		}
		return nil
	}
	if err := o.runJoinAgentNode(); err == nil {
		t.Fatal("runJoinAgentNode() error = nil")
	}
	if updates != 2 {
		t.Fatalf("deploy config updates = %d, want planned write and restore", updates)
	}
	if !reflect.DeepEqual(config.Agents, wantAgents) || !reflect.DeepEqual(config.SSHTransports, wantTransports) {
		t.Fatalf("state after failed planned write = agents %+v transports %+v", config.Agents, config.SSHTransports)
	}
}

func TestSendCertsAlwaysDownloadsCurrentServerCertificates(t *testing.T) {
	o := NewJoinOptions(options.IOStreams{})
	o.deployConfig.ServerIPs = []string{"10.0.0.1"}
	o.deployConfig.MQ.TLS = true
	o.deployConfig.MQ.CA = "/etc/kubeclipper-server/pki/ca.crt"
	o.deployConfig.MQ.ClientCert = "/etc/kubeclipper-server/pki/nats/kc-server-nats-client.crt"
	o.deployConfig.MQ.ClientKey = "/etc/kubeclipper-server/pki/nats/kc-server-nats-client.key"
	o.serverSSHConfig = &sshutils.SSH{User: "server"}
	o.sshConfig = &sshutils.SSH{User: "agent"}

	var downloadedPaths []string
	o.certDownload = recordCertificateDownload(t, o, &downloadedPaths)
	var copied int
	var copiedModes []os.FileMode
	o.certCopy = recordCertificateCopy(t, o, &copied, &copiedModes)

	if err := o.sendCerts("10.0.0.2"); err != nil {
		t.Fatalf("sendCerts() error = %v", err)
	}
	if len(downloadedPaths) != 3 || copied != 3 {
		t.Fatalf("downloads = %d, copies = %d, want 3 each", len(downloadedPaths), copied)
	}
	if want := []os.FileMode{0644, 0644, deliveryregistry.PrivateFileMode}; !reflect.DeepEqual(copiedModes, want) {
		t.Fatalf("copied modes = %v, want %v", copiedModes, want)
	}
	for _, path := range downloadedPaths {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("temporary certificate still exists at %s: %v", path, err)
		}
	}
}

func recordCertificateDownload(t *testing.T, o *JoinOptions, downloadedPaths *[]string) func(*sshutils.SSH, string, string, string) error {
	t.Helper()
	return func(sshConfig *sshutils.SSH, host, localPath, remotePath string) error {
		if sshConfig != o.serverSSHConfig || host != "10.0.0.1" {
			t.Fatalf("download transport = %+v host = %s", sshConfig, host)
		}
		if localPath == remotePath || filepath.Dir(localPath) == filepath.Dir(remotePath) {
			t.Fatalf("certificate download reused server path %q", remotePath)
		}
		*downloadedPaths = append(*downloadedPaths, localPath)
		return os.WriteFile(localPath, []byte("current:"+remotePath), deliveryregistry.PrivateFileMode)
	}
}

func recordCertificateCopy(t *testing.T, o *JoinOptions, copied *int, modes *[]os.FileMode) func(*sshutils.SSH, string, []string, string, os.FileMode) error {
	t.Helper()
	return func(sshConfig *sshutils.SSH, localPath string, hosts []string, _ string, mode os.FileMode) error {
		if sshConfig != o.sshConfig || !reflect.DeepEqual(hosts, []string{"10.0.0.2"}) {
			t.Fatalf("copy transport = %+v hosts = %v", sshConfig, hosts)
		}
		data, err := os.ReadFile(localPath)
		if err != nil || !strings.HasPrefix(string(data), "current:") {
			t.Fatalf("copied certificate = %q, error = %v", data, err)
		}
		info, err := os.Stat(localPath)
		if err != nil {
			t.Fatalf("stat copied certificate: %v", err)
		}
		if info.Mode().Perm() != deliveryregistry.PrivateFileMode {
			t.Fatalf("certificate mode = %v, want %v", info.Mode().Perm(), deliveryregistry.PrivateFileMode)
		}
		*modes = append(*modes, mode)
		*copied++
		return nil
	}
}

func TestSendCertsSkipsTransferWhenMQTLSIsDisabled(t *testing.T) {
	o := NewJoinOptions(options.IOStreams{})
	o.deployConfig.MQ.TLS = false
	o.certDownload = func(*sshutils.SSH, string, string, string) error {
		t.Fatal("certificate download called with MQ TLS disabled")
		return nil
	}
	o.certCopy = func(*sshutils.SSH, string, []string, string, os.FileMode) error {
		t.Fatal("certificate copy called with MQ TLS disabled")
		return nil
	}

	if err := o.sendCerts("10.0.0.2"); err != nil {
		t.Fatalf("sendCerts() error = %v", err)
	}
}

func TestPreparePackageRegistryConfigUsesServerTransportAndAgentCopy(t *testing.T) {
	o := NewJoinOptions(options.IOStreams{})
	o.deployConfig.PackageRegistry = "harbor.example.com/kubeclipper"
	o.deployConfig.ServerIPs = []string{"10.0.0.1"}
	o.serverSSHConfig = &sshutils.SSH{User: "server"}
	o.sshConfig = &sshutils.SSH{User: "agent"}

	var downloadedWith *sshutils.SSH
	o.packageRegistryDownload = func(sshConfig *sshutils.SSH, host, localPath, remotePath string) error {
		downloadedWith = sshConfig
		if host != "10.0.0.1" || remotePath != deliveryregistry.ServerConfigPath {
			t.Fatalf("download host/path = %s %s", host, remotePath)
		}
		return deliveryregistry.Write(localPath, &deliveryregistry.Config{
			Registry: "harbor.example.com/kubeclipper",
			Scheme:   deliveryregistry.SchemeHTTPS,
			Username: "robot$kc",
			Password: "token",
		})
	}
	cleanup, err := o.preparePackageRegistryConfig()
	if err != nil {
		t.Fatalf("preparePackageRegistryConfig() error = %v", err)
	}
	defer cleanup()
	if downloadedWith != o.serverSSHConfig {
		t.Fatal("package registry config was not downloaded with server SSH transport")
	}
	if o.packageRegistryConfig.Password != "token" {
		t.Fatal("inherited package registry credential was not loaded")
	}

	var copiedWith *sshutils.SSH
	o.packageRegistryCopy = func(sshConfig *sshutils.SSH, host, localPath, remotePath string) error {
		copiedWith = sshConfig
		if host != "10.0.0.2" || remotePath != deliveryregistry.AgentConfigPath {
			t.Fatalf("copy host/path = %s %s", host, remotePath)
		}
		data, readErr := os.ReadFile(localPath)
		if readErr != nil || !strings.Contains(string(data), "token") {
			t.Fatalf("copied config missing inherited credential: %v", readErr)
		}
		return nil
	}
	if err = o.copyPackageRegistryConfig(o.sshConfig, []string{"10.0.0.2"}, deliveryregistry.AgentConfigPath); err != nil {
		t.Fatalf("copyPackageRegistryConfig() error = %v", err)
	}
	if copiedWith != o.sshConfig {
		t.Fatal("package registry config was not copied with agent SSH transport")
	}
	path := o.packageRegistryConfigPath
	cleanup()
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("temporary package registry config still exists: %v", err)
	}
}

func TestExistingPackageRegistryTargetsUseEachAgentTransport(t *testing.T) {
	o := NewJoinOptions(options.IOStreams{})
	o.deployConfig.ServerIPs = []string{"10.0.0.1"}
	o.deployConfig.SSHConfig = &sshutils.SSH{User: "deploy", PkFile: "/keys/deploy"}
	o.deployConfig.Agents = options.Agents{
		"10.0.0.2": {AgentID: "a", SSHTransportID: "join-a"},
		"10.0.0.3": {AgentID: "b", SSHTransportID: "join-b"},
		"10.0.0.4": {AgentID: "new"},
	}
	o.deployConfig.SSHTransports = options.SSHTransports{
		options.SSHTransportIDDeploy: {User: "deploy", PkFile: "/keys/deploy"},
		"join-a":                     {User: "agent-a", PkFile: "/keys/a"},
		"join-b":                     {User: "agent-b", PkFile: "/keys/b"},
	}
	o.parseAgent = options.Agents{"10.0.0.4": {}}
	o.serverSSHConfig = &sshutils.SSH{User: "server", PkFile: "/keys/server"}

	targets, err := o.existingPackageRegistryTargets()
	if err != nil {
		t.Fatalf("existingPackageRegistryTargets() error = %v", err)
	}
	keys := make(map[string]string)
	for _, target := range targets {
		keys[target.host+":"+target.remotePath] = target.sshConfig.PkFile
	}
	want := map[string]string{
		"10.0.0.1:" + deliveryregistry.ServerConfigPath: "/keys/server",
		"10.0.0.2:" + deliveryregistry.AgentConfigPath:  "/keys/a",
		"10.0.0.3:" + deliveryregistry.AgentConfigPath:  "/keys/b",
	}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("target SSH keys = %v, want %v", keys, want)
	}
}

func TestUpdateExistingPackageRegistryConfigsRollsBackPartialUpdate(t *testing.T) {
	o := NewJoinOptions(options.IOStreams{})
	o.deployConfig.ServerIPs = []string{"10.0.0.1", "10.0.0.2"}
	o.deployConfig.Agents = options.Agents{
		"10.0.0.1": {},
		"10.0.0.3": {},
		"10.0.0.4": {},
	}
	o.parseAgent = options.Agents{"10.0.0.4": {}}
	o.serverSSHConfig = &sshutils.SSH{User: "server"}
	o.deployConfig.AgentSSHConfig = &sshutils.SSH{User: "agent"}
	newPath, cleanup, err := writeTemporaryPackageRegistryConfig(&deliveryregistry.Config{
		Registry: "harbor.example.com/team-a", Scheme: deliveryregistry.SchemeHTTPS,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	o.packageRegistryConfigPath = newPath
	o.packageRegistryDownload = func(_ *sshutils.SSH, host, localPath, _ string) error {
		return deliveryregistry.Write(localPath, &deliveryregistry.Config{
			Registry: "harbor.example.com/team-a", Scheme: deliveryregistry.SchemeHTTPS, Password: "old-" + host, Username: "robot",
		})
	}
	var operations []string
	o.packageRegistryCopy = func(sshConfig *sshutils.SSH, host, localPath, remotePath string) error {
		config, loadErr := deliveryregistry.Load(localPath)
		if loadErr != nil {
			return loadErr
		}
		if host == "10.0.0.3" && config.Password == "" {
			return errors.New("injected update failure")
		}
		operations = append(operations, sshConfig.User+":"+host+":"+remotePath+":"+config.Password)
		return nil
	}

	if err = o.updateExistingPackageRegistryConfigs(func() error { return nil }); err == nil {
		t.Fatal("updateExistingPackageRegistryConfigs() error = nil")
	}
	want := []string{
		"server:10.0.0.1:" + deliveryregistry.ServerConfigPath + ":",
		"server:10.0.0.2:" + deliveryregistry.ServerConfigPath + ":",
		"server:10.0.0.1:" + deliveryregistry.AgentConfigPath + ":",
		"server:10.0.0.1:" + deliveryregistry.AgentConfigPath + ":old-10.0.0.1",
		"server:10.0.0.2:" + deliveryregistry.ServerConfigPath + ":old-10.0.0.2",
		"server:10.0.0.1:" + deliveryregistry.ServerConfigPath + ":old-10.0.0.1",
	}
	if !reflect.DeepEqual(operations, want) {
		t.Fatalf("server update operations = %v, want %v", operations, want)
	}
}

func TestUpdateExistingPackageRegistryConfigsRollsBackCommitFailure(t *testing.T) {
	o := NewJoinOptions(options.IOStreams{})
	o.deployConfig.ServerIPs = []string{"10.0.0.1"}
	o.serverSSHConfig = &sshutils.SSH{User: "server"}
	newPath, cleanup, err := writeTemporaryPackageRegistryConfig(&deliveryregistry.Config{
		Registry: "harbor.example.com/team-a", Scheme: deliveryregistry.SchemeHTTPS,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	o.packageRegistryConfigPath = newPath
	o.packageRegistryDownload = func(_ *sshutils.SSH, host, localPath, _ string) error {
		return deliveryregistry.Write(localPath, &deliveryregistry.Config{
			Registry: "harbor.example.com/team-a", Scheme: deliveryregistry.SchemeHTTPS,
			Username: "robot", Password: "old-" + host,
		})
	}
	var passwords []string
	o.packageRegistryCopy = func(_ *sshutils.SSH, _ string, localPath, _ string) error {
		config, loadErr := deliveryregistry.Load(localPath)
		if loadErr != nil {
			return loadErr
		}
		passwords = append(passwords, config.Password)
		return nil
	}

	err = o.updateExistingPackageRegistryConfigs(func() error {
		return errors.New("injected persist failure")
	})
	if err == nil || !strings.Contains(err.Error(), "node configs were restored") {
		t.Fatalf("updateExistingPackageRegistryConfigs() error = %v", err)
	}
	if want := []string{"", "old-10.0.0.1"}; !reflect.DeepEqual(passwords, want) {
		t.Fatalf("server update passwords = %v, want %v", passwords, want)
	}
}

func TestUpdateExistingPackageRegistryConfigsMigratesMissingConfig(t *testing.T) {
	o := NewJoinOptions(options.IOStreams{})
	o.deployConfig.ServerIPs = []string{"10.0.0.1"}
	o.serverSSHConfig = &sshutils.SSH{User: "server"}
	newPath, cleanup, err := writeTemporaryPackageRegistryConfig(&deliveryregistry.Config{
		Registry: "harbor.example.com/team-a", Scheme: deliveryregistry.SchemeHTTPS,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	o.packageRegistryConfigPath = newPath
	o.packageRegistryDownload = func(_ *sshutils.SSH, _, _, _ string) error {
		return os.ErrNotExist
	}
	o.packageRegistryExists = func(_ *sshutils.SSH, _, _ string) (bool, error) {
		return false, nil
	}
	var copied, removed bool
	o.packageRegistryCopy = func(_ *sshutils.SSH, _, _, _ string) error {
		copied = true
		return nil
	}
	o.packageRegistryRemove = func(_ *sshutils.SSH, _, _ string) error {
		removed = true
		return nil
	}

	err = o.updateExistingPackageRegistryConfigs(func() error {
		return errors.New("injected persist failure")
	})
	if err == nil || !copied || !removed {
		t.Fatalf("migration rollback = err %v, copied %t, removed %t", err, copied, removed)
	}
}
