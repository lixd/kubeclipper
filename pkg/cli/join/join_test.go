package join

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
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
