package join

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
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
