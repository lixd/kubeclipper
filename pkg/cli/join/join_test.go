package join

import (
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
