package options

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewDeployOptionsAllowsCleanServerInitialization(t *testing.T) {
	opts := NewDeployOptions()
	if opts.KCServerHealthCheckTimeout != 2*time.Minute {
		t.Fatalf("KCServerHealthCheckTimeout = %s, want 2m", opts.KCServerHealthCheckTimeout)
	}
}

func TestDeployConfigWriteUsesPrivatePermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deploy-config.yaml")
	// The fixture starts with legacy broad permissions to verify they are repaired.
	if err := os.WriteFile(path, []byte("old"), 0644); err != nil { //nolint:gosec // Legacy mode is the behavior under test.
		t.Fatal(err)
	}
	config := NewDeployOptions()
	config.Config = path
	if err := config.Write(); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("deploy config mode = %#o, want 0600", got)
	}
}
