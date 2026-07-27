package options

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kubeclipper/kubeclipper/pkg/utils/sshutils"
)

func TestNewDeployOptionsAllowsCleanServerInitialization(t *testing.T) {
	opts := NewDeployOptions()
	if opts.KCServerHealthCheckTimeout != 2*time.Minute {
		t.Fatalf("KCServerHealthCheckTimeout = %s, want 2m", opts.KCServerHealthCheckTimeout)
	}
}

func TestDeployConfigCompleteMigratesLegacySSHTransports(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deploy-config.yaml")
	data := []byte(`
ssh:
  user: deploy
  pkFile: /keys/deploy
agentSSH:
  user: joined
  pkFile: /keys/joined
serverIPs:
- 10.0.0.1
defaultRegion: default
agents:
  10.0.0.1:
    agentID: aio
  10.0.0.2:
    agentID: joined
`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	config := NewDeployOptions()
	config.Config = path
	if err := config.Complete(); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	if got := config.Agents["10.0.0.1"].SSHTransportID; got != SSHTransportIDDeploy {
		t.Fatalf("server-local agent transport = %q, want %q", got, SSHTransportIDDeploy)
	}
	if got := config.Agents["10.0.0.2"].SSHTransportID; got != SSHTransportIDLegacyAgent {
		t.Fatalf("legacy joined agent transport = %q, want %q", got, SSHTransportIDLegacyAgent)
	}
	if got := config.SSHTransports[SSHTransportIDDeploy].PkFile; got != "/keys/deploy" {
		t.Fatalf("deploy transport key = %q", got)
	}
	if got := config.SSHTransports[SSHTransportIDLegacyAgent].PkFile; got != "/keys/joined" {
		t.Fatalf("legacy agent transport key = %q", got)
	}
}

func TestNormalizeSSHTransportsRejectsUnknownExplicitReference(t *testing.T) {
	config := NewDeployOptions()
	config.ServerIPs = []string{"10.0.0.1"}
	config.Agents = Agents{
		"10.0.0.2": {AgentID: "agent", SSHTransportID: "missing"},
	}
	if err := config.NormalizeSSHTransports(); err == nil {
		t.Fatal("NormalizeSSHTransports() accepted an unknown explicit transport")
	}
}

func TestRecordInitialAgentSSHTransportUsesDeployConfigForEveryAgent(t *testing.T) {
	config := NewDeployOptions()
	config.SSHConfig = &sshutils.SSH{User: "deploy", PkFile: "/keys/deploy", PrivateKey: "cached"}
	config.AgentSSHConfig = &sshutils.SSH{User: "legacy", PkFile: "/keys/legacy"}
	config.Agents = Agents{
		"10.0.0.1": {},
		"10.0.0.2": {SSHTransportID: SSHTransportIDLegacyAgent},
	}
	if err := config.RecordInitialAgentSSHTransport(); err != nil {
		t.Fatalf("RecordInitialAgentSSHTransport() error = %v", err)
	}
	for ip, metadata := range config.Agents {
		if metadata.SSHTransportID != SSHTransportIDDeploy {
			t.Fatalf("agent %s transport = %q, want deploy", ip, metadata.SSHTransportID)
		}
	}
	transport := config.SSHTransports[SSHTransportIDDeploy]
	if transport.PkFile != "/keys/deploy" || transport.PrivateKey != "" {
		t.Fatalf("persisted deploy transport = %+v", transport)
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
