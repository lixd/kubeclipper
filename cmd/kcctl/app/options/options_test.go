package options

import (
	"testing"
	"time"
)

func TestNewDeployOptionsAllowsCleanServerInitialization(t *testing.T) {
	opts := NewDeployOptions()
	if opts.KCServerHealthCheckTimeout != 2*time.Minute {
		t.Fatalf("KCServerHealthCheckTimeout = %s, want 2m", opts.KCServerHealthCheckTimeout)
	}
}
