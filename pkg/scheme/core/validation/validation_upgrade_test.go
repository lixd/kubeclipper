package validation

import (
	"strings"
	"testing"
)

func TestValidateKubernetesUpgradeVersion(t *testing.T) {
	tests := []struct {
		name    string
		current string
		target  string
		wantErr string
	}{
		{name: "patch upgrade", current: "v1.36.4", target: "v1.36.5"},
		{name: "one minor upgrade", current: "v1.36.4", target: "v1.37.0"},
		{name: "same version", current: "v1.36.4", target: "v1.36.4", wantErr: "already at version"},
		{name: "downgrade", current: "v1.36.4", target: "v1.35.8", wantErr: "cannot downgrade"},
		{name: "skip minor", current: "v1.36.4", target: "v1.38.0", wantErr: "too high"},
		{name: "change major", current: "v1.36.4", target: "v2.0.0", wantErr: "too high"},
		{name: "invalid target", current: "v1.36.4", target: "not-a-version", wantErr: "wrong version format"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateKubernetesUpgradeVersion("cluster-a", tt.current, tt.target)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateKubernetesUpgradeVersion() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateKubernetesUpgradeVersion() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}
