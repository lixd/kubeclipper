/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package options

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMetadataLogPort(t *testing.T) {
	tests := []struct {
		name string
		port int
		want int
		ok   bool
	}{
		{name: "default", want: 10260, ok: true},
		{name: "configured", port: 18080, want: 18080, ok: true},
		{name: "zero is default", port: 0, want: 10260, ok: true},
		{name: "negative", port: -1},
		{name: "too large", port: 65536},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := (Metadata{AgentLogPort: tt.port}).LogPort()
			if tt.ok {
				if err != nil || got != tt.want {
					t.Fatalf("LogPort() = (%d, %v), want (%d, nil)", got, err, tt.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("LogPort() = (%d, nil), want validation error", got)
			}
		})
	}
}

// A top-level initialPassword key in the deploy config is silently dropped by
// the decode and deploys then fall back to the built-in default password —
// reject the mistake instead (R22).
func TestCompleteRejectsTopLevelInitialPassword(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "deploy-config.yaml")
	body := `ssh:
  user: root
initialPassword: oops
serverIPs:
- 10.0.0.1
`
	if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c := NewDeployOptions()
	c.Config = cfg
	err := c.Complete()
	if err == nil {
		t.Fatal("top-level initialPassword accepted, want an error")
	}
	if !strings.Contains(err.Error(), "authentication.initialPassword") {
		t.Fatalf("error %q does not point at the correct location", err)
	}
}
