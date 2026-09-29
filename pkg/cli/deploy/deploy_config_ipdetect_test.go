/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package deploy

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
)

// The deploy-config's ipDetect/nodeIPDetect must survive the load: they feed
// the generated agent configs. A deploy on a multi-NIC host where the keys are
// lost silently registers agents on the wrong management IP (observed live in
// R29: agents came up on docker-bridge addresses and became unreachable).
func TestDeployConfigReadsIPDetect(t *testing.T) {
	const cfg = `ssh:
  user: root
  pkFile: /root/.ssh/id_ed25519
  port: 22
serverIPs:
- 172.16.131.208
agents:
  172.16.131.146:
    region: default
ipDetect: interface=ens3
nodeIPDetect: cidr=172.16.131.0/24
packageRegistry: 172.16.131.146:5003
defaultRegion: default
authentication:
  initialPassword: Abcd1234
  jwtSecret: x
  loginHistoryMaximumEntries: 100
  loginHistoryRetentionPeriod: 168h0m0s
`
	dir := t.TempDir()
	f := filepath.Join(dir, "dc.yaml")
	if err := os.WriteFile(f, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	c := options.NewDeployOptions()
	c.Config = f
	if err := c.Complete(); err != nil {
		t.Fatalf("Complete() = %v", err)
	}
	if c.IPDetect != "interface=ens3" {
		t.Fatalf("IPDetect = %q, want interface=ens3", c.IPDetect)
	}
	if c.NodeIPDetect != "cidr=172.16.131.0/24" {
		t.Fatalf("NodeIPDetect = %q, want cidr=172.16.131.0/24", c.NodeIPDetect)
	}
}
