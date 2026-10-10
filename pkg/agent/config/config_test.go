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

package config

import (
	"testing"
	"time"
)

// The agent config rendered by the server writes durations as strings
// (nodeStatusUpdateFrequency: 1m) and the server reads that file back when
// checking whether an agent is already deployed. Both sides must agree on the
// parsing, so a bare yaml.Unmarshal is not good enough.
func TestLoadFromBytesParsesDurationStrings(t *testing.T) {
	payload := []byte(`agentID: 11111111-2222-4333-8444-555555555555
metadata:
  region: default
registerNode: true
nodeStatusUpdateFrequency: 1m
apiServer:
  endpoint: https://kubeclipper.example.com:8080
  logAddress: ":10260"
`)

	conf, err := LoadFromBytes(payload)
	if err != nil {
		t.Fatalf("LoadFromBytes returned error: %v", err)
	}
	if conf.NodeStatusUpdateFrequency != time.Minute {
		t.Fatalf("NodeStatusUpdateFrequency = %v, want %v", conf.NodeStatusUpdateFrequency, time.Minute)
	}
	if conf.AgentID != "11111111-2222-4333-8444-555555555555" {
		t.Fatalf("AgentID = %q, want the id from the payload", conf.AgentID)
	}
	if conf.Metadata.Region != "default" {
		t.Fatalf("Metadata.Region = %q, want default", conf.Metadata.Region)
	}
}
