/*
 *
 *  * Copyright 2021 KubeClipper Authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package deploy

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
	"sigs.k8s.io/yaml"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
)

func TestDefaultDeliveryPolicyConfigMap(t *testing.T) {
	cm, err := defaultDeliveryPolicyConfigMap()
	if err != nil {
		t.Fatalf("defaultDeliveryPolicyConfigMap() error: %v", err)
	}
	if cm.Name != deliveryapis.DeliveryPolicyConfigMapName {
		t.Fatalf("configmap name = %q", cm.Name)
	}
	var policy deliveryapis.SupportPolicy
	if err = json.Unmarshal([]byte(cm.Data[deliveryapis.DeliveryPolicyConfigMapKey]), &policy); err != nil {
		t.Fatalf("unmarshal policy: %v", err)
	}
	if err = policy.Validate(); err != nil {
		t.Fatalf("default policy validation: %v", err)
	}
}

func TestDeployOptions_getEtcdTemplateContent(t *testing.T) {
	d := NewDeployOptions(options.IOStreams{})
	d.deployConfig.ServerIPs = []string{"192.168.234.3", "192.168.234.4", "192.168.234.5"}
	d.servers = map[string]string{
		"192.168.234.3": "master1",
		"192.168.234.4": "master2",
		"192.168.234.5": "master3",
	}

	for _, s := range d.deployConfig.ServerIPs {
		t.Log(d.getEtcdTemplateContent(s))
	}
}

func TestDeployOptions_getKcServerConfigTemplateContent(t *testing.T) {
	d := NewDeployOptions(options.IOStreams{})
	d.deployConfig.ServerIPs = []string{"192.168.234.3", "192.168.234.4", "192.168.234.5"}
	d.servers = map[string]string{
		"192.168.234.3": "master1",
		"192.168.234.4": "master2",
		"192.168.234.5": "master3",
	}

	for _, s := range d.deployConfig.ServerIPs {
		t.Log(d.deployConfig.GetKcServerConfigTemplateContent(s))
	}
}

func TestDeployOptions_getKcAgentConfigTemplateContent(t *testing.T) {
	d := NewDeployOptions(options.IOStreams{})
	d.deployConfig.ServerIPs = []string{"192.168.234.3", "192.168.234.4", "192.168.234.5"}
	d.servers = map[string]string{
		"192.168.234.3": "master1",
		"192.168.234.4": "master2",
		"192.168.234.5": "master3",
	}
	metadata := options.Metadata{
		Region:  d.deployConfig.DefaultRegion,
		FloatIP: "1.1.1.1",
	}
	for range d.deployConfig.ServerIPs {
		metadata.AgentID = uuid.New().String()
		t.Log(d.deployConfig.GetKcAgentConfigTemplateContent(metadata))
	}
}

func TestDeployOptions_getKcConsoleTemplateContent(t *testing.T) {
	d := NewDeployOptions(options.IOStreams{})
	d.deployConfig.ServerIPs = []string{"192.168.234.3", "192.168.234.4", "192.168.234.5"}
	d.servers = map[string]string{
		"192.168.234.3": "master1",
		"192.168.234.4": "master2",
		"192.168.234.5": "master3",
	}

	t.Log(d.getKcConsoleTemplateContent())
}

func TestDeployOptions_nodeRole(t *testing.T) {
	tests := []struct {
		name      string
		serverIPs []string
		agentIPs  []string
		queryIP   string
		wantRole  string
	}{
		{
			name:      "server only",
			serverIPs: []string{"10.0.0.1", "10.0.0.2"},
			agentIPs:  []string{"10.0.0.3"},
			queryIP:   "10.0.0.1",
			wantRole:  "server",
		},
		{
			name:      "agent only",
			serverIPs: []string{"10.0.0.1"},
			agentIPs:  []string{"10.0.0.2", "10.0.0.3"},
			queryIP:   "10.0.0.2",
			wantRole:  "agent",
		},
		{
			name:      "AIO node is server+agent",
			serverIPs: []string{"10.0.0.1"},
			agentIPs:  []string{"10.0.0.1", "10.0.0.2"},
			queryIP:   "10.0.0.1",
			wantRole:  "server+agent",
		},
		{
			name:      "unknown IP returns empty",
			serverIPs: []string{"10.0.0.1"},
			agentIPs:  []string{"10.0.0.2"},
			queryIP:   "10.0.0.99",
			wantRole:  "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewDeployOptions(options.IOStreams{})
			d.deployConfig.ServerIPs = tt.serverIPs
			d.deployConfig.Agents = make(options.Agents)
			for _, ip := range tt.agentIPs {
				d.deployConfig.Agents[ip] = options.Metadata{}
			}
			got := d.nodeRole(tt.queryIP)
			if got != tt.wantRole {
				t.Errorf("nodeRole(%q) = %q, want %q", tt.queryIP, got, tt.wantRole)
			}
		})
	}
}

func TestDeployOptionsValidateArgsRequiresPackageRegistry(t *testing.T) {
	d := NewDeployOptions(options.IOStreams{})
	d.deployConfig.ServerIPs = []string{"10.0.0.1"}
	d.deployConfig.SSHConfig.Password = "secret"

	if err := d.ValidateArgs(); err == nil {
		t.Fatalf("ValidateArgs() expected package registry error")
	}

	d.deployConfig.PackageRegistry = "registry.local:5000"
	if err := d.ValidateArgs(); err != nil {
		t.Fatalf("ValidateArgs() unexpected error: %+v", err)
	}
}

func TestDeployOptionsValidateArgsDoesNotRequirePackage(t *testing.T) {
	d := NewDeployOptions(options.IOStreams{})
	d.deployConfig.ServerIPs = []string{"10.0.0.1"}
	d.deployConfig.SSHConfig.Password = "secret"
	d.deployConfig.PackageRegistry = "registry.local:5000"

	if err := d.ValidateArgs(); err != nil {
		t.Fatalf("ValidateArgs() unexpected error: %+v", err)
	}
}

func TestPackageRegistryCredentialsAreNotSerializedInDeployConfig(t *testing.T) {
	d := NewDeployOptions(options.IOStreams{})
	d.deployConfig.PackageRegistry = "harbor.example.com/kubeclipper"
	d.packageRegistryConfig = &deliveryregistry.Config{
		Registry: "harbor.example.com/kubeclipper",
		Scheme:   deliveryregistry.SchemeHTTPS,
		Username: "robot$kc",
		Password: "super-secret-token",
		CA:       "private-ca-data",
	}
	data, err := yaml.Marshal(d.deployConfig) // #nosec G117 -- the assertion verifies Registry secrets are absent from this serialized config.
	if err != nil {
		t.Fatalf("marshal deploy config: %v", err)
	}
	serialized := string(data)
	if !strings.Contains(serialized, "packageRegistry: harbor.example.com/kubeclipper") {
		t.Fatalf("deploy config lost public registry address: %s", serialized)
	}
	for _, secret := range []string{"super-secret-token", "robot$kc", "private-ca-data", "skipTLSVerify"} {
		if strings.Contains(serialized, secret) {
			t.Fatalf("deploy config contains package registry secret field %q", secret)
		}
	}
}
