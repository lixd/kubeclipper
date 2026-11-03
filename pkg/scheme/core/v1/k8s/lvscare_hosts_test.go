package k8s

import (
	"encoding/json"
	"github.com/txn2/txeh"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestLvsCareRefreshCarriesAPIServerDomain(t *testing.T) {
	for _, domain := range []string{"", "oci.test"} {
		cluster := &v1.Cluster{}
		cluster.Networking.WorkerNodeVip = "169.254.169.100"
		cluster.Networking.DNSDomain = domain
		steps, err := LvsCareRefreshSteps(cluster, &component.ExtraMetadata{Masters: component.NodeList{{ID: "master", IPv4: "192.0.2.1"}}}, []v1.StepNode{{ID: "worker"}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]interface{}
		if err := json.Unmarshal(steps[0].Commands[0].CustomCommand, &payload); err != nil {
			t.Fatal(err)
		}
		want := "apiserver.cluster.local"
		if domain != "" {
			want = "apiserver." + domain
		}
		if payload["apiServerDomainName"] != want {
			t.Fatalf("worker refresh payload domain=%v, want %s", payload["apiServerDomainName"], want)
		}
	}
}

func TestLvsCareSwitchesAPIHostsAndPreservesUnrelatedEntries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hosts")
	original := "127.0.0.1 localhost\n169.254.169.100 apiserver.oci.test\n192.0.2.99 unrelated.test\n"
	if err := os.WriteFile(path, []byte(original), 0644); err != nil {
		t.Fatal(err)
	}
	hosts, err := txeh.NewHosts(&txeh.HostsConfig{ReadFilePath: path, WriteFilePath: path})
	if err != nil {
		t.Fatal(err)
	}
	stepper := &LvsCare{APIServerDomainName: "apiserver.oci.test"}
	for _, address := range []string{"192.0.2.1", "169.254.169.100"} {
		if err := stepper.updateAPIHosts(hosts, address, component.Options{}); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		matches := 0
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			for _, field := range fields {
				if field == "apiserver.oci.test" {
					matches++
					if fields[0] != address {
						t.Errorf("stale API mapping: %s", line)
					}
				}
			}
		}
		if matches != 1 || !strings.Contains(string(data), "localhost") || !strings.Contains(string(data), "unrelated.test") {
			t.Fatalf("API switch must preserve unrelated hosts and leave one mapping: %s", data)
		}
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := stepper.updateAPIHosts(hosts, "192.0.2.2", component.Options{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("dryRun modified the hosts file")
	}
}
