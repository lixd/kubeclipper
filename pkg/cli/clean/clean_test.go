package clean

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

func TestMergeOnlineAgentsIncludesJoinedNodes(t *testing.T) {
	config := options.NewDeployOptions()
	config.Agents.Add("10.0.0.1", options.Metadata{AgentID: "deployed"})
	nodes := &kc.NodesList{Items: []v1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: "deployed"}, Status: v1.NodeStatus{Ipv4DefaultIP: "10.0.0.1"}},
		{ObjectMeta: metav1.ObjectMeta{Name: "joined"}, Status: v1.NodeStatus{NodeIpv4DefaultIP: "10.0.0.2"}},
	}}

	if err := mergeOnlineAgents(config, nodes); err != nil {
		t.Fatalf("mergeOnlineAgents() error: %v", err)
	}
	if got, want := config.Agents.ListIP(), []string{"10.0.0.1", "10.0.0.2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("agents = %v, want %v", got, want)
	}
}

func TestMergeOnlineAgentsRejectsNodeWithoutAddress(t *testing.T) {
	config := options.NewDeployOptions()
	if err := mergeOnlineAgents(config, &kc.NodesList{Items: []v1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "unknown"}}}}); err == nil {
		t.Fatal("mergeOnlineAgents() unexpectedly accepted node without an address")
	}
}
