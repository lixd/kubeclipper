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

package v1

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	apimachineryErrors "k8s.io/apimachinery/pkg/api/errors"

	mock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	"github.com/kubeclipper/kubeclipper/pkg/query"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func newBackupHandler(t *testing.T, backups []v1.Backup) (*handler, *mock.MockOperator) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := mock.NewMockOperator(ctrl)
	list := &v1.BackupList{Items: backups}
	m.EXPECT().ListBackups(gomock.Any(), gomock.Any()).Return(list, nil).AnyTimes()
	h := newHandler(nil, m, nil, nil, nil, nil, nil)
	return h, m
}

// newCheckHandler wires a handler whose operator reports the given nodes;
// GetClusterEx always answers "not found" so the duplicate-name gate passes.
func newCheckHandler(t *testing.T, nodes []v1.Node) (*handler, *mock.MockOperator) {
	t.Helper()
	ctrl := gomock.NewController(t)
	m := mock.NewMockOperator(ctrl)
	m.EXPECT().GetClusterEx(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	m.EXPECT().ListNodes(gomock.Any(), gomock.Any()).Return(&v1.NodeList{Items: nodes}, nil).AnyTimes()
	h := newHandler(nil, m, nil, nil, nil, nil, nil)
	return h, m
}

// Regression for the R7 finding: the describe endpoint previously passed
// resourceVersion into GetBackupEx as the backup name, so every existing
// backup was reported as NotFound.
func TestFindBackupExisting(t *testing.T) {
	backups := []v1.Backup{}
	b1 := v1.Backup{}
	b1.Name = "cluster-a-manual-1"
	backups = append(backups, b1)
	b2 := v1.Backup{}
	b2.Name = "cluster-b-cron-2"
	backups = append(backups, b2)

	h, _ := newBackupHandler(t, backups)

	got, err := h.findBackup(context.Background(), "cluster-b-cron-2")
	if err != nil {
		t.Fatalf("findBackup existing backup failed: %v", err)
	}
	if got.Name != "cluster-b-cron-2" {
		t.Fatalf("got backup %q, want cluster-b-cron-2", got.Name)
	}
}

func TestFindBackupNotFound(t *testing.T) {
	backups := []v1.Backup{}
	b1 := v1.Backup{}
	b1.Name = "cluster-a-manual-1"
	backups = append(backups, b1)

	h, _ := newBackupHandler(t, backups)

	_, err := h.findBackup(context.Background(), "no-such-backup")
	if err == nil {
		t.Fatalf("expected NotFound for missing backup")
	}
	if !apimachineryErrors.IsNotFound(err) {
		t.Fatalf("error = %v, want NotFound", err)
	}
}

// Regression test for R7 N9: the default watch timeout must be in seconds,
// not nanoseconds — a nanosecond-scale timer fires before the watch loop
// starts and closes the stream immediately.
func TestDefaultWatchTimeout(t *testing.T) {
	if got := defaultWatchTimeout(&query.Query{}); got < query.MinTimeoutSeconds*time.Second {
		t.Fatalf("default watch timeout %v is below MinTimeoutSeconds", got)
	}
	seconds := int64(42)
	if got := defaultWatchTimeout(&query.Query{TimeoutSeconds: &seconds}); got != 42*time.Second {
		t.Fatalf("explicit TimeoutSeconds not honored: got %v, want 42s", got)
	}
}

// createClusterCheck fixture: the cluster passes the dual-stack/overlap
// gates unless pods/svcs say otherwise, and claims exactly the given nodes.
func checkClusterFixture(pods, svcs []string, nodeIDs ...string) *v1.Cluster {
	c := &v1.Cluster{}
	c.Name = "r9-check"
	c.Networking.Pods.CIDRBlocks = pods
	c.Networking.Services.CIDRBlocks = svcs
	for _, id := range nodeIDs {
		c.Masters = append(c.Masters, v1.WorkerNode{ID: id})
	}
	return c
}

func checkNodeFixture(name, ip string) v1.Node {
	n := v1.Node{}
	n.Name = name
	n.Status.NodeIpv4DefaultIP = ip
	return n
}

func TestCreateClusterCheckOverlapRejectedBeforeStore(t *testing.T) {
	h, _ := newCheckHandler(t, nil)
	// no store expectations: any GetClusterEx/ListNodes call fails the test
	c := checkClusterFixture([]string{"10.96.0.0/16"}, []string{"10.96.0.0/12"}, "node-1")
	err := h.createClusterCheck(context.Background(), c)
	if err == nil || !strings.Contains(err.Error(), "overlaps") {
		t.Fatalf("expected overlap rejection, got %v", err)
	}
}

func TestCreateClusterCheckHostConflict(t *testing.T) {
	nodes := []v1.Node{
		checkNodeFixture("node-1", "172.16.131.208"),
		checkNodeFixture("node-2", "172.16.131.146"),
	}
	h, _ := newCheckHandler(t, nodes)
	c := checkClusterFixture([]string{"172.16.131.0/24"}, []string{"10.96.0.0/12"}, "node-1", "node-2")
	err := h.createClusterCheck(context.Background(), c)
	if err == nil || !strings.Contains(err.Error(), "pod subnet 172.16.131.0/24 conflicts with node node-1 address 172.16.131.208") {
		t.Fatalf("expected host conflict rejection, got %v", err)
	}

	// same check on the service list
	c2 := checkClusterFixture([]string{"172.20.0.0/16"}, []string{"172.16.131.144/28"}, "node-1", "node-2")
	err = h.createClusterCheck(context.Background(), c2)
	if err == nil || !strings.Contains(err.Error(), "service subnet 172.16.131.144/28 conflicts with node node-2 address 172.16.131.146") {
		t.Fatalf("expected service host conflict rejection, got %v", err)
	}
}

func TestCreateClusterCheckPasses(t *testing.T) {
	nodes := []v1.Node{
		checkNodeFixture("node-1", "172.16.131.208"),
		checkNodeFixture("node-2", "172.16.131.146"),
	}
	h, _ := newCheckHandler(t, nodes)
	c := checkClusterFixture(
		[]string{"172.20.0.0/16", "fd00:172:20::/64"},
		[]string{"10.96.0.0/12", "fd00:10:96::/64"},
		"node-1", "node-2")
	if err := h.createClusterCheck(context.Background(), c); err != nil {
		t.Fatalf("expected dual-stack creation to pass, got %v", err)
	}
}

func TestCreateClusterCheckSkipsIPLessNodes(t *testing.T) {
	nodes := []v1.Node{
		checkNodeFixture("node-1", ""), // no discovered address yet
		checkNodeFixture("node-2", "172.16.131.146"),
	}
	h, _ := newCheckHandler(t, nodes)
	c := checkClusterFixture([]string{"172.20.0.0/16"}, []string{"10.96.0.0/12"}, "node-1", "node-2")
	if err := h.createClusterCheck(context.Background(), c); err != nil {
		t.Fatalf("expected clean subnets to pass, got %v", err)
	}
}
