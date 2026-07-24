//go:build linux
// +build linux

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

package cni

import (
	"reflect"
	"testing"

	"github.com/vishvananda/netlink"
)

func TestClearCalicoNICsRemovesModeDeviceAndCalicoVeths(t *testing.T) {
	var deleted []string
	deleteLink := func(name string) error {
		deleted = append(deleted, name)
		return nil
	}
	deleteModule := func(string, int) error { return nil }
	listLinks := func() ([]netlink.Link, error) {
		return []netlink.Link{
			&netlink.Veth{LinkAttrs: netlink.LinkAttrs{Name: "cali1234"}},
			&netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "calico-unrelated"}},
			&netlink.Veth{LinkAttrs: netlink.LinkAttrs{Name: "veth-other"}},
		}, nil
	}

	clearCalicoNICsWithOps(CalicoNetworkIPIPAll, deleteLink, deleteModule, listLinks)
	if want := []string{defaultIPIPDeviceName, "cali1234"}; !reflect.DeepEqual(deleted, want) {
		t.Fatalf("deleted links = %v, want %v", deleted, want)
	}

	deleted = nil
	clearCalicoNICsWithOps(CalicoNetworkVXLANAll, deleteLink, deleteModule, listLinks)
	if want := []string{defaultVTEPDeviceName, "cali1234"}; !reflect.DeepEqual(deleted, want) {
		t.Fatalf("deleted links = %v, want %v", deleted, want)
	}
}
