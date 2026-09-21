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

package netutil

import (
	"net"
	"strings"
	"testing"
)

func TestValidateSubnetOverlap(t *testing.T) {
	cases := []struct {
		name    string
		pods    []string
		svcs    []string
		wantErr string
	}{
		{
			name: "disjoint defaults",
			pods: []string{"172.25.0.0/16"},
			svcs: []string{"10.96.0.0/12"},
		},
		{
			name: "identical subnets",
			pods: []string{"10.96.0.0/16"},
			svcs: []string{"10.96.0.0/12"},
			// 10.96.0.0/12 contains 10.96.0.0/16
			wantErr: "overlaps",
		},
		{
			name:    "service inside pod range",
			pods:    []string{"10.0.0.0/8"},
			svcs:    []string{"10.96.0.0/12"},
			wantErr: "overlaps",
		},
		{
			name:    "duplicate pod subnets",
			pods:    []string{"172.25.0.0/16", "172.25.0.0/16"},
			svcs:    []string{"10.96.0.0/12"},
			wantErr: "duplicate",
		},
		{
			name:    "invalid cidr",
			pods:    []string{"not-a-cidr"},
			svcs:    []string{"10.96.0.0/12"},
			wantErr: "invalid pod subnet",
		},
		{
			name:    "empty pods",
			pods:    nil,
			svcs:    []string{"10.96.0.0/12"},
			wantErr: "required",
		},
		// R9 probe: nested entries inside one list were accepted. The
		// per-family cap (one subnet per family — what kubeadm takes) rejects
		// them first; disjointWithin still guards the overlap rule directly.
		{
			name:    "nested pod subnets",
			pods:    []string{"172.20.0.0/16", "172.20.1.0/24"},
			svcs:    []string{"10.96.0.0/12"},
			wantErr: "at most one ipv4 pod subnet is allowed",
		},
		{
			name:    "nested service subnets",
			pods:    []string{"172.20.0.0/16"},
			svcs:    []string{"10.96.0.0/12", "10.96.1.0/24"},
			wantErr: "at most one ipv4 service subnet is allowed",
		},
		// R9 probe: two IPv4 pod subnets were accepted; kubeadm rejects them
		{
			name:    "two ipv4 pod subnets",
			pods:    []string{"172.20.0.0/16", "172.21.0.0/16"},
			svcs:    []string{"10.96.0.0/12"},
			wantErr: "at most one ipv4 pod subnet",
		},
		{
			name:    "two ipv6 service subnets",
			pods:    []string{"172.20.0.0/16"},
			svcs:    []string{"fd00::/64", "fd01::/64"},
			wantErr: "at most one ipv6 service subnet",
		},
		{
			name:    "two ipv4 service subnets",
			pods:    []string{"172.20.0.0/16"},
			svcs:    []string{"10.96.0.0/12", "10.97.0.0/16"},
			wantErr: "at most one ipv4 service subnet",
		},
		{
			name:    "ipv4-mapped ipv6 cidr",
			pods:    []string{"::ffff:172.20.0.0/112"},
			svcs:    []string{"10.96.0.0/12"},
			wantErr: "IPv4-mapped IPv6",
		},
		// dual-stack: one subnet per family in each list must keep passing
		{
			name: "dual-stack disjoint",
			pods: []string{"172.20.0.0/16", "fd00:172:20::/64"},
			svcs: []string{"10.96.0.0/12", "fd00:10:96::/64"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSubnetOverlap(tc.pods, tc.svcs)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateCIDRHostConflict(t *testing.T) {
	hosts := []HostAddress{
		{Name: "dev-2", IP: "172.16.131.208"},
		{Name: "dev-3", IP: "172.16.131.146"},
	}
	cases := []struct {
		name    string
		kind    string
		cidrs   []string
		hosts   []HostAddress
		wantErr string
	}{
		{
			name:    "pod subnet covers node lan",
			kind:    "pod",
			cidrs:   []string{"172.16.131.0/24"},
			hosts:   hosts,
			wantErr: "pod subnet 172.16.131.0/24 conflicts with node dev-2 address 172.16.131.208",
		},
		{
			name:    "service subnet hits second node",
			kind:    "service",
			cidrs:   []string{"172.16.131.144/28"},
			hosts:   hosts,
			wantErr: "service subnet 172.16.131.144/28 conflicts with node dev-3 address 172.16.131.146",
		},
		{
			name:  "disjoint subnets pass",
			kind:  "pod",
			cidrs: []string{"172.20.0.0/16"},
			hosts: hosts,
		},
		{
			name:  "ipv6 cidr ignores ipv4 host",
			kind:  "pod",
			cidrs: []string{"fd00::/64"},
			hosts: hosts,
		},
		{
			name:  "ipv4 cidr ignores ipv6 host",
			kind:  "pod",
			cidrs: []string{"172.20.0.0/16"},
			hosts: []HostAddress{{Name: "v6-node", IP: "fd00::1"}},
		},
		{
			name:  "no hosts is a no-op",
			kind:  "pod",
			cidrs: []string{"172.16.0.0/16"},
			hosts: nil,
		},
		{
			name:    "boundary address rejected",
			kind:    "pod",
			cidrs:   []string{"172.16.131.200/29"},
			hosts:   []HostAddress{{Name: "dev-4", IP: "172.16.131.206"}},
			wantErr: "conflicts with node dev-4",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCIDRHostConflict(tc.kind, tc.cidrs, tc.hosts)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}

// disjointWithin is unreachable through ValidateSubnetOverlap while
// parseCIDRs caps each family at one subnet, but it must keep holding on its
// own: it is the overlap rule's last line of defense.
func TestDisjointWithin(t *testing.T) {
	_, v16, _ := net.ParseCIDR("172.20.0.0/16")
	_, v24, _ := net.ParseCIDR("172.20.1.0/24")
	nested := []parsedCIDR{{IPNet: v16, IP: v16.IP}, {IPNet: v24, IP: v24.IP}}
	err := disjointWithin("pod", nested)
	if err == nil || !strings.Contains(err.Error(), "pod subnet 172.20.0.0/16 overlaps pod subnet 172.20.1.0/24") {
		t.Fatalf("expected nested overlap error, got %v", err)
	}
	_, other, _ := net.ParseCIDR("172.21.0.0/16")
	disjoint := []parsedCIDR{{IPNet: v16, IP: v16.IP}, {IPNet: other, IP: other.IP}}
	if err := disjointWithin("pod", disjoint); err != nil {
		t.Fatalf("unexpected error for disjoint subnets: %v", err)
	}
}
