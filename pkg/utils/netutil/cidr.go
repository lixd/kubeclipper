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
	"fmt"
	"net"
	"strings"
)

// ValidateSubnetOverlap validates that every CIDR parses, that each list
// carries at most one subnet per address family, and that no two subnets
// overlap — within the pod list, within the service list, or across the two.
// Overlapping (or malformed) pod/service subnets used to be accepted at
// cluster creation and left the cluster stuck in Installing (R6/R7), so
// creation paths call this before persisting anything.
func ValidateSubnetOverlap(podCIDRs, serviceCIDRs []string) error {
	pods, err := parseCIDRs("pod", podCIDRs)
	if err != nil {
		return err
	}
	services, err := parseCIDRs("service", serviceCIDRs)
	if err != nil {
		return err
	}
	if err := disjointWithin("pod", pods); err != nil {
		return err
	}
	if err := disjointWithin("service", services); err != nil {
		return err
	}
	for _, p := range pods {
		for _, s := range services {
			if subnetsOverlap(p, s) {
				return fmt.Errorf("pod subnet %s overlaps service subnet %s; use disjoint subnets",
					p.IPNet.String(), s.IPNet.String())
			}
		}
	}
	return nil
}

// HostAddress is a node address that cluster CIDRs must not cover, carried
// with the node name so the error can point at the offending node.
type HostAddress struct {
	Name string
	IP   string
}

// ValidateCIDRHostConflict rejects CIDRs covering one of the given host
// addresses. A pod or service range swallowing the node network breaks
// routing once the CNI applies (R9: pods=172.16.131.0/24 covering the node
// LAN was accepted). Cross-family pairs are ignored.
func ValidateCIDRHostConflict(kind string, cidrs []string, hosts []HostAddress) error {
	if len(cidrs) == 0 || len(hosts) == 0 {
		return nil
	}
	nets, err := parseCIDRs(kind, cidrs)
	if err != nil {
		return err
	}
	for _, h := range hosts {
		ip := net.ParseIP(h.IP)
		if ip == nil {
			return fmt.Errorf("invalid host address %q for node %s", h.IP, h.Name)
		}
		if v4 := ip.To4(); v4 != nil {
			ip = v4
		}
		for _, n := range nets {
			if n.isIPv4() != (ip.To4() != nil) {
				continue
			}
			if n.IPNet.Contains(ip) {
				return fmt.Errorf("%s subnet %s conflicts with node %s address %s; use subnets outside the node network",
					kind, n.IPNet.String(), h.Name, ip)
			}
		}
	}
	return nil
}

// disjointWithin reports the first overlapping pair inside one CIDR list.
// Nested entries like ["10.0.0.0/8","10.96.0.0/16"] used to pass because
// only the pod-vs-service lists were compared (R9 probe).
func disjointWithin(kind string, nets []parsedCIDR) error {
	for i := 0; i < len(nets); i++ {
		for j := i + 1; j < len(nets); j++ {
			if subnetsOverlap(nets[i], nets[j]) {
				return fmt.Errorf("%s subnet %s overlaps %s subnet %s; use disjoint subnets",
					kind, nets[i].IPNet.String(), kind, nets[j].IPNet.String())
			}
		}
	}
	return nil
}

// subnetsOverlap reports whether two parsed CIDRs of the same address family
// share address space. Prefixes either nest or are disjoint, so containing
// the other's network address is enough; cross-family pairs never overlap.
func subnetsOverlap(a, b parsedCIDR) bool {
	if a.isIPv4() != b.isIPv4() {
		return false
	}
	return a.IPNet.Contains(b.IP) || b.IPNet.Contains(a.IP)
}

type parsedCIDR struct {
	IPNet *net.IPNet
	IP    net.IP
}

func (c parsedCIDR) isIPv4() bool {
	return c.IP.To4() != nil
}

func parseCIDRs(kind string, cidrs []string) ([]parsedCIDR, error) {
	if len(cidrs) == 0 {
		return nil, fmt.Errorf("%s subnet is required", kind)
	}
	parsed := make([]parsedCIDR, 0, len(cidrs))
	seen := map[string]struct{}{}
	familySeen := map[string]bool{}
	for _, cidr := range cidrs {
		ip, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid %s subnet %q: %v", kind, cidr, err)
		}
		// net.ParseCIDR accepts IPv4-mapped IPv6 literals (::ffff:a.b.c.d/n)
		// and silently normalizes them onto the IPv4 space; reject them so
		// callers spell plain IPv4. Go 1.26+ returns 16-byte addresses for
		// every family, so the literal is the only usable signal — an IPv4
		// CIDR never contains ':'.
		if ip.To4() != nil && strings.Contains(cidr, ":") {
			return nil, fmt.Errorf("invalid %s subnet %q: IPv4-mapped IPv6 CIDRs are not allowed, use the IPv4 form", kind, cidr)
		}
		family := "ipv6"
		if ip.To4() != nil {
			family = "ipv4"
		}
		// canonicalize so duplicate detection is stable across spellings
		key := ipnet.String()
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("duplicate %s subnet %q", kind, cidr)
		}
		seen[key] = struct{}{}
		// kubeadm accepts one pod and one service subnet per family
		if familySeen[family] {
			return nil, fmt.Errorf("at most one %s %s subnet is allowed", family, kind)
		}
		familySeen[family] = true
		parsed = append(parsed, parsedCIDR{IPNet: ipnet, IP: ip})
	}
	return parsed, nil
}
