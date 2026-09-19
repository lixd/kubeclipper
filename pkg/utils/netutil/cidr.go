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
)

// ValidateSubnetOverlap validates that every CIDR parses and that no pod
// subnet overlaps a service subnet. Overlapping (or malformed) pod/service
// subnets used to be accepted at cluster creation and left the cluster stuck
// in Installing (R6/R7), so creation paths call this before persisting
// anything.
func ValidateSubnetOverlap(podCIDRs, serviceCIDRs []string) error {
	pods, err := parseCIDRs("pod", podCIDRs)
	if err != nil {
		return err
	}
	services, err := parseCIDRs("service", serviceCIDRs)
	if err != nil {
		return err
	}
	for _, p := range pods {
		for _, s := range services {
			if p.IPNet.Contains(s.IP) || s.IPNet.Contains(p.IP) {
				return fmt.Errorf("pod subnet %s overlaps service subnet %s; use disjoint subnets",
					p.IPNet.String(), s.IPNet.String())
			}
		}
	}
	return nil
}

type parsedCIDR struct {
	IPNet *net.IPNet
	IP    net.IP
}

func parseCIDRs(kind string, cidrs []string) ([]parsedCIDR, error) {
	if len(cidrs) == 0 {
		return nil, fmt.Errorf("%s subnet is required", kind)
	}
	parsed := make([]parsedCIDR, 0, len(cidrs))
	seen := map[string]struct{}{}
	for _, cidr := range cidrs {
		ip, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("invalid %s subnet %q: %v", kind, cidr, err)
		}
		// canonicalize so duplicate detection is stable across spellings
		key := ipnet.String()
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("duplicate %s subnet %q", kind, cidr)
		}
		seen[key] = struct{}{}
		parsed = append(parsed, parsedCIDR{IPNet: ipnet, IP: ip})
	}
	return parsed, nil
}
