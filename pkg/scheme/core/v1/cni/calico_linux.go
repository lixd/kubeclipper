//go:build linux
// +build linux

/*
 *
 *  * Copyright 2024 KubeClipper Authors.
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

package cni

import (
	"strings"

	"golang.org/x/sys/unix"

	"github.com/kubeclipper/kubeclipper/pkg/logger"
	"github.com/kubeclipper/kubeclipper/pkg/utils/netutil"
	"github.com/vishvananda/netlink"
)

const (
	defaultVTEPDeviceName = "vxlan.calico"
	defaultIPIPDeviceName = "tunl0"
	ipipKernelModuleName  = "ipip"
)

// clearCalicoNICs cleans up all the NICs created by calico
func clearCalicoNICs(mode string) {
	clearCalicoNICsWithOps(mode, netutil.DeleteLink, unix.DeleteModule, netlink.LinkList)
}

func clearCalicoNICsWithOps(mode string, deleteLink func(string) error, deleteModule func(string, int) error, listLinks func() ([]netlink.Link, error)) {
	switch mode {
	case CalicoNetworkIPIPAll, CalicoNetworkIPIPSubnet:
		logger.Infof("remove IPIP tunnel device: %s", defaultIPIPDeviceName)
		if err := deleteLink(defaultIPIPDeviceName); err != nil {
			logger.Errorf("failed to remove IPIP tunnel device: %v", err)
		}
	case CalicoNetworkVXLANAll, CalicoNetworkVXLANSubnet:
		logger.Infof("remove VTEP device: %s", defaultVTEPDeviceName)
		if err := deleteLink(defaultVTEPDeviceName); err != nil {
			logger.Errorf("failed to remove VTEP device: %v", err)
		}
	}

	// Calico may load IPIP even when VXLAN is selected. Unloading the module
	// removes its automatically managed tunl0 fallback device.
	logger.Info("disable IPIP kernel module")
	if err := deleteModule(ipipKernelModuleName, 0); err != nil {
		logger.Errorf("failed to disable IPIP kernel module: %v", err)
	}

	links, err := listLinks()
	if err != nil {
		logger.Errorf("failed to list links: %v", err)
		return
	}
	// remove veth pairs created by calico
	for _, link := range links {
		name := link.Attrs().Name
		if !(strings.Contains(name, "cali") && netutil.IsLinkVeth(link)) {
			continue
		}
		if err := deleteLink(name); err != nil {
			logger.Errorf("failed to remove link %s: %v", name, err)
		}
	}
}
