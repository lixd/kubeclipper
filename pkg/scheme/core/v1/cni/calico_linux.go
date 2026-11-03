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
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"

	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/utils/netutil"
)

func clearCalicoNetwork(ctx context.Context, calico *v1.Calico) error {
	if err := clearCNINamespaces(ctx, "/var/run/netns"); err != nil {
		return err
	}
	links, err := netlink.LinkList()
	if err != nil {
		return fmt.Errorf("list Calico links: %w", err)
	}
	for _, link := range links {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := link.Attrs().Name
		owned := name == "vxlan.calico" || name == "vxlan-v6.calico" || (strings.HasPrefix(name, "cali") && netutil.IsLinkVeth(link))
		if name == "tunl0" && calico != nil {
			owned = calico.Mode == CalicoNetworkIPIPAll || calico.Mode == CalicoNetworkIPIPSubnet
		}
		if owned {
			if err := netutil.DeleteLink(name); err != nil {
				return fmt.Errorf("remove Calico link %s: %w", name, err)
			}
		}
	}
	return clearClusterRules(ctx)
}

func clearCNINamespaces(ctx context.Context, directory string) error {
	entries, err := os.ReadDir(directory)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !strings.HasPrefix(entry.Name(), "cni-") || entry.IsDir() {
			continue
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("refuse CNI namespace symlink %s", entry.Name())
		}
		path := filepath.Join(directory, entry.Name())
		if err := unix.Unmount(path, unix.MNT_DETACH); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			return fmt.Errorf("unmount CNI namespace %s: %w", path, err)
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func clearClusterRules(ctx context.Context) error {
	// nft and legacy rules may coexist. Clean the selected backend and any
	// populated legacy backend, without flushing unrelated host chains.
	commands := []string{"iptables", "ip6tables"}
	for _, family := range []struct{ prefix, tables string }{{"iptables-legacy", "/proc/net/ip_tables_names"}, {"ip6tables-legacy", "/proc/net/ip6_tables_names"}} {
		tables, err := os.ReadFile(family.tables)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		if len(strings.TrimSpace(string(tables))) != 0 {
			commands = append(commands, family.prefix)
		}
	}
	for _, prefix := range commands {
		save, err := exec.LookPath(prefix + "-save")
		if err != nil {
			if prefix == "ip6tables" && errors.Is(err, exec.ErrNotFound) {
				continue
			}
			return err
		}
		data, err := exec.CommandContext(ctx, save).Output()
		if err != nil {
			return fmt.Errorf("read %s rules: %w", prefix, err)
		}
		script := clusterRuleDeletion(string(data))
		if script == "" {
			continue
		}
		restore := exec.CommandContext(ctx, prefix+"-restore", "--wait", "--noflush")
		restore.Stdin = strings.NewReader(script)
		if out, err := restore.CombinedOutput(); err != nil {
			return fmt.Errorf("remove %s cluster rules: %w: %s", prefix, err, out)
		}
	}
	return nil
}
