//go:build linux

package cni

import (
	"context"
	"github.com/moby/sys/mountinfo"
	"os"
	"path/filepath"
	"testing"
)

func TestCalicoRuntimeUnmountTargetsProtectHostFilesystems(t *testing.T) {
	for _, test := range []struct {
		name   string
		mounts []*mountinfo.Info
		want   int
		reject bool
	}{
		{name: "host cgroup and unrelated sibling are preserved", mounts: []*mountinfo.Info{{Mountpoint: "/sys/fs/cgroup", FSType: "cgroup2"}, {Mountpoint: "/run/calico-other/cgroup", FSType: "cgroup2"}}},
		{name: "only own cgroup view is unmounted", mounts: []*mountinfo.Info{{Mountpoint: "/run/calico/cgroup", FSType: "cgroup2"}}, want: 1},
		{name: "runtime root is mounted", mounts: []*mountinfo.Info{{Mountpoint: "/run/calico", FSType: "tmpfs"}}, reject: true},
		{name: "unexpected nested mount", mounts: []*mountinfo.Info{{Mountpoint: "/run/calico/other", FSType: "ext4"}}, reject: true},
		{name: "unexpected cgroup filesystem", mounts: []*mountinfo.Info{{Mountpoint: "/run/calico/cgroup", FSType: "ext4"}}, reject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			targets, err := calicoRuntimeUnmountTargets("/run/calico", test.mounts)
			if (err != nil) != test.reject || len(targets) != test.want {
				t.Fatalf("targets=%v error=%v", targets, err)
			}
		})
	}
}

func TestCalicoRuntimeDirectoryCleanup(t *testing.T) {
	parent := t.TempDir()
	directory := filepath.Join(parent, "calico")
	if err := os.Mkdir(directory, 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(directory, "old-state")
	if err := os.WriteFile(file, []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := clearCalicoRuntimeDirectory(ctx, directory); err == nil {
		t.Fatal("cancellation must preserve runtime state")
	}
	if _, err := os.Stat(file); err != nil {
		t.Fatal(err)
	}
	if err := clearCalicoRuntimeDirectory(context.Background(), directory); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("runtime directory not removed: %v", err)
	}
	if err := clearCalicoRuntimeDirectory(context.Background(), directory); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(parent, directory); err != nil {
		t.Fatal(err)
	}
	if err := clearCalicoRuntimeDirectory(context.Background(), directory); err == nil {
		t.Fatal("runtime symlink must not be followed")
	}
	if _, err := os.Stat(parent); err != nil {
		t.Fatal("symlink target changed")
	}
}
