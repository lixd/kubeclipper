//go:build linux

package cni

import (
	"context"
	"errors"
	"github.com/moby/sys/mountinfo"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
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

// Run with KC_TEST_CALICO_MOUNTS=1 in a privileged Linux test container.
// The child has its own mount namespace, so this never changes host mounts.
func TestCalicoBusyCgroupRuntimeView(t *testing.T) {
	if os.Getenv("KC_TEST_CALICO_MOUNTS") != "1" {
		t.Skip("requires a privileged Linux mount test container")
	}
	if os.Getenv("KC_TEST_CALICO_MOUNT_CHILD") != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCalicoBusyCgroupRuntimeView$", "-test.v")
		cmd.Env = append(os.Environ(), "KC_TEST_CALICO_MOUNT_CHILD=1")
		cmd.SysProcAttr = &syscall.SysProcAttr{Cloneflags: unix.CLONE_NEWNS}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("isolated mount test: %v\n%s", err, out)
		}
		return
	}
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), "calico")
	view := filepath.Join(directory, "cgroup")
	if err := os.MkdirAll(view, 0700); err != nil {
		t.Fatal(err)
	}
	if err := unix.Mount("/sys/fs/cgroup", view, "", unix.MS_BIND, ""); err != nil {
		t.Fatal(err)
	}
	defer unix.Unmount(view, unix.MNT_DETACH)
	held, err := os.Open(view)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	if err := unix.Unmount(view, 0); !errors.Is(err, unix.EBUSY) {
		t.Fatalf("fixture must reproduce a genuinely busy cgroup mount: %v", err)
	}
	if err := clearCalicoRuntimeDirectory(context.Background(), directory); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("runtime directory remains: %v", err)
	}
	var fs unix.Statfs_t
	if err := unix.Statfs("/sys/fs/cgroup", &fs); err != nil || fs.Type != unix.CGROUP2_SUPER_MAGIC {
		t.Fatalf("host cgroup filesystem must remain available: type=%x error=%v", fs.Type, err)
	}
}
