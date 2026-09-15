package publisher

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

const (
	publishLockHelperEnv = "KUBECLIPPER_PUBLISH_LOCK_HELPER"
	publishLockMarkerEnv = "KUBECLIPPER_PUBLISH_LOCK_MARKER"
)

func TestMain(m *testing.M) {
	if os.Getenv(PublishLockDirEnv) != "" {
		os.Exit(m.Run())
	}
	dir, err := os.MkdirTemp("", "kubeclipper-publisher-lock-test-")
	if err != nil {
		panic(err)
	}
	if err = os.Setenv(PublishLockDirEnv, dir); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func TestPublishReferenceLockSerializesAcrossProcesses(t *testing.T) {
	lockDir := filepath.Join(t.TempDir(), "locks")
	marker := filepath.Join(t.TempDir(), "acquired")
	t.Setenv(PublishLockDirEnv, lockDir)
	unlock, err := lockPublishReference(t.Context(), "registry.example.test/project/package:v2.0.0")
	if err != nil {
		t.Fatal(err)
	}

	command := exec.Command(os.Args[0], "-test.run=^TestPublishReferenceLockHelper$") //nolint:gosec // Re-executes this test binary with a fixed argument.
	command.Env = append(os.Environ(),
		publishLockHelperEnv+"=1",
		publishLockMarkerEnv+"="+marker,
		PublishLockDirEnv+"="+lockDir,
	)
	if err = command.Start(); err != nil {
		unlock()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			if killErr := command.Process.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				t.Errorf("kill publisher lock helper: %v", killErr)
			}
			if _, waitErr := command.Process.Wait(); waitErr != nil {
				t.Logf("publisher lock helper exited during cleanup: %v", waitErr)
			}
		}
	})

	time.Sleep(100 * time.Millisecond)
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		unlock()
		t.Fatalf("helper acquired publisher lock before release, stat error = %v", statErr)
	}
	unlock()
	if err = command.Wait(); err != nil {
		t.Fatalf("publisher lock helper failed: %v", err)
	}
	if _, err = os.Stat(marker); err != nil {
		t.Fatalf("helper did not acquire publisher lock after release: %v", err)
	}
}

func TestPublishReferenceLockHelper(t *testing.T) {
	if os.Getenv(publishLockHelperEnv) != "1" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	unlock, err := lockPublishReference(ctx, "registry.example.test/project/package:v2.0.0")
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	marker := os.Getenv(publishLockMarkerEnv)
	//nolint:gosec // The parent test supplies an isolated absolute path under t.TempDir.
	if err = os.WriteFile(marker, []byte("acquired"), 0o600); err != nil {
		t.Fatal(err)
	}
}
