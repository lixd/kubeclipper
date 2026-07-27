package publisher

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	PublishLockDirEnv     = "KUBECLIPPER_PUBLISH_LOCK_DIR"
	publishLockDirMode    = 0o700
	publishLockFileMode   = 0o600
	publishLockRetryDelay = 50 * time.Millisecond
)

var publishReferenceLocks = struct {
	sync.Mutex
	refs map[string]*sync.Mutex
}{refs: make(map[string]*sync.Mutex)}

// lockPublishReference serializes read-modify-write tag updates both within a
// publisher process and between cooperating publisher processes on one host.
// GitHub Actions provides the corresponding cross-run serialization.
func lockPublishReference(ctx context.Context, reference string) (func(), error) {
	publishReferenceLocks.Lock()
	mutex := publishReferenceLocks.refs[reference]
	if mutex == nil {
		mutex = &sync.Mutex{}
		publishReferenceLocks.refs[reference] = mutex
	}
	publishReferenceLocks.Unlock()
	mutex.Lock()

	unlockWithError := func(err error) (func(), error) {
		mutex.Unlock()
		return nil, err
	}
	lockDir, err := publishLockDir()
	if err != nil {
		return unlockWithError(err)
	}
	if err = os.MkdirAll(lockDir, publishLockDirMode); err != nil {
		return unlockWithError(fmt.Errorf("create publisher lock directory: %w", err))
	}
	info, err := os.Lstat(lockDir)
	if err != nil {
		return unlockWithError(fmt.Errorf("inspect publisher lock directory: %w", err))
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return unlockWithError(fmt.Errorf("publisher lock path %q must be a directory and not a symlink", lockDir))
	}
	if err = os.Chmod(lockDir, publishLockDirMode); err != nil {
		return unlockWithError(fmt.Errorf("protect publisher lock directory: %w", err))
	}

	lockName := fmt.Sprintf("%x.lock", sha256.Sum256([]byte(reference)))
	file, err := os.OpenFile(filepath.Join(lockDir, lockName), os.O_CREATE|os.O_RDWR, publishLockFileMode)
	if err != nil {
		return unlockWithError(fmt.Errorf("open publisher lock: %w", err))
	}
	for {
		err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = file.Close()
			return unlockWithError(fmt.Errorf("lock publisher reference: %w", err))
		}
		timer := time.NewTimer(publishLockRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			_ = file.Close()
			return unlockWithError(ctx.Err())
		case <-timer.C:
		}
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			if err := unix.Flock(int(file.Fd()), unix.LOCK_UN); err != nil {
				_ = file.Close()
				mutex.Unlock()
				return
			}
			_ = file.Close()
			mutex.Unlock()
		})
	}, nil
}

func publishLockDir() (string, error) {
	if configured := os.Getenv(PublishLockDirEnv); configured != "" {
		if !filepath.IsAbs(configured) {
			return "", fmt.Errorf("publisher lock directory %q must be absolute", configured)
		}
		return filepath.Clean(configured), nil
	}
	cacheDir, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("resolve publisher lock directory: %w", err)
	}
	return filepath.Join(cacheDir, "kubeclipper", "publisher-locks"), nil
}
