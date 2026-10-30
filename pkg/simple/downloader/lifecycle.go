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

package downloader

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

const (
	CacheBudget      int64 = 20 << 30
	CacheReserve     int64 = 1 << 30
	PartialRetention       = 7 * 24 * time.Hour
)

// CacheLock uses stable lock inodes outside directories eligible for collection.
type CacheLock struct {
	file *os.File
	once sync.Once
	err  error
}

func LockCache(ctx context.Context, key string, exclusive, nonblock bool) (*CacheLock, error) {
	dir := filepath.Join(CacheDir(), "locks")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	sum := sha256.Sum256([]byte(key))
	f, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%x.lock", sum)), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	op := unix.LOCK_SH
	if exclusive {
		op = unix.LOCK_EX
	}
	for {
		if err = ctx.Err(); err != nil {
			f.Close()
			return nil, err
		}
		err = unix.Flock(int(f.Fd()), op|unix.LOCK_NB)
		if err == nil {
			return &CacheLock{file: f}, nil
		}
		if nonblock || !(errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN)) {
			f.Close()
			return nil, err
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			f.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (l *CacheLock) Release() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() { l.err = errors.Join(unix.Flock(int(l.file.Fd()), unix.LOCK_UN), l.file.Close()) })
	return l.err
}

func SyncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func CheckCacheSpace(additional int64) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(CacheDir(), &stat); err != nil {
		return err
	}
	available := uint64(stat.Bavail) * uint64(stat.Bsize)
	if additional < 0 || uint64(additional)+uint64(CacheReserve) > available {
		return fmt.Errorf("insufficient cache space: need %d bytes plus %d reserve; available %d", additional, CacheReserve, available)
	}
	return nil
}

func accessPath(object string) string {
	sum := sha256.Sum256([]byte(object))
	return filepath.Join(CacheDir(), "access", fmt.Sprintf("%x", sum))
}

// TouchCache stores usage outside immutable receipts. Call only while protected.
func TouchCache(object string) error {
	return AtomicWriteFile(accessPath(object), []byte(time.Now().UTC().Format(time.RFC3339Nano)), 0600)
}

type cacheObject struct {
	path    string
	size    int64
	used    time.Time
	partial bool
}

func cacheObjects(root string) ([]cacheObject, error) {
	var result []cacheObject
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if os.IsNotExist(err) {
			return nil
		}
		if err != nil {
			return err
		}
		ingest := d.IsDir() && filepath.Dir(p) == filepath.Join(root, "content", "ingest")
		if d.IsDir() && !ingest && !strings.HasPrefix(d.Name(), ".extract-") {
			return nil
		}
		// A receipt marks an entire immutable snapshot as one eviction unit.
		var object string
		partial := false
		switch {
		case d.IsDir() && strings.HasPrefix(d.Name(), ".extract-"):
			object = p
			partial = true
		case d.Name() == "receipt.json":
			object = filepath.Dir(p)
		case strings.Contains(p, string(filepath.Separator)+"blobs"+string(filepath.Separator)):
			object = p
		case ingest:
			object = p
			partial = true
		default:
			return nil
		}
		var size int64
		var modified time.Time
		e := filepath.WalkDir(object, func(q string, x fs.DirEntry, e error) error {
			if e != nil {
				return e
			}
			i, e := x.Info()
			if e != nil {
				return e
			}
			if !x.IsDir() {
				size += i.Size()
			}
			if !x.IsDir() && i.ModTime().After(modified) {
				modified = i.ModTime()
			}
			return nil
		})
		if e != nil {
			return e
		}
		if modified.IsZero() {
			i, e := os.Stat(object)
			if e != nil {
				return e
			}
			modified = i.ModTime()
		}
		if a, e := os.Stat(accessPath(object)); e == nil {
			modified = a.ModTime()
		}
		result = append(result, cacheObject{object, size, modified, partial})
		if d.IsDir() {
			return filepath.SkipDir
		}
		return nil
	})
	return result, err
}

// PruneCache never blocks a consumer. Budget and retention are soft policies.
func PruneCache(ctx context.Context, protected []string) error {
	return pruneCache(ctx, protected, CacheBudget, time.Now())
}

func pruneCache(ctx context.Context, protected []string, budget int64, now time.Time) error {
	lock, err := LockCache(ctx, "lifecycle", true, true)
	if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
		return nil
	}
	if err != nil {
		return err
	}
	defer lock.Release()
	// Staging and obsolete version-directory receipts are not reusable snapshots.
	packages := filepath.Join(CacheDir(), "packages")
	err = filepath.WalkDir(packages, func(p string, d fs.DirEntry, e error) error {
		if os.IsNotExist(e) {
			return nil
		}
		if e != nil {
			return e
		}
		if d.IsDir() && strings.HasPrefix(d.Name(), ".extract-") {
			i, e := d.Info()
			if e != nil {
				return e
			}
			if now.Sub(i.ModTime()) > PartialRetention {
				if e = os.RemoveAll(p); e != nil {
					return e
				}
			}
			return filepath.SkipDir
		}
		rel, _ := filepath.Rel(packages, p)
		if d.IsDir() && d.Name() == packageContents && len(strings.Split(rel, string(filepath.Separator))) == 5 {
			// Native Helm still owns its version-directory cache. All other
			// legacy contents are obsolete, including interrupted downloads.
			if _, err := os.Stat(filepath.Join(p, ChartFilename+".source.json")); err == nil {
				return filepath.SkipDir
			} else if !os.IsNotExist(err) {
				return err
			}
			if err := os.RemoveAll(p); err != nil {
				return err
			}
			return filepath.SkipDir
		}
		if !d.IsDir() && d.Name() == ManifestFilename && len(strings.Split(rel, string(filepath.Separator))) == 5 {
			return os.Remove(p)
		}
		return nil
	})
	if err != nil {
		return err
	}
	objects, err := cacheObjects(CacheDir())
	if err != nil {
		return err
	}
	var total int64
	for _, o := range objects {
		total += o.size
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].used.Before(objects[j].used) })
	for _, o := range objects {
		if err = ctx.Err(); err != nil {
			return err
		}
		keep := false
		for _, p := range protected {
			if o.path == p {
				keep = true
				break
			}
		}
		if keep {
			continue
		}
		if total <= budget && (!o.partial || now.Sub(o.used) <= PartialRetention) {
			continue
		}
		if err = os.RemoveAll(o.path); err != nil {
			return err
		}
		total -= o.size
		if err = os.Remove(accessPath(o.path)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
