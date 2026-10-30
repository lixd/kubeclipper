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

package fetcher

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/content/local"
	"github.com/containerd/containerd/errdefs"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
	"github.com/kubeclipper/kubeclipper/pkg/logger"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
)

const maxManifestSize int64 = 4 << 20

type corruptCache struct{ path string }

func (e *corruptCache) Error() string { return "corrupt OCI cache: " + e.path }

type localContent struct {
	store content.Store
	root  string
}

func openContent() (*localContent, error) {
	root := filepath.Join(downloader.CacheDir(), "content")
	for _, p := range []string{root, filepath.Join(root, "ingest"), filepath.Join(root, "blobs")} {
		if err := os.MkdirAll(p, 0700); err != nil {
			return nil, err
		}
		if err := os.Chmod(p, 0700); err != nil {
			return nil, err
		}
	}
	s, err := local.NewStore(root)
	return &localContent{s, root}, err
}
func (c *localContent) path(d digest.Digest) string {
	return filepath.Join(c.root, "blobs", d.Algorithm().String(), d.Encoded())
}
func checkDescriptor(d ocispec.Descriptor) error {
	if err := d.Digest.Validate(); err != nil {
		return err
	}
	if d.Digest.Algorithm() != digest.SHA256 {
		return fmt.Errorf("unsupported digest algorithm")
	}
	if d.Size < 0 {
		return fmt.Errorf("negative descriptor size")
	}
	return nil
}
func (c *localContent) verified(ctx context.Context, d ocispec.Descriptor) (bool, error) {
	if err := checkDescriptor(d); err != nil {
		return false, err
	}
	f, err := os.Open(c.path(d.Digest))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return false, err
	}
	if !stat.Mode().IsRegular() || stat.Size() != d.Size {
		return false, &corruptCache{c.path(d.Digest)}
	}
	h := d.Digest.Algorithm().Hash()
	n, err := copyContext(ctx, h, f, d.Size)
	if err != nil {
		return false, err
	}
	if n != d.Size || digest.NewDigest(d.Digest.Algorithm(), h) != d.Digest {
		return false, &corruptCache{c.path(d.Digest)}
	}
	return true, nil
}
func copyContext(ctx context.Context, w io.Writer, r io.Reader, limit int64) (int64, error) {
	buf := make([]byte, 128<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		n, e := r.Read(buf)
		if n > 0 {
			total += int64(n)
			if total > limit {
				return total, fmt.Errorf("content exceeds limit %d", limit)
			}
			if _, err := w.Write(buf[:n]); err != nil {
				return total, err
			}
		}
		if e == io.EOF {
			return total, nil
		}
		if e != nil {
			return total, e
		}
	}
}
func (c *localContent) put(ctx context.Context, d ocispec.Descriptor, data []byte) error {
	lock, err := downloader.LockCache(ctx, "blob:"+d.Digest.String(), true, false)
	if err != nil {
		return err
	}
	defer lock.Release()
	ok, err := c.verified(ctx, d)
	if err != nil || ok {
		return err
	}
	if err := content.WriteBlob(ctx, c.store, c.root+":"+d.Digest.String(), bytes.NewReader(data), d); err != nil {
		return err
	}
	return downloader.SyncDirectory(filepath.Dir(c.path(d.Digest)))
}

func (f *OCIArtifactFetcher) blobClient(ctx context.Context, ref string) (*http.Client, string, error) {
	cfg := f.RegistryConfig
	if cfg != nil {
		value := *cfg
		cfg = &value
	}
	var err error
	if cfg == nil {
		cfg, err = deliveryregistry.ResolveReference(ref)
		if err != nil {
			return nil, "", err
		}
	}
	if err = cfg.ValidateReference(ref); err != nil {
		return nil, "", err
	}
	opts, err := cfg.CraneOptions(ctx)
	if err != nil {
		return nil, "", err
	}
	parsed := crane.GetOptions(opts...)
	r, err := name.ParseReference(ref, parsed.Name...)
	if err != nil {
		return nil, "", err
	}
	repo := r.Context()
	base, ok := parsed.Transport.(*http.Transport)
	if !ok {
		return nil, "", fmt.Errorf("unsupported registry transport %T", parsed.Transport)
	}
	raw := base.Clone()
	raw.DisableCompression = true
	if raw.TLSClientConfig != nil {
		raw.TLSClientConfig = raw.TLSClientConfig.Clone()
		raw.TLSClientConfig.InsecureSkipVerify = cfg.SkipTLSVerify
	}
	auth := authn.Anonymous
	if cfg.Username != "" {
		auth = &authn.Basic{Username: cfg.Username, Password: cfg.Password}
	}
	rt, err := transport.NewWithContext(ctx, repo.Registry, auth, raw, []string{repo.Scope(transport.PullScope)})
	if err != nil {
		return nil, "", safeNetworkError(err)
	}
	scheme := cfg.Scheme
	client := &http.Client{Transport: rt, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("too many registry redirects")
		}
		if req.URL.Scheme != "https" && req.URL.Scheme != "http" {
			return fmt.Errorf("invalid redirect scheme")
		}
		if len(via) > 0 && via[0].URL.Scheme == "https" && req.URL.Scheme != "https" {
			return fmt.Errorf("registry redirect downgrades HTTPS")
		}
		if len(via) > 0 && req.URL.Host != via[0].URL.Host {
			req.Header.Del("Authorization")
		}
		return nil
	}}
	return client, scheme + "://" + repo.RegistryStr() + "/v2/" + repo.RepositoryStr() + "/blobs/", nil
}

// Drop URL wrappers: redirect errors can contain expiring URLs and credentials.
func safeNetworkError(err error) error {
	var u *url.Error
	if errors.As(err, &u) {
		return safeNetworkError(u.Err)
	}
	var op *net.OpError
	if errors.As(err, &op) {
		return fmt.Errorf("network %s failed", op.Op)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if strings.Contains(err.Error(), "://") {
		return fmt.Errorf("registry transfer failed (%T)", err)
	}
	return fmt.Errorf("registry transfer failed: %w", err)
}

func retryable(err error) bool {
	var u *url.Error
	if errors.As(err, &u) {
		return retryable(u.Err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		return true
	}
	var n net.Error
	if errors.As(err, &n) {
		return true
	}
	// HTTP2 StreamError does not implement net.Error in all supported transports.
	return strings.Contains(err.Error(), "stream error:") || strings.Contains(err.Error(), "connection reset")
}

func (c *localContent) fetch(ctx context.Context, d ocispec.Descriptor, source string, open func(int64) (*http.Response, error)) error {
	lock, err := downloader.LockCache(ctx, "blob:"+d.Digest.String(), true, false)
	if err != nil {
		return err
	}
	defer lock.Release()
	ok, err := c.verified(ctx, d)
	if err != nil || ok {
		return err
	}
	ref := c.transactionRef(d, source)
	stage := filepath.Join(c.root, "ingest", digest.FromString(ref).Encoded())
	if _, e := os.Stat(stage); e == nil {
		// local.Writer creates metadata before opening data. A process can exit
		// in that window; there are no payload bytes to resume or discard.
		if _, dataErr := os.Stat(filepath.Join(stage, "data")); os.IsNotExist(dataErr) {
			logger.Warnf("discard incomplete OCI ingest metadata for layer %s: no data file", d.Digest)
			if err = c.store.Abort(ctx, ref); err != nil {
				return err
			}
		} else if dataErr != nil {
			return dataErr
		} else {
			status, se := c.store.Status(ctx, ref)
			if se != nil || status.Ref != ref || status.Total != d.Size {
				return fmt.Errorf("invalid OCI recovery transaction metadata")
			}
		}
	} else if !os.IsNotExist(e) {
		return e
	}
	w, err := c.store.Writer(ctx, content.WithRef(ref), content.WithDescriptor(d))
	if err != nil {
		return err
	}
	defer w.Close()
	commit := func() error {
		e := w.Commit(ctx, d.Size, d.Digest)
		if errdefs.IsAlreadyExists(e) {
			return nil
		}
		if e == nil {
			e = downloader.SyncDirectory(filepath.Dir(c.path(d.Digest)))
		}
		return e
	}
	for attempt := 0; attempt < 5; attempt++ {
		if canceled := ctx.Err(); canceled != nil {
			return canceled
		}
		if attempt > 0 {
			logger.Warnf("retry OCI layer %s transfer (%d/5): %v", d.Digest, attempt+1, err)
			wait := time.Second * time.Duration(1<<uint(attempt-1))
			if wait > 30*time.Second {
				wait = 30 * time.Second
			}
			wait = time.Duration(float64(wait) * (0.5 + rand.Float64()*0.5))
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		status, e := w.Status()
		if e != nil {
			return e
		}
		if status.Offset > d.Size {
			w.Close()
			_ = c.store.Abort(ctx, ref)
			return fmt.Errorf("partial OCI layer exceeds expected size")
		}
		if status.Offset == d.Size {
			e = commit()
			if e != nil {
				_ = c.store.Abort(ctx, ref)
			}
			return e
		}
		if e = downloader.CheckCacheSpace(d.Size - status.Offset); e != nil {
			return e
		}
		resp, e := open(status.Offset)
		if e != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !retryable(e) {
				return safeNetworkError(e)
			}
			err = safeNetworkError(e)
			continue
		}
		offset := status.Offset
		if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
			resp.Body.Close()
			return fmt.Errorf("registry rejected incomplete layer range")
		}
		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			resp.Body.Close()
			err = fmt.Errorf("registry returned HTTP %d", resp.StatusCode)
			continue
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			return fmt.Errorf("registry returned HTTP %d", resp.StatusCode)
		}
		if resp.Header.Get("Content-Encoding") != "" && resp.Header.Get("Content-Encoding") != "identity" {
			resp.Body.Close()
			return fmt.Errorf("unexpected HTTP content encoding")
		}
		if resp.StatusCode == http.StatusPartialContent {
			var first, last, total int64
			n, e := fmt.Sscanf(resp.Header.Get("Content-Range"), "bytes %d-%d/%d", &first, &last, &total)
			if e != nil || n != 3 || first != offset || last != d.Size-1 || total != d.Size || resp.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", first, last, total) {
				resp.Body.Close()
				return fmt.Errorf("invalid registry Content-Range")
			}
		} else if offset > 0 {
			logger.Warnf("registry ignored Range for layer %s; restarting transfer", d.Digest)
			if e = w.Truncate(0); e != nil {
				resp.Body.Close()
				return e
			}
			offset = 0
		}
		if resp.ContentLength >= 0 && resp.ContentLength != d.Size-offset {
			resp.Body.Close()
			return fmt.Errorf("registry layer length mismatch")
		}
		_, e = copyContext(ctx, w, resp.Body, d.Size-offset)
		resp.Body.Close()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if e == nil {
			status, e = w.Status()
			if e != nil {
				return e
			}
			if status.Offset == d.Size {
				e = commit()
				if e != nil {
					_ = c.store.Abort(ctx, ref)
				}
				return e
			}
			e = io.ErrUnexpectedEOF
		}
		if !retryable(e) {
			return safeNetworkError(e)
		}
		err = safeNetworkError(e)
	}
	return fmt.Errorf("OCI layer transfer exhausted five attempts: %w", err)
}

func (c *localContent) transactionRef(d ocispec.Descriptor, source string) string {
	sum := sha256.Sum256([]byte(c.root + "\x00" + source + "\x00" + d.Digest.String() + fmt.Sprint(d.Size)))
	return fmt.Sprintf("kc-%x", sum)
}
