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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/containerd/content"

	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
)

func TestContentTransferFaults(t *testing.T) {
	for _, protocol := range []string{"HTTP1", "HTTP2"} {
		for _, mode := range []string{"resume", "ignored", "bad-range", "bad-digest", "unauthorized", "range-416", "exhaust"} {
			t.Run(protocol+"/"+mode, func(t *testing.T) {
				t.Setenv(downloader.CacheDirEnv, t.TempDir())
				payload := bytes.Repeat([]byte("verified-content-"), 32768)
				desc := ocispec.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))}
				var attempts atomic.Int32
				var resumed atomic.Bool
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					n := attempts.Add(1)
					offset := int64(0)
					if v := r.Header.Get("Range"); v != "" {
						if _, err := fmt.Sscanf(v, "bytes=%d-", &offset); err != nil {
							t.Error(err)
						}
						resumed.Store(true)
					}
					if mode == "unauthorized" {
						w.WriteHeader(401)
						return
					}
					if n == 1 || mode == "exhaust" {
						if offset > 0 {
							w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, desc.Size-1, desc.Size))
							w.WriteHeader(206)
						}
						_, _ = w.Write(payload[offset : offset+(desc.Size-offset)/2])
						w.(http.Flusher).Flush()
						panic(http.ErrAbortHandler)
					}
					if mode == "range-416" {
						w.WriteHeader(416)
						return
					}
					if mode == "ignored" {
						offset = 0
					} else {
						start := offset
						if mode == "bad-range" {
							start++
						}
						w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, desc.Size-1, desc.Size))
						w.WriteHeader(206)
					}
					data := append([]byte(nil), payload[offset:]...)
					if mode == "bad-digest" {
						data[0] ^= 1
					}
					_, _ = w.Write(data)
				}))
				server.EnableHTTP2 = protocol == "HTTP2"
				if server.EnableHTTP2 {
					server.StartTLS()
				} else {
					server.Start()
				}
				defer server.Close()
				client := server.Client()
				store, err := openContent()
				if err != nil {
					t.Fatal(err)
				}
				open := func(offset int64) (*http.Response, error) {
					r, _ := http.NewRequestWithContext(context.Background(), "GET", server.URL, nil)
					if offset > 0 {
						r.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
					}
					return client.Do(r)
				}
				err = store.fetch(context.Background(), desc, server.URL, open)
				success := mode == "resume" || mode == "ignored"
				if success {
					if err != nil {
						t.Fatal(err)
					}
					data, e := os.ReadFile(store.path(desc.Digest))
					if e != nil || !bytes.Equal(data, payload) {
						t.Fatal("wrong committed bytes", e)
					}
					if attempts.Load() != 2 || !resumed.Load() {
						t.Fatalf("attempts=%d resumed=%v", attempts.Load(), resumed.Load())
					}
				} else {
					if err == nil {
						t.Fatal("fault accepted")
					}
					if _, e := os.Stat(store.path(desc.Digest)); !os.IsNotExist(e) {
						t.Fatal("failed blob published")
					}
					if mode == "exhaust" && attempts.Load() != 5 {
						t.Fatalf("attempt budget %d", attempts.Load())
					}
					if mode == "unauthorized" && attempts.Load() != 1 {
						t.Fatal("auth retried")
					}
				}
			})
		}
	}
}

func TestContentResumeAcrossCallsAndCancel(t *testing.T) {
	t.Setenv(downloader.CacheDirEnv, t.TempDir())
	payload := bytes.Repeat([]byte("resume"), 65536)
	desc := ocispec.Descriptor{Digest: digest.FromBytes(payload), Size: int64(len(payload))}
	ctx, cancel := context.WithCancel(context.Background())
	var first atomic.Bool
	first.Store(true)
	var offsetSeen atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if first.Swap(false) {
			_, _ = w.Write(payload[:len(payload)/2])
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		var offset int64
		_, _ = fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &offset)
		offsetSeen.Store(offset)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, desc.Size-1, desc.Size))
		w.WriteHeader(206)
		_, _ = w.Write(payload[offset:])
	}))
	defer server.Close()
	store, err := openContent()
	if err != nil {
		t.Fatal(err)
	}
	initialCtx := ctx
	open := func(ctx context.Context) func(int64) (*http.Response, error) {
		return func(offset int64) (*http.Response, error) {
			r, _ := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
			if offset > 0 {
				r.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
			}
			resp, err := server.Client().Do(r)
			if err == nil && ctx == initialCtx {
				resp.Body = &cancelReadCloser{ReadCloser: resp.Body, cancel: cancel}
			}
			return resp, err
		}
	}
	if err = store.fetch(ctx, desc, server.URL, open(ctx)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	// A fresh store instance must rediscover the interrupted transaction.
	store, err = openContent()
	if err != nil {
		t.Fatal(err)
	}
	if err = store.fetch(context.Background(), desc, server.URL, open(context.Background())); err != nil {
		t.Fatal(err)
	}
	if offsetSeen.Load() <= 0 {
		t.Fatal("resume did not retain progress")
	}
}

func TestContentPrivateStoreAndWarmVerification(t *testing.T) {
	t.Setenv(downloader.CacheDirEnv, t.TempDir())
	store, err := openContent()
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("manifest")
	d := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
	if err = store.put(context.Background(), d, data); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{store.root, filepath.Join(store.root, "ingest")} {
		info, e := os.Stat(p)
		if e != nil || info.Mode().Perm()&0077 != 0 {
			t.Fatalf("private directory %s: %v", p, e)
		}
	}
	if ok, e := store.verified(context.Background(), d); e != nil || !ok {
		t.Fatal(ok, e)
	}
	if err = os.Chmod(store.path(d.Digest), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(store.path(d.Digest), []byte("corrupt!"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, e := store.verified(context.Background(), d); e == nil {
		t.Fatal("corruption trusted")
	}
}

func TestCopyContextLimitsAndCancellation(t *testing.T) {
	if _, e := copyContext(context.Background(), io.Discard, strings.NewReader("too long"), 2); e == nil {
		t.Fatal("limit ignored")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if _, e := copyContext(ctx, io.Discard, strings.NewReader("data"), 4); !errors.Is(e, context.Canceled) || time.Since(start) > time.Second {
		t.Fatal("cancel ignored", e)
	}
}

type cancelReadCloser struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r *cancelReadCloser) Read(p []byte) (int, error) {
	n, e := r.ReadCloser.Read(p)
	if n > 0 {
		r.cancel()
	}
	return n, e
}

func TestContentCompletedAndOversizedIngest(t *testing.T) {
	for _, extra := range []bool{false, true} {
		t.Run(fmt.Sprint(extra), func(t *testing.T) {
			t.Setenv(downloader.CacheDirEnv, t.TempDir())
			s, err := openContent()
			if err != nil {
				t.Fatal(err)
			}
			data := []byte("completed layer")
			d := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
			source := "source"
			w, err := s.store.Writer(context.Background(), content.WithRef(s.transactionRef(d, source)), content.WithDescriptor(d))
			if err != nil {
				t.Fatal(err)
			}
			if extra {
				data = append(data, 'x')
			}
			if _, err = w.Write(data); err != nil {
				t.Fatal(err)
			}
			if err = w.Close(); err != nil {
				t.Fatal(err)
			}
			err = s.fetch(context.Background(), d, source, func(int64) (*http.Response, error) {
				t.Fatal("complete/oversize ingest should not access network")
				return nil, nil
			})
			if extra {
				if err == nil {
					t.Fatal("oversize ingest accepted")
				}
			} else if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContentIngestSurvivesProcessExit(t *testing.T) {
	data := bytes.Repeat([]byte("crash-recovery"), 4096)
	d := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
	if os.Getenv("KC_INGEST_CHILD") == "1" {
		s, err := openContent()
		if err != nil {
			os.Exit(2)
		}
		if os.Getenv("KC_INGEST_MODE") == "metadata-only" {
			ref := s.transactionRef(d, "crash-source")
			stage := filepath.Join(s.root, "ingest", digest.FromString(ref).Encoded())
			if err = os.MkdirAll(stage, 0700); err != nil {
				os.Exit(5)
			}
			if err = os.WriteFile(filepath.Join(stage, "ref"), []byte(ref), 0600); err != nil {
				os.Exit(6)
			}
			os.Exit(0)
		}
		w, err := s.store.Writer(context.Background(), content.WithRef(s.transactionRef(d, "crash-source")), content.WithDescriptor(d))
		if err != nil {
			os.Exit(3)
		}
		n := len(data) / 2
		if os.Getenv("KC_INGEST_MODE") == "complete" {
			n = len(data)
		}
		if _, err = w.Write(data[:n]); err != nil {
			os.Exit(4)
		}
		os.Exit(0) // Deliberately skip Close and Commit.
	}
	for _, mode := range []string{"partial", "complete", "metadata-only"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(downloader.CacheDirEnv, t.TempDir())
			cmd := exec.Command(os.Args[0], "-test.run=^TestContentIngestSurvivesProcessExit$")
			cmd.Env = append(os.Environ(), "KC_INGEST_CHILD=1", "KC_INGEST_MODE="+mode)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child: %v %s", err, out)
			}
			s, err := openContent()
			if err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(s.path(d.Digest)); !os.IsNotExist(err) {
				t.Fatal("uncommitted content published")
			}
			err = s.fetch(context.Background(), d, "crash-source", func(offset int64) (*http.Response, error) {
				if mode == "complete" {
					t.Fatal("complete ingest downloaded again")
				}
				want := int64(len(data) / 2)
				if mode == "metadata-only" {
					want = 0
				}
				if offset != want {
					t.Fatalf("crash offset %d; want %d", offset, want)
				}
				return &http.Response{StatusCode: 206, ContentLength: d.Size - offset, Header: http.Header{"Content-Range": []string{fmt.Sprintf("bytes %d-%d/%d", offset, d.Size-1, d.Size)}}, Body: io.NopCloser(bytes.NewReader(data[offset:]))}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestContentInvalidRecoveryMetadata(t *testing.T) {
	t.Setenv(downloader.CacheDirEnv, t.TempDir())
	s, err := openContent()
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("metadata")
	d := ocispec.Descriptor{Digest: digest.FromBytes(data), Size: int64(len(data))}
	ref := s.transactionRef(d, "source")
	w, err := s.store.Writer(context.Background(), content.WithRef(ref), content.WithDescriptor(d))
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(s.root, "ingest", digest.FromString(ref).Encoded())
	if err = os.WriteFile(filepath.Join(stage, "total"), []byte("1"), 0600); err != nil {
		t.Fatal(err)
	}
	err = s.fetch(context.Background(), d, "source", func(int64) (*http.Response, error) { t.Fatal("bad metadata reached network"); return nil, nil })
	if err == nil {
		t.Fatal("invalid metadata accepted")
	}
}

func TestCopyContextReportsFilesystemWriteFailure(t *testing.T) {
	f, err := os.OpenFile("/dev/full", os.O_WRONLY, 0)
	if err != nil {
		t.Skip("requires /dev/full")
	}
	defer f.Close()
	if _, err = copyContext(context.Background(), f, strings.NewReader("data"), 100); err == nil {
		t.Fatal("write failure swallowed")
	}
}
