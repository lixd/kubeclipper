/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package releasemanifest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDownloaderVerifiesChecksum(t *testing.T) {
	data := []byte(validManifestYAML())
	digest := sha256.Sum256(data)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".sha256") {
			_, _ = fmt.Fprintf(w, "%x  release-manifest-v2.0.0.yaml\n", digest)
			return
		}
		if _, err := w.Write(data); err != nil {
			t.Errorf("write manifest response: %v", err)
		}
	}))
	defer server.Close()

	got, err := (Downloader{Client: server.Client(), BaseURL: server.URL}).Download(t.Context(), "v2.0.0")
	if err != nil {
		t.Fatalf("Download() error = %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("Download() returned unexpected data")
	}
}

func TestDownloaderUsesZoneDownloadOverride(t *testing.T) {
	data := []byte(validManifestYAML())
	digest := sha256.Sum256(data)
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if strings.HasSuffix(r.URL.Path, ".sha256") {
			_, _ = fmt.Fprintf(w, "%x  release-manifest-v2.0.0.yaml\n", digest)
			return
		}
		if _, err := w.Write(data); err != nil {
			t.Errorf("write manifest response: %v", err)
		}
	}))
	defer server.Close()
	t.Setenv("KCZONE", "cn")
	t.Setenv("KC_DOWNLOAD_URL", server.URL+"/kc")
	got, err := (Downloader{}).Download(t.Context(), "v2.0.0")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Download() = %q, %v", got, err)
	}
	if len(paths) != 2 || paths[0] != "/kc/v2.0.0/release-manifest-v2.0.0.yaml" {
		t.Fatalf("download paths = %v", paths)
	}
}

func TestDownloaderRejectsDevelopmentVersionAndChecksumMismatch(t *testing.T) {
	if _, err := (Downloader{}).Download(t.Context(), "v2.0.0-dirty"); err == nil || !strings.Contains(err.Error(), "use --manifest") {
		t.Fatalf("Download() error = %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".sha256") {
			_, _ = fmt.Fprintln(w, strings.Repeat("1", 64))
			return
		}
		if _, err := w.Write([]byte("manifest")); err != nil {
			t.Errorf("write manifest response: %v", err)
		}
	}))
	defer server.Close()
	if _, err := (Downloader{Client: server.Client(), BaseURL: server.URL}).Download(t.Context(), "v2.0.0"); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("Download() error = %v", err)
	}
}

func TestDownloaderRetriesTransientResponses(t *testing.T) {
	for _, failure := range []string{"manifest", "checksum", "truncated"} {
		t.Run(failure, func(t *testing.T) {
			data := []byte(validManifestYAML())
			digest := sha256.Sum256(data)
			var failures atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				checksum := strings.HasSuffix(r.URL.Path, ".sha256")
				if checksum == (failure == "checksum") && failures.Add(1) == 1 {
					if failure == "truncated" {
						w.Header().Set("Content-Length", fmt.Sprint(len(data)+1))
						if _, err := w.Write(data); err != nil {
							t.Errorf("write truncated response: %v", err)
						}
					} else if checksum {
						http.Error(w, "retry", http.StatusTooManyRequests)
					} else {
						http.Error(w, "retry", http.StatusServiceUnavailable)
					}
					return
				}
				if checksum {
					_, _ = fmt.Fprintf(w, "%x  release-manifest-v2.0.0.yaml\n", digest)
				} else {
					if _, err := w.Write(data); err != nil {
						t.Errorf("write manifest response: %v", err)
					}
				}
			}))
			defer server.Close()
			got, err := (Downloader{BaseURL: server.URL}).Download(t.Context(), "v2.0.0")
			require.NoError(t, err)
			require.Equal(t, data, got)
			require.EqualValues(t, 2, failures.Load())
		})
	}
}

func TestDownloaderLimitsRetries(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusUnauthorized, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				http.Error(w, "failure", status)
			}))
			defer server.Close()
			_, err := (Downloader{BaseURL: server.URL}).Download(t.Context(), "v2.0.0")
			require.Error(t, err)
			require.Contains(t, err.Error(), http.StatusText(status))
			want := 1
			if status == http.StatusServiceUnavailable {
				want += manifestDownloadRetries
			}
			require.EqualValues(t, want, requests.Load())
		})
	}
}

type manifestDeadlineTransport struct {
	t *testing.T
}

func (transport manifestDeadlineTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	deadline, ok := request.Context().Deadline()
	require.True(transport.t, ok)
	require.InDelta(transport.t, manifestDownloadTimeout.Seconds(), time.Until(deadline).Seconds(), 1)
	return http.DefaultTransport.RoundTrip(request)
}

func TestDownloaderSetsDefaultTimeoutWithoutChangingClient(t *testing.T) {
	data := []byte(validManifestYAML())
	digest := sha256.Sum256(data)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, ".sha256") {
			_, _ = fmt.Fprintf(w, "%x  release-manifest-v2.0.0.yaml\n", digest)
		} else {
			if _, err := w.Write(data); err != nil {
				t.Errorf("write manifest response: %v", err)
			}
		}
	}))
	defer server.Close()
	client := &http.Client{Transport: manifestDeadlineTransport{t: t}}
	_, err := (Downloader{Client: client, BaseURL: server.URL}).Download(t.Context(), "v2.0.0")
	require.NoError(t, err)
	require.Zero(t, client.Timeout)
}

func TestDownloaderHonorsCallerDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err := (Downloader{BaseURL: server.URL}).Download(ctx, "v2.0.0")
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
