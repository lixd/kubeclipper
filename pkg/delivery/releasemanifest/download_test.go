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
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
