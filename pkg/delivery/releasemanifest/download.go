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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const defaultReleaseBaseURL = "https://github.com/kubeclipper/kubeclipper/releases/download"

const maxErrorResponseSize = 64 << 10

type Downloader struct {
	Client  *http.Client
	BaseURL string
}

func (d Downloader) Download(ctx context.Context, version string) ([]byte, error) {
	if !IsStableVersion(version) {
		return nil, fmt.Errorf("kcctl version %q is not a stable vX.Y.Z release; use --manifest", version)
	}
	client := d.Client
	if client == nil {
		client = http.DefaultClient
	}
	baseURL := strings.TrimRight(d.BaseURL, "/")
	if baseURL == "" {
		baseURL = defaultReleaseBaseURL
	}
	filename := "release-manifest-" + version + ".yaml"
	manifestURL := baseURL + "/" + version + "/" + filename
	data, err := download(ctx, client, manifestURL)
	if err != nil {
		return nil, err
	}
	checksum, err := download(ctx, client, manifestURL+".sha256")
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(checksum))
	if len(fields) == 0 || len(fields[0]) != sha256.Size*2 {
		return nil, fmt.Errorf("invalid release manifest checksum file")
	}
	expected, err := hex.DecodeString(fields[0])
	if err != nil {
		return nil, fmt.Errorf("invalid release manifest checksum: %w", err)
	}
	actual := sha256.Sum256(data)
	if !equalBytes(expected, actual[:]) {
		return nil, fmt.Errorf("release manifest checksum mismatch: expected=%s actual=%x", fields[0], actual)
	}
	return data, nil
}

func download(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create release manifest request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if _, copyErr := io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorResponseSize)); copyErr != nil {
			return nil, fmt.Errorf("drain error response from %s: %w", url, copyErr)
		}
		return nil, fmt.Errorf("download %s: HTTP %s", url, resp.Status)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, (16<<20)+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", url, err)
	}
	if len(data) > 16<<20 {
		return nil, fmt.Errorf("download %s: response exceeds 16 MiB", url)
	}
	return data, nil
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var different byte
	for i := range left {
		different |= left[i] ^ right[i]
	}
	return different == 0
}
