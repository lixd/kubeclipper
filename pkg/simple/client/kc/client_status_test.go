/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package kc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/cli/config"
)

func TestFromConfigWithoutValidationRejectsIncompleteConfig(t *testing.T) {
	_, err := FromConfigWithoutValidation(config.Config{CurrentContext: "missing"})
	if err == nil || !strings.Contains(err.Error(), "current context") {
		t.Fatalf("expected current context error, got %v", err)
	}
}

func TestFromConfigDoesNotRequestUnrelatedPermissions(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/core.kubeclipper.io/v1/clusters" || r.Header.Get("Authorization") != "Bearer scoped-token" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()
	c := config.Config{
		CurrentContext: "scoped",
		Contexts:       map[string]*config.Context{"scoped": {Server: "server", AuthInfo: "user"}},
		Servers:        map[string]*config.Server{"server": {Server: server.URL}},
		AuthInfos:      map[string]*config.AuthInfo{"user": {Token: "scoped-token"}},
	}
	client, err := FromConfig(c)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 0 {
		t.Fatalf("client initialization made %d requests", requests)
	}
	if _, err := client.ListClusters(context.Background(), Queries{}); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("cluster request count = %d, want 1", requests)
	}
}

func TestPlatformStatusRejectsInvalidResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != platformStatusPath {
			t.Fatalf("request path = %q, want %q", request.URL.Path, platformStatusPath)
		}
		writer.Header().Set("Content-Type", "application/json")
		if _, err := writer.Write([]byte(`{}`)); err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClientWithOpts(WithEndpoint(server.URL), WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PlatformStatus(context.Background()); err == nil || !strings.Contains(err.Error(), "invalid platform status response") {
		t.Fatalf("expected invalid response error, got %v", err)
	}
}

func TestHealthz(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != healthzPath {
			t.Fatalf("request path = %q, want %q", request.URL.Path, healthzPath)
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client, err := NewClientWithOpts(WithEndpoint(server.URL), WithHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Healthz(context.Background()); err != nil {
		t.Fatalf("healthz: %v", err)
	}
}
