/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package server

import (
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/authentication/mfa"
	"github.com/kubeclipper/kubeclipper/pkg/authentication/oauth"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/cache"
)

// The server binary must carry the provider packages: they register their
// factories in init(), and a deploy that enables mfaOptions otherwise fails
// at startup with "mfa provider <type> is not supported" and crash-loops
// kc-server (observed live in R24). The blank import in server.go keeps this
// test green; SetupWithOptions is the production path that turns the
// registered factory into a usable provider.
func TestMFAProviderFactoriesRegistered(t *testing.T) {
	c, err := cache.NewMemory()
	if err != nil {
		t.Fatalf("memory cache: %v", err)
	}
	opts := mfa.NewOptions()
	opts.Enabled = true
	opts.MFAProviders = []mfa.ProviderOptions{{
		Type:    "fake_sms",
		Options: oauth.DynamicOptions{"ttl": "60s"},
	}}
	if err := mfa.SetupWithOptions(c, opts); err != nil {
		t.Fatalf("SetupWithOptions(fake_sms) = %v, want the provider to be accepted", err)
	}
	if _, err := mfa.GetProvider("fake_sms"); err != nil {
		t.Fatalf("fake_sms provider is not registered: %v", err)
	}
}
