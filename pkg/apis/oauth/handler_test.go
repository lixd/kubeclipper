/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package oauth

import (
	"testing"
	"time"

	"github.com/kubeclipper/kubeclipper/pkg/authentication/auth"
	"github.com/kubeclipper/kubeclipper/pkg/authentication/options"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/cache"
)

// captureCache records the TTL passed to Set so the fallback duration of the
// failure counter can be asserted without waiting for real expiry.
type captureCache struct {
	cache.Interface
	data     map[string]string
	lastTTL  time.Duration
	setCalls int
}

func newCaptureCache() *captureCache {
	return &captureCache{data: map[string]string{}}
}

func (c *captureCache) Get(key string) (string, error) {
	if v, ok := c.data[key]; ok {
		return v, nil
	}
	return "", cache.ErrNotExists
}

func (c *captureCache) Exist(key string) (bool, error) {
	_, ok := c.data[key]
	return ok, nil
}

func (c *captureCache) Set(key, value string, expire time.Duration) error {
	c.data[key] = value
	c.lastTTL = expire
	c.setCalls++
	return nil
}

func (c *captureCache) Update(key, value string) error {
	if _, ok := c.data[key]; !ok {
		return cache.ErrNotExists
	}
	c.data[key] = value
	return nil
}

func (c *captureCache) Remove(key string) error {
	delete(c.data, key)
	return nil
}

func newLimiterHandler(t *testing.T, maxTries int, duration time.Duration, c cache.Interface) *handler {
	t.Helper()
	return &handler{
		cache: c,
		authOptions: &options.AuthenticationOptions{
			AuthenticateRateLimiterMaxTries: maxTries,
			AuthenticateRateLimiterDuration: duration,
		},
	}
}

// A non-positive threshold disables the limiter: with MaxTries=0 the counter
// check `count >= 0` is true as soon as the first failure is recorded, so the
// account is rejected forever — including with the correct password — while
// the counter token (TTL 0 = no expiration) never goes away. R24.
func TestRateLimiterDisabledWhenMaxTriesNonPositive(t *testing.T) {
	c := newCaptureCache()
	c.data["auth-rate-limit-admin"] = "7" // leftover counter from a bad config
	h := newLimiterHandler(t, 0, 0, c)

	if err := h.rateLimiterChecker("admin"); err != nil {
		t.Fatalf("checker with MaxTries=0 = %v, want nil (limiter disabled)", err)
	}
	if err := h.rateLimiterCounter("admin"); err != nil {
		t.Fatalf("counter with MaxTries=0 = %v, want nil (no counting)", err)
	}
	if c.setCalls != 0 {
		t.Fatalf("counter wrote %d keys with the limiter disabled, want 0", c.setCalls)
	}
}

// A configured threshold still counts failures and blocks at the limit.
func TestRateLimiterBlocksAtThreshold(t *testing.T) {
	c := newCaptureCache()
	h := newLimiterHandler(t, 3, 2*time.Minute, c)

	for i := 0; i < 3; i++ {
		err := h.rateLimiterCounter("u1")
		if i < 2 && err != nil {
			t.Fatalf("attempt %d = %v, want nil below the threshold", i+1, err)
		}
		if i == 2 && err != auth.ErrRateLimitExceeded {
			t.Fatalf("attempt %d = %v, want ErrRateLimitExceeded", i+1, err)
		}
	}
	if err := h.rateLimiterChecker("u1"); err != auth.ErrRateLimitExceeded {
		t.Fatalf("checker at the threshold = %v, want ErrRateLimitExceeded", err)
	}
	if c.lastTTL != 2*time.Minute {
		t.Fatalf("counter TTL = %s, want the configured 2m", c.lastTTL)
	}
	if err := h.rateLimiterFinalizer("u1"); err != nil {
		t.Fatalf("finalizer = %v", err)
	}
	if err := h.rateLimiterChecker("u1"); err != nil {
		t.Fatalf("checker after a successful login = %v, want nil", err)
	}
}

// A zero or negative duration must not produce a counter that never expires:
// the cache stores TTL 0 as "no expiration", which pins the account beyond
// its window. Fall back to the built-in default instead.
func TestRateLimiterFallsBackWhenDurationNonPositive(t *testing.T) {
	c := newCaptureCache()
	h := newLimiterHandler(t, 5, 0, c)

	if err := h.rateLimiterCounter("u2"); err != nil {
		t.Fatalf("counter = %v", err)
	}
	if c.lastTTL != defaultRateLimiterDuration {
		t.Fatalf("counter TTL = %s, want fallback %s", c.lastTTL, defaultRateLimiterDuration)
	}
	if got := h.rateLimitTTL(); got != defaultRateLimiterDuration {
		t.Fatalf("rateLimitTTL() = %s, want %s (also used in the 429 message)", got, defaultRateLimiterDuration)
	}
}
