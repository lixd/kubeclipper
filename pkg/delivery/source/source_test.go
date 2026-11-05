/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package source

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegistrySelection(t *testing.T) {
	for _, test := range []struct {
		zone, configured, want string
	}{
		{"", "", GlobalPackageRegistry},
		{"global", "", GlobalPackageRegistry},
		{"cn", "", CNPackageRegistry},
		{"cn", "harbor.example.com/kc/", "harbor.example.com/kc"},
	} {
		t.Run(test.zone+test.configured, func(t *testing.T) {
			t.Setenv("KCZONE", test.zone)
			registry, err := Registry(test.configured)
			require.NoError(t, err)
			require.Equal(t, test.want, registry)
		})
	}
	t.Setenv("KCZONE", "")
	override, err := RegistryOverride("")
	require.NoError(t, err)
	require.Empty(t, override)
	_, err = Registry("https://harbor.example.com/kc")
	require.Error(t, err)
	_, err = Registry(" ")
	require.Error(t, err)
	t.Setenv("KCZONE", "unknown")
	_, err = Registry("")
	require.Error(t, err)
	require.Contains(t, err.Error(), "KCZONE")
}

func TestReleaseSourceSelection(t *testing.T) {
	for _, test := range []struct {
		zone, custom, explicit, want string
	}{
		{"", "", "", GlobalReleaseBaseURL},
		{"cn", "", "", CNReleaseBaseURL},
		{"cn", "https://custom.example.com/kc/", "", "https://custom.example.com/kc"},
		{"cn", "https://custom.example.com/kc", "https://explicit.example.com/kc/", "https://explicit.example.com/kc"},
	} {
		t.Run(test.zone+test.explicit+test.custom, func(t *testing.T) {
			t.Setenv("KCZONE", test.zone)
			t.Setenv("KC_DOWNLOAD_URL", test.custom)
			base, err := ReleaseBaseURL(test.explicit)
			require.NoError(t, err)
			require.Equal(t, test.want, base)
		})
	}
	t.Setenv("KCZONE", "cn")
	t.Setenv("KC_DOWNLOAD_URL", "file:///tmp/release")
	_, err := ReleaseBaseURL("")
	require.Error(t, err)
}
