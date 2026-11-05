/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package registry

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
)

func TestRegistryExportDirectoryAndCredentials(t *testing.T) {
	for _, cmd := range []*cobra.Command{
		NewCmdRegistryExport(NewRegistryOptions(options.IOStreams{})),
		NewCmdRegistrySync(NewRegistryOptions(options.IOStreams{})),
	} {
		require.NotNil(t, cmd.Flags().Lookup("source-password"))
		require.Nil(t, cmd.Flags().Lookup("source-password-file"))
	}
	cmd := NewCmdRegistryExport(NewRegistryOptions(options.IOStreams{}))
	output, err := cmd.Flags().GetString("output")
	require.NoError(t, err)
	require.Equal(t, "./offline", output)
}

func TestSourceRegistryInlinePassword(t *testing.T) {
	const username, password = "source-user", "literal/token with spaces"
	manifest, err := empty.Image.RawManifest()
	require.NoError(t, err)
	mediaType, err := empty.Image.MediaType()
	require.NoError(t, err)
	var authenticated atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != username || pass != password {
			w.Header().Set("WWW-Authenticate", `Basic realm="source-registry"`)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		authenticated.Store(true)
		if r.URL.Path == "/v2/" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", string(mediaType))
		if _, writeErr := w.Write(manifest); writeErr != nil {
			t.Error(writeErr)
		}
	}))
	defer server.Close()
	opts, err := sourceRegistryOptions(t.Context(), username, password)
	require.NoError(t, err)
	_, err = crane.Get(strings.TrimPrefix(server.URL, "http://")+"/image:latest", opts...)
	require.NoError(t, err)
	require.True(t, authenticated.Load())
}

func TestSourceRegistryCredentialsMustBePaired(t *testing.T) {
	_, err := sourceRegistryOptions(context.Background(), "user", "")
	require.Error(t, err)
	_, err = sourceRegistryOptions(context.Background(), "", "password")
	require.Error(t, err)
	_, err = sourceRegistryOptions(context.Background(), "", "")
	require.NoError(t, err)
}
