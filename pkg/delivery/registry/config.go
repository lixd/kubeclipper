/*
 *
 *  * Copyright 2026 KubeClipper Authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package registry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/crane"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

const (
	SchemeHTTPS = "https"
	SchemeHTTP  = "http"

	ServerConfigPath = "/etc/kubeclipper-server/delivery/package-registry.json"
	AgentConfigPath  = "/etc/kubeclipper-agent/delivery/package-registry.json"

	ConfigPathEnv = "KC_PACKAGE_REGISTRY_CONFIG"

	PrivateFileMode os.FileMode = 0600
	PrivateDirMode  os.FileMode = 0700
)

type Config struct {
	Registry      string `json:"registry"`
	Scheme        string `json:"scheme"`
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	CA            string `json:"ca,omitempty"`
	SkipTLSVerify bool   `json:"skipTLSVerify,omitempty"`
}

func SplitPrefix(registry string) (host, repositoryPrefix string) {
	normalized := strings.Trim(strings.TrimSpace(registry), "/")
	host, repositoryPrefix, _ = strings.Cut(normalized, "/")
	return host, strings.Trim(repositoryPrefix, "/")
}

type FileOptions struct {
	Scheme        string
	Username      string
	PasswordFile  string
	CAFile        string
	SkipTLSVerify bool
}

func (o FileOptions) Specified() bool {
	return o.Scheme != "" || o.Username != "" || o.PasswordFile != "" || o.CAFile != "" || o.SkipTLSVerify
}

func (o FileOptions) Resolve(registry string) (*Config, error) {
	cfg := &Config{
		Registry:      strings.TrimRight(strings.TrimSpace(registry), "/"),
		Scheme:        strings.ToLower(strings.TrimSpace(o.Scheme)),
		Username:      strings.TrimSpace(o.Username),
		SkipTLSVerify: o.SkipTLSVerify,
	}
	if cfg.Scheme == "" {
		cfg.Scheme = SchemeHTTPS
	}
	var err error
	if o.PasswordFile != "" {
		cfg.Password, err = readSecretFile(o.PasswordFile, "package registry password")
		if err != nil {
			return nil, err
		}
	}
	if o.CAFile != "" {
		ca, readErr := os.ReadFile(o.CAFile)
		if readErr != nil {
			return nil, fmt.Errorf("read package registry CA file: %w", readErr)
		}
		cfg.CA = string(ca)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func readSecretFile(path, description string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s file: %w", description, err)
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return "", fmt.Errorf("%s file is empty", description)
	}
	return value, nil
}

func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("package registry config is required")
	}
	c.Registry = strings.TrimRight(strings.TrimSpace(c.Registry), "/")
	c.Scheme = strings.ToLower(strings.TrimSpace(c.Scheme))
	if c.Scheme == "" {
		c.Scheme = SchemeHTTPS
	}
	if c.Registry == "" {
		return fmt.Errorf("package registry is required")
	}
	if strings.Contains(c.Registry, "://") {
		return fmt.Errorf("package registry must not include a URL scheme")
	}
	if c.Scheme != SchemeHTTPS && c.Scheme != SchemeHTTP {
		return fmt.Errorf("package registry scheme must be https or http")
	}
	if (c.Username == "") != (c.Password == "") {
		return fmt.Errorf("package registry username and password must be specified together")
	}
	if err := c.validateTLS(); err != nil {
		return err
	}
	return nil
}

func (c *Config) validateTLS() error {
	if c.Scheme == SchemeHTTP {
		if c.CA != "" {
			return fmt.Errorf("package registry CA cannot be used with plain HTTP")
		}
		if c.SkipTLSVerify {
			return fmt.Errorf("package registry skip TLS verify cannot be used with plain HTTP")
		}
	}
	if c.CA != "" {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM([]byte(c.CA)) {
			return fmt.Errorf("package registry CA file contains no valid certificates")
		}
	}
	return nil
}

func (c *Config) CraneOptions(ctx context.Context) ([]crane.Option, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	opts := []crane.Option{crane.WithContext(ctx), crane.WithAuth(authn.Anonymous)}
	if c.Username != "" {
		opts = append(opts, crane.WithAuth(&authn.Basic{Username: c.Username, Password: c.Password}))
	}
	if c.Scheme == SchemeHTTP {
		return append(opts, crane.Insecure), nil
	}
	if c.CA == "" && !c.SkipTLSVerify {
		return opts, nil
	}
	base, ok := remote.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("unexpected registry HTTP transport type %T", remote.DefaultTransport)
	}
	transport := base.Clone()
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	} else {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
	}
	if c.CA != "" {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		pool.AppendCertsFromPEM([]byte(c.CA))
		transport.TLSClientConfig.RootCAs = pool
	}
	transport.TLSClientConfig.InsecureSkipVerify = c.SkipTLSVerify
	return append(opts, crane.WithTransport(transport)), nil
}

// ValidateRegistry prevents credentials from being sent to a registry other
// than the one they were configured for.
func (c *Config) ValidateRegistry(registry string) error {
	if err := c.Validate(); err != nil {
		return err
	}
	want := strings.TrimRight(strings.TrimSpace(registry), "/")
	if c.Registry != want {
		return fmt.Errorf("package registry config is for %s, not %s", c.Registry, want)
	}
	return nil
}

func (c *Config) ValidateReference(ref string) error {
	if err := c.Validate(); err != nil {
		return err
	}
	normalized := strings.TrimSpace(strings.TrimPrefix(ref, "oci://"))
	if before, _, ok := strings.Cut(normalized, "@"); ok {
		normalized = before
	}
	if normalized != c.Registry && !strings.HasPrefix(normalized, c.Registry+"/") {
		return fmt.Errorf("package registry config is for %s, not reference %s", c.Registry, ref)
	}
	return nil
}

func Write(path string, cfg *Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(cfg) // #nosec G117 -- this is the protected credential file payload.
	if err != nil {
		return fmt.Errorf("marshal package registry config: %w", err)
	}
	if err = os.MkdirAll(filepath.Dir(path), PrivateDirMode); err != nil {
		return fmt.Errorf("create package registry config directory: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".package-registry-*")
	if err != nil {
		return fmt.Errorf("create package registry config: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err = tmp.Chmod(PrivateFileMode); err == nil {
		_, err = tmp.Write(data)
	}
	closeErr := tmp.Close()
	if err != nil {
		return fmt.Errorf("write package registry config: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close package registry config: %w", closeErr)
	}
	if err = os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace package registry config: %w", err)
	}
	return nil
}

func Load(path string) (*Config, error) {
	info, err := os.Stat(path) // #nosec G703 -- callers select the local config path explicitly.
	if err != nil {
		return nil, fmt.Errorf("stat package registry config: %w", err)
	}
	if info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("package registry config %s must have mode 0600 or stricter", path)
	}
	data, err := os.ReadFile(path) // #nosec G703 -- callers select the local config path explicitly.
	if err != nil {
		return nil, fmt.Errorf("read package registry config: %w", err)
	}
	cfg := &Config{}
	if err = json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse package registry config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func Resolve(registry string) (*Config, error) {
	normalized := strings.TrimRight(strings.TrimSpace(registry), "/")
	return resolve(func(cfg *Config) bool { return cfg.Registry == normalized }, normalized)
}

func ResolveReference(ref string) (*Config, error) {
	normalized := strings.TrimSpace(ref)
	if before, _, ok := strings.Cut(normalized, "@"); ok {
		normalized = before
	}
	return resolve(func(cfg *Config) bool {
		return normalized == cfg.Registry || strings.HasPrefix(normalized, cfg.Registry+"/")
	}, strings.SplitN(normalized, "/", 2)[0])
}

func resolve(matches func(*Config) bool, fallbackRegistry string) (*Config, error) {
	if path := strings.TrimSpace(os.Getenv(ConfigPathEnv)); path != "" {
		cfg, err := Load(path)
		if err != nil {
			return nil, err
		}
		if !matches(cfg) {
			return nil, fmt.Errorf("package registry config %s is for %s, not %s", path, cfg.Registry, fallbackRegistry)
		}
		return cfg, nil
	}
	paths := []string{AgentConfigPath, ServerConfigPath}
	for _, path := range paths {
		cfg, err := Load(path)
		if err != nil {
			if os.IsNotExist(rootCause(err)) {
				continue
			}
			return nil, err
		}
		if matches(cfg) {
			return cfg, nil
		}
	}
	return (&FileOptions{}).Resolve(fallbackRegistry)
}

func rootCause(err error) error {
	for {
		unwrapped, ok := err.(interface{ Unwrap() error })
		if !ok || unwrapped.Unwrap() == nil {
			return err
		}
		err = unwrapped.Unwrap()
	}
}
