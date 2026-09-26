/*
 *
 *  * Copyright 2021 KubeClipper Authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package sms

import (
	"io"
	"net/url"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	apimachineryErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"

	"github.com/kubeclipper/kubeclipper/pkg/authentication/mfa"
	"github.com/kubeclipper/kubeclipper/pkg/authentication/oauth"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/cache"
)

func TestFakeSMSProvider(t *testing.T) {
	kv, err := cache.NewMemory()
	require.NoError(t, err)
	err = mfa.SetupWithOptions(kv, &mfa.Options{
		Enabled: true,
		MFAProviders: []mfa.ProviderOptions{
			{Type: FakeSMSProvider, Options: oauth.DynamicOptions{"ttl": "5m"}},
		},
	})
	require.NoError(t, err)

	sms, err := mfa.GetProvider(FakeSMSProvider)
	require.NoError(t, err)

	// intercept stdout
	tempStdout, err := os.CreateTemp("", "")
	require.NoError(t, err)
	defer func() {
		_ = tempStdout.Close()
		_ = os.Remove(tempStdout.Name())
	}()
	stdout := os.Stdout
	os.Stdout = tempStdout
	defer func() {
		os.Stdout = stdout
	}()

	userInfo := &user.DefaultInfo{
		Name:   "",
		UID:    "",
		Groups: nil,
		Extra: map[string][]string{
			"phone": {"13888888888"},
		},
	}

	err = sms.Request(userInfo)
	require.NoError(t, err)

	req := make(url.Values)
	req.Set("code", "xxxxxx")

	err = sms.Verify(req, userInfo)
	require.Error(t, err)

	code := findCodeInFile(tempStdout)
	require.NotEmpty(t, code)
	req.Set("code", code)

	err = sms.Verify(req, userInfo)
	require.NoError(t, err)
}

// lastCodeInFile returns the most recently printed verification code, which is
// the one the provider just stored.
func lastCodeInFile(f *os.File) string {
	_, _ = f.Seek(0, io.SeekStart)
	buf, _ := io.ReadAll(f)
	reg := regexp.MustCompile(`code:(\d+) `)
	all := reg.FindAllSubmatch(buf, -1)
	if len(all) == 0 {
		return ""
	}
	return string(all[len(all)-1][1])
}

func findCodeInFile(f *os.File) string {
	_, _ = f.Seek(0, io.SeekStart)
	buf, _ := io.ReadAll(f)
	reg := regexp.MustCompile(`code:(\d+) `)
	res := reg.FindSubmatch(buf)
	if len(res) == 0 {
		return ""
	}
	return string(res[1])
}

// createOnlyCache mirrors the etcd-backed cache the server actually uses:
// Set is a create that fails on an existing key and nothing expires on its
// own (pruning is a separate controller), while Remove reports not-found for
// missing keys. The fake provider must work against these semantics — it did
// not, and every send answered 500 with AlreadyExists on the code key (R24).
type createOnlyCache struct {
	data map[string]string
}

func newCreateOnlyCache() *createOnlyCache { return &createOnlyCache{data: map[string]string{}} }

func (c *createOnlyCache) Set(key, value string, _ time.Duration) error {
	if _, ok := c.data[key]; ok {
		return apimachineryErrors.NewAlreadyExists(schema.GroupResource{Resource: "tokens"}, key)
	}
	c.data[key] = value
	return nil
}

func (c *createOnlyCache) Update(key, newValue string) error {
	if _, ok := c.data[key]; !ok {
		return cache.ErrNotExists
	}
	c.data[key] = newValue
	return nil
}

func (c *createOnlyCache) Get(key string) (string, error) {
	if v, ok := c.data[key]; ok {
		return v, nil
	}
	return "", cache.ErrNotExists
}

func (c *createOnlyCache) Exist(key string) (bool, error) {
	_, ok := c.data[key]
	return ok, nil
}

func (c *createOnlyCache) Remove(key string) error {
	if _, ok := c.data[key]; !ok {
		return apimachineryErrors.NewNotFound(schema.GroupResource{Resource: "tokens"}, key)
	}
	delete(c.data, key)
	return nil
}

func (c *createOnlyCache) Expire(string, time.Duration) error { return nil }

func TestFakeSMSProviderAgainstCreateOnlyCache(t *testing.T) {
	kv := newCreateOnlyCache()
	provider := &fakeSMSProvider{ttl: time.Minute, cache: kv}
	userInfo := &user.DefaultInfo{
		Extra: map[string][]string{"phone": {"13900000000"}},
	}

	codeKey := smsCacheKey(FakeSMSProvider, "13900000000")

	// capture stdout for the printed code
	tempStdout, err := os.CreateTemp("", "")
	require.NoError(t, err)
	defer func() { _ = tempStdout.Close(); _ = os.Remove(tempStdout.Name()) }()
	stdout := os.Stdout
	os.Stdout = tempStdout
	defer func() { os.Stdout = stdout }()

	require.NoError(t, provider.Request(userInfo), "first send must store the code")
	require.NotEmpty(t, kv.data[codeKey])

	// a second send inside the interval is rate limited
	require.ErrorIs(t, provider.Request(userInfo), ErrSMSRateLimitExceeded)

	// the next window: the marker is gone but the previous code was never
	// pruned. The send must still succeed instead of failing on AlreadyExists.
	delete(kv.data, smsRateLimitKey(FakeSMSProvider, "13900000000"))
	require.NoError(t, provider.Request(userInfo), "send after the window must replace the stale code")

	// the freshly stored code is the one printed last
	require.Equal(t, lastCodeInFile(tempStdout), kv.data[codeKey])
}
