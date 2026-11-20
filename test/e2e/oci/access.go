/*
 * Copyright 2021 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"

	iamapi "github.com/kubeclipper/kubeclipper/pkg/apis/iam/v1"
	"github.com/kubeclipper/kubeclipper/pkg/authentication/oauth"
	"github.com/kubeclipper/kubeclipper/pkg/cli/config"
	apierror "github.com/kubeclipper/kubeclipper/pkg/errors"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	iamv1 "github.com/kubeclipper/kubeclipper/pkg/scheme/iam/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
	"github.com/kubeclipper/kubeclipper/test/framework"
)

var _ = ginkgo.Describe("[OCI-Access] [Serial]", func() {
	f := framework.NewDefaultFramework("oci-access")

	ginkgo.It("checks the active CLI identity and authorizes the same resources by role", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		revision := os.Getenv("KC_E2E_BACKEND_REVISION")
		Expect(revision).To(MatchRegexp("^[0-9a-f]{40}$"), "set KC_E2E_BACKEND_REVISION to the candidate commit")
		version, err := f.Client.Version(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(version.GitCommit).To(Equal(revision))
		endpoint := f.Client.Scheme() + "://" + f.Client.Host()

		suffix := string(uuid.NewUUID())[:8]
		viewerName, unassignedName := "oci-view-"+suffix, "oci-unassigned-"+suffix
		password := "Kc7-" + string(uuid.NewUUID())[:8]
		for _, user := range []struct{ name, role string }{{viewerName, "platform-view"}, {unassignedName, ""}} {
			result, err := f.Client.DescribeUser(ctx, user.name)
			Expect(apierror.IsNotFound(err)).To(BeTrue(), "the test username must be new")
			Expect(result).To(BeNil())
			defer func() {
				cleanupCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
				defer done()
				err := f.Client.DeleteUser(cleanupCtx, user.name)
				if !apierror.IsNotFound(err) {
					Expect(err).NotTo(HaveOccurred(), "cleanup test account %s", user.name)
				}
			}()
			created, err := f.Client.CreateUser(ctx, &iamv1.User{
				ObjectMeta: metav1.ObjectMeta{Name: user.name, Annotations: map[string]string{common.RoleAnnotation: user.role}},
				Spec:       iamv1.UserSpec{Email: user.name + "@example.test", EncryptedPassword: password},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(created.Items).To(HaveLen(1))
			Expect(created.Items[0].Name).To(Equal(user.name))
			Expect(created.Items[0].UID).NotTo(BeEmpty())
		}

		ginkgo.By("[3-25] Logging in with the candidate CLI and preserving the context after a rejected login")
		home, err := os.MkdirTemp("", "kc-oci-login-")
		Expect(err).NotTo(HaveOccurred())
		defer os.RemoveAll(home)
		configPath := filepath.Join(home, ".kc", "config")
		login := func(name, credential string) ([]byte, error) {
			cmd := exec.CommandContext(ctx, "kcctl", "login", "-H", endpoint, "-u", name, "-p", credential)
			cmd.Env = append(os.Environ(), "HOME="+home)
			return cmd.CombinedOutput()
		}
		_, err = login(viewerName, password)
		Expect(err).NotTo(HaveOccurred(), "valid account login")
		before, err := os.ReadFile(configPath)
		Expect(err).NotTo(HaveOccurred())
		cfg, err := config.TryLoadFromFile(configPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(cfg.CurrentContext).To(Equal(viewerName + "@default"))
		output, err := login(unassignedName, password+"x")
		Expect(err).To(BeAssignableToTypeOf(&exec.ExitError{}))
		Expect(strings.Contains(string(output), "incorrect username or password")).To(BeTrue(), "the server refused the bad password")
		after, err := os.ReadFile(configPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(bytes.Equal(before, after)).To(BeTrue(), "the prior CLI identity remains unchanged")

		ginkgo.By("[4-08b] Checking real viewer, unassigned and anonymous requests")
		viewer, err := kc.FromConfig(*cfg)
		Expect(err).NotTo(HaveOccurred())
		_, err = viewer.ListClusters(ctx, kc.Queries{})
		Expect(err).NotTo(HaveOccurred(), "viewer can list clusters")
		token, err := f.Client.Login(ctx, kc.LoginRequest{Username: unassignedName, Password: password})
		Expect(err).NotTo(HaveOccurred())
		Expect(token.AccessToken != "").To(BeTrue(), "unassigned account was authenticated")
		unassigned, err := kc.NewClientWithOpts(kc.WithEndpoint(endpoint), kc.WithBearerAuth(token.AccessToken), kc.WithHTTPClient(f.Client.HTTPClient()))
		Expect(err).NotTo(HaveOccurred())
		_, err = unassigned.ListClusters(ctx, kc.Queries{})
		Expect(apierror.CodeForError(err)).To(Equal(int32(http.StatusForbidden)), "unassigned account cannot list clusters")
		anonymous, err := kc.NewClientWithOpts(kc.WithEndpoint(endpoint), kc.WithHTTPClient(f.Client.HTTPClient()))
		Expect(err).NotTo(HaveOccurred())
		_, err = anonymous.ListClusters(ctx, kc.Queries{})
		Expect(apierror.CodeForError(err)).To(BeElementOf(int32(http.StatusUnauthorized), int32(http.StatusForbidden)))

		target, err := f.Client.DescribeUser(ctx, unassignedName)
		Expect(err).NotTo(HaveOccurred())
		Expect(apierror.CodeForError(viewer.DeleteUser(ctx, unassignedName))).To(Equal(int32(http.StatusForbidden)))
		existing, err := f.Client.DescribeUser(ctx, unassignedName)
		Expect(err).NotTo(HaveOccurred())
		Expect(existing.Items[0].UID).To(Equal(target.Items[0].UID))
		Expect(existing.Items[0].ResourceVersion).To(Equal(target.Items[0].ResourceVersion))
		Expect(f.Client.DeleteUser(ctx, unassignedName)).To(Succeed(), "admin can delete the same test account")
		_, err = f.Client.DescribeUser(ctx, unassignedName)
		Expect(apierror.IsNotFound(err)).To(BeTrue())
		fmt.Fprintf(ginkgo.GinkgoWriter, "verified CLI context and access for test users %s and %s\n", viewerName, unassignedName)
	})
})

var _ = ginkgo.Describe("[OCI-Tokens] [Serial]", func() {
	f := framework.NewDefaultFramework("oci-tokens")
	ginkgo.It("[4-08d] issues, refreshes and revokes a test user's tokens", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		revision := os.Getenv("KC_E2E_BACKEND_REVISION")
		Expect(revision).To(MatchRegexp("^[0-9a-f]{40}$"))
		version, err := f.Client.Version(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(version.GitCommit).To(Equal(revision))

		name := "oci-token-" + string(uuid.NewUUID())[:8]
		password := "Kc7-" + string(uuid.NewUUID())[:8]
		_, err = f.Client.CreateUser(ctx, &iamv1.User{
			ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: map[string]string{common.RoleAnnotation: "platform-view"}},
			Spec:       iamv1.UserSpec{Email: name + "@example.test", EncryptedPassword: password},
		})
		Expect(err).NotTo(HaveOccurred())
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			Expect(f.Client.DeleteUser(cleanup, name)).To(Succeed())
		}()
		endpoint := f.Client.Scheme() + "://" + f.Client.Host()
		refresh := func(token string, expected int) oauth.Token {
			body := url.Values{"grant_type": {"refresh_token"}, "refresh_token": {token}}
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/oauth/token", strings.NewReader(body.Encode()))
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			res, err := f.Client.HTTPClient().Do(req)
			Expect(err).NotTo(HaveOccurred())
			defer res.Body.Close()
			Expect(res.StatusCode).To(Equal(expected))
			var result oauth.Token
			if expected == http.StatusOK {
				Expect(json.NewDecoder(res.Body).Decode(&result)).To(Succeed())
				Expect(result.AccessToken != "" && result.RefreshToken != "").To(BeTrue(), "refresh issued both tokens")
			}
			return result
		}
		list := func(token string) error {
			client, err := kc.NewClientWithOpts(kc.WithEndpoint(endpoint), kc.WithBearerAuth(token), kc.WithHTTPClient(f.Client.HTTPClient()))
			Expect(err).NotTo(HaveOccurred())
			_, err = client.ListClusters(ctx, kc.Queries{})
			return err
		}
		issued, err := f.Client.Login(ctx, kc.LoginRequest{Username: name, Password: password})
		Expect(err).NotTo(HaveOccurred())
		Expect(issued.AccessToken != "" && issued.RefreshToken != "").To(BeTrue(), "login issued both tokens")
		Expect(list(issued.AccessToken)).To(Succeed())
		refreshed := refresh(issued.RefreshToken, http.StatusOK)
		Expect(list(refreshed.AccessToken)).To(Succeed())

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/oauth/logout", nil)
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+refreshed.AccessToken)
		res, err := f.Client.HTTPClient().Do(req)
		Expect(err).NotTo(HaveOccurred())
		res.Body.Close()
		Expect(res.StatusCode).To(Equal(http.StatusOK))
		for _, token := range []string{issued.AccessToken, refreshed.AccessToken} {
			Expect(apierror.CodeForError(list(token))).To(Equal(int32(http.StatusUnauthorized)))
		}
		refresh(issued.RefreshToken, http.StatusUnauthorized)
		refresh(refreshed.RefreshToken, http.StatusUnauthorized)
		_, err = f.Client.ListClusters(ctx, kc.Queries{})
		Expect(err).NotTo(HaveOccurred(), "the test user's logout must leave administrator access intact")
	})
})

var _ = ginkgo.Describe("[OCI-Accounts] [Serial]", func() {
	f := framework.NewDefaultFramework("oci-accounts")

	ginkgo.It("[4-08] manages a real CLI user and role, rotates the password and verifies login history", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		revision := os.Getenv("KC_E2E_BACKEND_REVISION")
		Expect(revision).To(MatchRegexp("^[0-9a-f]{40}$"))
		version, err := f.Client.Version(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(version.GitCommit).To(Equal(revision))

		suffix := string(uuid.NewUUID())[:8]
		user, role := "oci-user-"+suffix, "oci-role-"+suffix
		password := "Kc7-" + string(uuid.NewUUID())[:8]
		newPassword := "Kc8-" + string(uuid.NewUUID())[:8]
		run := func(args ...string) {
			Expect(exec.CommandContext(ctx, "kcctl", args...).Run()).To(Succeed(), "kcctl %s %s", args[0], args[1])
		}
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			userErr := f.Client.DeleteUser(cleanup, user)
			roleErr := f.Client.DeleteRole(cleanup, role)
			if !apierror.IsNotFound(userErr) {
				Expect(userErr).NotTo(HaveOccurred())
			}
			if !apierror.IsNotFound(roleErr) {
				Expect(roleErr).NotTo(HaveOccurred())
			}
		}()

		ginkgo.By("Creating the test role and user with the candidate CLI")
		run("create", "role", "--name", role, "--rules=role-template-view-clusters")
		createdRole, err := f.Client.DescribeRole(ctx, role)
		Expect(err).NotTo(HaveOccurred())
		Expect(createdRole.Items).To(HaveLen(1))
		Expect(createdRole.Items[0].UID).NotTo(BeEmpty())
		run("create", "user", "--name", user, "--role", role, "--email", user+"@example.test", "--phone", "2025550100", "--password", password)
		createdUser, err := f.Client.DescribeUser(ctx, user)
		Expect(err).NotTo(HaveOccurred())
		Expect(createdUser.Items).To(HaveLen(1))
		Expect(createdUser.Items[0].Annotations[common.RoleAnnotation]).To(Equal(role))

		endpoint := f.Client.Scheme() + "://" + f.Client.Host()
		path := endpoint + "/api/iam.kubeclipper.io/v1/users/" + user
		mutate := func(action string, payload []byte) {
			req, err := http.NewRequestWithContext(ctx, http.MethodPut, path+"/"+action, bytes.NewReader(payload))
			Expect(err).NotTo(HaveOccurred())
			req.Header.Set("Authorization", "Bearer "+f.Client.Token())
			req.Header.Set("Content-Type", "application/json")
			res, err := f.Client.HTTPClient().Do(req)
			Expect(err).NotTo(HaveOccurred())
			defer res.Body.Close()
			Expect(res.StatusCode).To(Equal(http.StatusOK), "user action %s", action)
		}
		login := func(credential string) (*oauth.Token, error) {
			return f.Client.Login(ctx, kc.LoginRequest{Username: user, Password: credential})
		}
		checkAccess := func(credential string) {
			token, err := login(credential)
			Expect(err).NotTo(HaveOccurred())
			Expect(token.AccessToken != "").To(BeTrue())
			session, err := kc.NewClientWithOpts(kc.WithEndpoint(endpoint), kc.WithBearerAuth(token.AccessToken), kc.WithHTTPClient(f.Client.HTTPClient()))
			Expect(err).NotTo(HaveOccurred())
			_, err = session.ListClusters(ctx, kc.Queries{})
			Expect(err).NotTo(HaveOccurred())
		}
		checkAccess(password)

		ginkgo.By("Changing the password and verifying disabled and enabled login")
		payload, err := json.Marshal(iamapi.PasswordReset{CurrentPassword: password, NewPassword: newPassword})
		Expect(err).NotTo(HaveOccurred())
		mutate("password", payload)
		_, err = login(password)
		Expect(apierror.CodeForError(err)).To(Equal(int32(http.StatusUnauthorized)))
		checkAccess(newPassword)
		mutate("disable", nil)
		_, err = login(newPassword)
		Expect(apierror.CodeForError(err)).To(Equal(int32(http.StatusForbidden)))
		mutate("enable", nil)
		checkAccess(newPassword)

		ginkgo.By("Finding the successful and failed attempts in the real login history")
		Eventually(func() error {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, path+"/loginrecords", nil)
			if err != nil {
				return err
			}
			req.Header.Set("Authorization", "Bearer "+f.Client.Token())
			res, err := f.Client.HTTPClient().Do(req)
			if err != nil {
				return err
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				return fmt.Errorf("login history returned HTTP %d", res.StatusCode)
			}
			var history iamv1.LoginRecordList
			if err := json.NewDecoder(res.Body).Decode(&history); err != nil {
				return err
			}
			var accepted, rejected int
			for _, record := range history.Items {
				if record.Labels[common.LabelUserReference] != user {
					return fmt.Errorf("login history included another account")
				}
				if record.Spec.Success {
					accepted++
				} else {
					rejected++
				}
			}
			if accepted != 3 || rejected < 1 {
				return fmt.Errorf("expected 3 successful and at least 1 failed password login records, got %d successful and %d failed", accepted, rejected)
			}
			return nil
		}, 20*time.Second, time.Second).Should(Succeed())

		ginkgo.By("Deleting the user and role with the CLI and checking they are gone")
		run("delete", "user", user)
		run("delete", "role", role)
		_, err = f.Client.DescribeUser(ctx, user)
		Expect(apierror.IsNotFound(err)).To(BeTrue())
		_, err = f.Client.DescribeRole(ctx, role)
		Expect(apierror.IsNotFound(err)).To(BeTrue())
	})
})
