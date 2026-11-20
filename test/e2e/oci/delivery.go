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
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	delivery "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/scheme"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
	"github.com/kubeclipper/kubeclipper/test/framework"
)

var _ = ginkgo.Describe("[OCI-DeliveryPolicy] [Serial]", func() {
	f := framework.NewDefaultFramework("oci-delivery-policy")

	ginkgo.It("[2.6-08, 2.6-09, 3-23] checks the default and applies a narrower delivery policy through kcctl", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		revision := os.Getenv("KC_E2E_BACKEND_REVISION")
		Expect(revision).To(MatchRegexp("^[0-9a-f]{40}$"))
		version, err := f.Client.Version(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(version.GitCommit).To(Equal(revision))
		clusters, err := f.Client.ListClusters(ctx, kc.Queries{})
		Expect(err).NotTo(HaveOccurred())
		Expect(clusters.Items).To(BeEmpty(), "use an isolated platform before changing its delivery policy")

		run := func(args ...string) []byte {
			output, err := exec.CommandContext(ctx, "kcctl", append([]string{"delivery-policy"}, args...)...).CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), "delivery-policy %s: %s", args[0], output)
			return output
		}
		ginkgo.By("[2.6-08] Matching the seeded API policy to the candidate CLI and published components")
		original, err := f.Client.GetDeliveryPolicy(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(original.Spec).To(Equal(delivery.DefaultSupportPolicy().Spec))
		var printed, narrowed delivery.SupportPolicy
		Expect(json.Unmarshal(run("get", "-o", "json"), &printed)).To(Succeed())
		Expect(&printed).To(Equal(original))
		Expect(json.Unmarshal(run("template", "-o", "json"), &narrowed)).To(Succeed())
		Expect(narrowed.Spec).To(Equal(original.Spec))
		versions := func() []string {
			meta, err := f.Client.GetComponentMeta(ctx, url.Values{"arch": {"amd64"}})
			Expect(err).NotTo(HaveOccurred())
			Expect(meta.Registry).NotTo(BeEmpty())
			var available []string
			for _, resource := range meta.Addons {
				if resource.Type == "k8s" {
					available = append(available, resource.Version)
				}
			}
			Expect(slices.ContainsFunc(meta.Addons, func(resource scheme.MetaResource) bool {
				return resource.Type == "cri" && resource.Name == "containerd" && resource.Version == "2.2.4" && resource.Arch == "amd64"
			})).To(BeTrue(), "component metadata must contain the published runtime allowed by the retained policy")
			return available
		}
		before := versions()
		Expect(before).To(ContainElements("v1.36.4", "v1.37.0"))

		ginkgo.By("[3-23, 2.6-09] Comparing a narrower policy, applying it, and reading the changed version list")
		narrowed.Spec.Policies = slices.DeleteFunc(narrowed.Spec.Policies, func(policy delivery.KubernetesSupportPolicy) bool {
			return policy.Match.KubernetesVersion == "v1.36.*"
		})
		payload, err := json.Marshal(narrowed)
		Expect(err).NotTo(HaveOccurred())
		dir, err := os.MkdirTemp("", "oci-delivery-")
		Expect(err).NotTo(HaveOccurred())
		defer os.RemoveAll(dir)
		path := filepath.Join(dir, "policy.json")
		Expect(os.WriteFile(path, payload, 0600)).To(Succeed())
		Expect(string(run("validate", "-f", path))).To(ContainSubstring("is valid"))
		Expect(string(run("diff", "-f", path))).To(ContainSubstring("k8s-v1.36"))
		current, err := f.Client.GetDeliveryPolicy(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(current).To(Equal(original), "validate and diff must leave the stored policy unchanged")

		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			restored, err := f.Client.UpdateDeliveryPolicy(cleanup, original)
			Expect(err).NotTo(HaveOccurred())
			Expect(restored.Spec).To(Equal(original.Spec))
			stored, err := f.Client.GetDeliveryPolicy(cleanup)
			Expect(err).NotTo(HaveOccurred())
			Expect(stored.Spec).To(Equal(original.Spec))
		}()
		Expect(json.Unmarshal(run("apply", "-f", path, "-o", "json"), &printed)).To(Succeed())
		Expect(printed.Spec).To(Equal(narrowed.Spec))
		Expect(string(run("diff", "-f", path))).To(ContainSubstring("has no changes"))
		after := versions()
		Expect(after).To(ContainElement("v1.37.0"))
		Expect(after).NotTo(ContainElement("v1.36.4"))
	})
})
