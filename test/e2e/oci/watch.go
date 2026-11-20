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
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"

	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/test/framework"
)

var _ = ginkgo.Describe("[OCI-NamedWatch] [Serial]", func() {
	f := framework.NewDefaultFramework("oci-named-watch")

	ginkgo.It("[3-10] only emits JSON changes to the requested object and stops on interrupt", func() {
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		defer cancel()
		revision := os.Getenv("KC_E2E_BACKEND_REVISION")
		Expect(revision).To(MatchRegexp("^[0-9a-f]{40}$"))
		version, err := f.Client.Version(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(version.GitCommit).To(Equal(revision))

		create := func(name string) *corev1.ConfigMap {
			result, err := f.Client.CreateConfigMap(ctx, &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: name},
				Data:       map[string]string{"marker": "initial"},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Items).To(HaveLen(1))
			Expect(result.Items[0].UID).NotTo(BeEmpty())
			return &result.Items[0]
		}
		remove := func(name string) {
			cleanup, done := context.WithTimeout(context.Background(), 20*time.Second)
			defer done()
			Expect(f.Client.DeleteConfigMap(cleanup, name)).To(Succeed())
		}
		suffix := string(uuid.NewUUID())[:8]
		target := create("oci-watch-target-" + suffix)
		defer remove(target.Name)
		control := create("oci-watch-control-" + suffix)
		defer remove(control.Name)

		watchCtx, stopWatch := context.WithCancel(ctx)
		defer stopWatch()
		cmd := exec.CommandContext(watchCtx, "kcctl", "get", "configmap", target.Name, "--watch", "-o", "json")
		cmd.Cancel = func() error { return cmd.Process.Signal(os.Interrupt) }
		cmd.WaitDelay = 5 * time.Second
		stdout, err := cmd.StdoutPipe()
		Expect(err).NotTo(HaveOccurred())
		Expect(cmd.Start()).To(Succeed())
		waited := false
		defer func() {
			if !waited {
				stopWatch()
				_ = cmd.Wait()
			}
		}()

		type event struct {
			Type   string
			Object corev1.ConfigMap
		}
		events := make(chan event, 16)
		readErr := make(chan error, 1)
		initial := make(chan corev1.ConfigMap, 1)
		go func() {
			decoder := json.NewDecoder(stdout)
			var current corev1.ConfigMap
			if err := decoder.Decode(&current); err != nil {
				readErr <- err
				return
			}
			initial <- current
			for {
				var update event
				if err := decoder.Decode(&update); err != nil {
					readErr <- err
					return
				}
				select {
				case events <- update:
				case <-watchCtx.Done():
					return
				}
			}
		}()
		var info corev1.ConfigMap
		Eventually(initial, 10*time.Second).Should(Receive(&info))
		Expect(info.UID).To(Equal(target.UID))

		update := func(object *corev1.ConfigMap, marker string) *corev1.ConfigMap {
			object.Data["marker"] = marker
			result, err := f.Client.UpdateConfigMap(ctx, object)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Items).To(HaveLen(1))
			return &result.Items[0]
		}
		observe := func(prefix string) {
			attempt := 0
			Eventually(func() bool {
				// The initial CLI GET is emitted before it opens the stream.
				attempt++
				target = update(target, fmt.Sprintf("%s-%d", prefix, attempt))
				for {
					select {
					case result := <-events:
						Expect(result.Object.UID).To(Equal(target.UID), "a named watch must exclude other objects")
						if result.Type == "ADDED" {
							continue
						}
						Expect(result.Type).To(Equal("MODIFIED"))
						if strings.HasPrefix(result.Object.Data["marker"], prefix+"-") {
							return true
						}
					case err := <-readErr:
						Expect(err).NotTo(HaveOccurred(), "watch stream ended before the expected update")
					default:
						return false
					}
				}
			}, 15*time.Second, 500*time.Millisecond).Should(BeTrue())
		}
		observe("opened")
		update(control, "control-only")
		observe("after-control")
		Expect(cmd.Process.Signal(os.Interrupt)).To(Succeed())
		wait := make(chan error, 1)
		go func() { wait <- cmd.Wait() }()
		var exitErr error
		select {
		case exitErr = <-wait:
		case <-time.After(8 * time.Second):
			stopWatch()
			<-wait
			waited = true
			ginkgo.Fail("kcctl watch did not exit after SIGINT")
		}
		waited = true
		Expect(exitErr).NotTo(HaveOccurred())
	})
})
