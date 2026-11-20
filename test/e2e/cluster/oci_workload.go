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

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	kubev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

type ociWorkload struct {
	namespace string
	dnsDomain string
	hosts     map[string]string // Probe label to Kubernetes node name.
}

func startOCIWorkload(ctx context.Context, k8s *kubernetes.Clientset, namespace, image, domain string, nodes map[string]corev1.Node, primaryMaster string, workers []string) ociWorkload {
	_, err := k8s.CoreV1().Namespaces().Create(ctx, &kubev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())

	_, err = k8s.CoreV1().ConfigMaps(namespace).Create(ctx, &kubev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "marker"},
		Data:       map[string]string{"value": "before"},
	}, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	Expect(domain).NotTo(BeEmpty())

	probe := ociWorkload{namespace: namespace, dnsDomain: domain, hosts: make(map[string]string)}
	ids := append(append([]string{}, workers...), primaryMaster)
	for _, id := range ids {
		probe.add(ctx, k8s, image, nodes[id].Status.NodeInfo.Hostname, id == primaryMaster && len(nodes) > 1)
	}
	_, err = k8s.CoreV1().Services(namespace).Create(ctx, &kubev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "echo"},
		Spec: kubev1.ServiceSpec{
			Selector: map[string]string{"probe": "probe-0"},
			Ports:    []kubev1.ServicePort{{Port: 8080}},
		},
	}, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	return probe
}

func (probe ociWorkload) add(ctx context.Context, k8s *kubernetes.Clientset, image, host string, tolerateControlPlane bool) string {
	name := fmt.Sprintf("probe-%d", len(probe.hosts))
	selector := map[string]string{"probe": name}
	probe.hosts[name] = host
	spec := kubev1.PodSpec{
		NodeSelector: map[string]string{kubev1.LabelHostname: host},
		Containers: []kubev1.Container{{
			Name:    "echo",
			Image:   image,
			Command: []string{"sh", "-ec", "printf '%s' \"$RUN_MARKER\" >/tmp/index.html; exec httpd -f -p 8080 -h /tmp"},
			Env:     []kubev1.EnvVar{{Name: "RUN_MARKER", Value: probe.namespace}},
			ReadinessProbe: &kubev1.Probe{ProbeHandler: kubev1.ProbeHandler{
				HTTPGet: &kubev1.HTTPGetAction{Port: intstr.FromInt(8080), Path: "/"},
			}},
		}},
	}
	if tolerateControlPlane {
		spec.Tolerations = []kubev1.Toleration{
			{Key: "node-role.kubernetes.io/control-plane", Operator: kubev1.TolerationOpExists},
			{Key: "node-role.kubernetes.io/master", Operator: kubev1.TolerationOpExists},
		}
	}
	_, err := k8s.AppsV1().Deployments(probe.namespace).Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: kubev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: selector}, Spec: spec},
		},
	}, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	return name
}

func (probe ociWorkload) readyPod(ctx context.Context, k8s *kubernetes.Clientset, name string) kubev1.Pod {
	var ready kubev1.Pod
	Eventually(func() error {
		pods, err := k8s.CoreV1().Pods(probe.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labels.Set{"probe": name}.String(),
		})
		if err != nil {
			return err
		}
		for _, pod := range pods.Items {
			if pod.DeletionTimestamp != nil || pod.Status.PodIP == "" || pod.Spec.NodeName != probe.hosts[name] {
				continue
			}
			for _, condition := range pod.Status.Conditions {
				if condition.Type == kubev1.PodReady && condition.Status == kubev1.ConditionTrue {
					ready = pod
					return nil
				}
			}
		}
		return fmt.Errorf("%s has no Ready Pod scheduled to %s", name, probe.hosts[name])
	}, 4*time.Minute, 2*time.Second).Should(Succeed())
	return ready
}

func (probe ociWorkload) check(ctx context.Context, k8s *kubernetes.Clientset, config *rest.Config, expectedMarker string) {
	marker, err := k8s.CoreV1().ConfigMaps(probe.namespace).Get(ctx, "marker", metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())
	Expect(marker.Data).To(HaveKeyWithValue("value", expectedMarker))
	backend := probe.readyPod(ctx, k8s, "probe-0")
	pods := []kubev1.Pod{backend}
	for name := range probe.hosts {
		if name != "probe-0" {
			pods = append(pods, probe.readyPod(ctx, k8s, name))
		}
	}
	Eventually(func() error {
		endpoints, err := k8s.DiscoveryV1().EndpointSlices(probe.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: labels.Set{discoveryv1.LabelServiceName: "echo"}.String(),
		})
		if err != nil {
			return err
		}
		for _, slice := range endpoints.Items {
			for _, endpoint := range slice.Endpoints {
				if endpoint.TargetRef != nil && endpoint.TargetRef.UID == backend.UID &&
					endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready {
					return nil
				}
			}
		}
		return fmt.Errorf("Service echo has no ready endpoint for Pod %s", backend.Name)
	}, 4*time.Minute, 3*time.Second).Should(Succeed())
	service := fmt.Sprintf("echo.%s.svc.%s:8080", probe.namespace, probe.dnsDomain)
	for _, source := range pods {
		probe.http(ctx, k8s, config, source.Name, service)
		for _, target := range pods {
			if source.Name != target.Name {
				probe.http(ctx, k8s, config, source.Name, net.JoinHostPort(target.Status.PodIP, "8080"))
			}
		}
	}
}

func (probe ociWorkload) http(ctx context.Context, k8s *kubernetes.Clientset, config *rest.Config, from, endpoint string) {
	req := k8s.CoreV1().RESTClient().Post().Namespace(probe.namespace).Resource("pods").Name(from).SubResource("exec").
		VersionedParams(&kubev1.PodExecOptions{
			Container: "echo",
			Command:   []string{"wget", "-T", "5", "-qO-", "http://" + endpoint},
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	command, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	Expect(err).NotTo(HaveOccurred())
	Eventually(func() error {
		callCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
		defer cancel()
		var stdout, stderr bytes.Buffer
		if err := command.StreamWithContext(callCtx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
			return fmt.Errorf("%s to %s: %w: %s", from, endpoint, err, stderr.String())
		}
		if stdout.String() != probe.namespace {
			return fmt.Errorf("%s returned %q, expected the marker %s", endpoint, stdout.String(), probe.namespace)
		}
		return nil
	}, 90*time.Second, 3*time.Second).Should(Succeed())
}

func upgradeOCICluster(ctx context.Context, client *kc.Client, k8s *kubernetes.Clientset, config *rest.Config, target *corev1.Cluster, probe ociWorkload, nodes map[string]corev1.Node, masterIDs []string, version string, timeout time.Duration) {
	ginkgo.By("[2.3-01] Verifying explicit single-node maintenance and post-upgrade workload recovery")
	pod := probe.readyPod(ctx, k8s, "probe-0")
	one := intstr.FromInt(1)
	_, err := k8s.PolicyV1().PodDisruptionBudgets(probe.namespace).Create(ctx, &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "keep-probe"},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &one,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"probe": "probe-0"}},
		},
	}, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	Eventually(func() error {
		pdb, err := k8s.PolicyV1().PodDisruptionBudgets(probe.namespace).Get(ctx, "keep-probe", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if pdb.Status.ObservedGeneration != pdb.Generation || pdb.Status.CurrentHealthy != 1 || pdb.Status.DisruptionsAllowed != 0 {
			return fmt.Errorf("test PDB does not yet protect the only replica")
		}
		return nil
	}, time.Minute, 2*time.Second).Should(Succeed())

	args := []string{"cluster", "upgrade", "--cluster-name", target.Name, "--version", version}
	callCtx, cancel := context.WithTimeout(ctx, time.Minute)
	output, denied := exec.CommandContext(callCtx, "kcctl", args...).CombinedOutput()
	cancel()
	Expect(denied).To(BeAssignableToTypeOf(&exec.ExitError{}))
	Expect(strings.TrimSpace(string(output))).To(ContainSubstring("singleNodeMaintenance=true"))
	result, err := client.ListOperations(ctx, kc.OperationListOptions{FieldSelector: "spec.targetRef.uid=" + string(target.UID)})
	Expect(err).NotTo(HaveOccurred())
	for _, intent := range result.Items {
		Expect(intent.Spec.Action).NotTo(Equal(corev1.OperationUpgradeCluster))
	}

	runOCICommand(ctx, append(args, "--single-node-maintenance")...)
	waitForOCIOperation(ctx, client, target.UID, corev1.OperationUpgradeCluster, "", timeout)
	waitForOCIClusterRunning(ctx, client, target.Name, version, time.Minute)
	checkOCINodes(ctx, k8s, nodes, masterIDs, version, target.ContainerRuntime.Version)
	Expect(probe.readyPod(ctx, k8s, "probe-0").UID).NotTo(Equal(pod.UID), "protected Pod must be replaced during single-node maintenance")
	probe.check(ctx, k8s, config, "before")
}
