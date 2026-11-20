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
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	kubev1 "k8s.io/api/core/v1"
	kubeerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	coreapi "github.com/kubeclipper/kubeclipper/pkg/apis/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/clusteroperation"
	apierror "github.com/kubeclipper/kubeclipper/pkg/errors"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	iamv1 "github.com/kubeclipper/kubeclipper/pkg/scheme/iam/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
	"github.com/kubeclipper/kubeclipper/test/framework"
)

func checkOCIClusterAccess(ctx context.Context, admin *kc.Client, direct *kubernetes.Clientset, config *rest.Config, target *corev1.Cluster, _ map[string]corev1.Node, probe ociWorkload, _ func(func())) {
	ginkgo.By("[4-12] Writing and reading the same object via both downloaded kubeconfigs")
	proxyConfig, err := loadOCIKubeconfig(ctx, admin, target.Name, true)
	Expect(err).NotTo(HaveOccurred())
	Expect(proxyConfig.Host != config.Host).To(BeTrue(), "direct and proxy downloads must use different endpoints")
	proxy, err := kubernetes.NewForConfig(proxyConfig)
	Expect(err).NotTo(HaveOccurred())
	directMarkers := direct.CoreV1().ConfigMaps(probe.namespace)
	proxyMarkers := proxy.CoreV1().ConfigMaps(probe.namespace)
	name := "download-" + string(uuid.NewUUID())[:8]
	created, err := directMarkers.Create(ctx, &kubev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Data:       map[string]string{"value": "direct-" + name},
	}, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	viaProxy, err := proxyMarkers.Get(ctx, name, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())
	Expect(viaProxy.UID).To(Equal(created.UID))
	Expect(viaProxy.Data).To(Equal(created.Data))
	viaProxy.Data["value"] = "proxy-" + name
	_, err = proxyMarkers.Update(ctx, viaProxy, metav1.UpdateOptions{})
	Expect(err).NotTo(HaveOccurred())
	viaDirect, err := directMarkers.Get(ctx, name, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())
	Expect(viaDirect.UID).To(Equal(created.UID))
	Expect(viaDirect.Data).To(HaveKeyWithValue("value", "proxy-"+name))
	Expect(proxyMarkers.Delete(ctx, name, metav1.DeleteOptions{})).To(Succeed())
	_, err = directMarkers.Get(ctx, name, metav1.GetOptions{})
	Expect(kubeerrors.IsNotFound(err)).To(BeTrue())

	ginkgo.By("[4-08b, 4-12] Proving a real cluster viewer cannot get credentials or enqueue an upgrade")
	viewerName := "oci-kubeview-" + string(uuid.NewUUID())[:8]
	password := "Kc7-" + string(uuid.NewUUID())[:8]
	_, err = admin.CreateUser(ctx, &iamv1.User{
		ObjectMeta: metav1.ObjectMeta{Name: viewerName, Annotations: map[string]string{common.RoleAnnotation: "platform-view"}},
		Spec:       iamv1.UserSpec{Email: viewerName + "@example.test", EncryptedPassword: password},
	})
	Expect(err).NotTo(HaveOccurred())
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		Expect(admin.DeleteUser(cleanup, viewerName)).To(Succeed())
	}()
	token, err := admin.Login(ctx, kc.LoginRequest{Username: viewerName, Password: password})
	Expect(err).NotTo(HaveOccurred())
	Expect(token.AccessToken != "").To(BeTrue())
	viewer, err := kc.NewClientWithOpts(kc.WithEndpoint(admin.Scheme()+"://"+admin.Host()), kc.WithBearerAuth(token.AccessToken), kc.WithHTTPClient(admin.HTTPClient()))
	Expect(err).NotTo(HaveOccurred())
	visible, err := viewer.DescribeCluster(ctx, target.Name)
	Expect(err).NotTo(HaveOccurred())
	Expect(visible.Items).To(HaveLen(1))
	Expect(visible.Items[0].UID).To(Equal(target.UID))
	for _, proxyMode := range []bool{false, true} {
		_, err := viewer.ClusterKubeconfig(ctx, target.Name, proxyMode)
		Expect(apierror.CodeForError(err)).To(Equal(int32(http.StatusForbidden)), "viewer download proxy=%t", proxyMode)
		_, err = admin.ClusterKubeconfig(ctx, target.Name+"-missing", proxyMode)
		Expect(apierror.IsNotFound(err)).To(BeTrue(), "missing cluster download proxy=%t", proxyMode)
	}
	err = viewer.UpgradeCluster(ctx, target.Name, &coreapi.ClusterUpgrade{Version: target.KubernetesVersion})
	Expect(apierror.CodeForError(err)).To(Equal(int32(http.StatusForbidden)), "authorization must reject the request before version validation")
	ops, err := admin.ListOperations(ctx, kc.OperationListOptions{FieldSelector: "spec.targetRef.uid=" + string(target.UID)})
	Expect(err).NotTo(HaveOccurred())
	for _, op := range ops.Items {
		Expect(op.Spec.Action).NotTo(Equal(corev1.OperationUpgradeCluster))
	}
}

func checkOCIUpgradeAdmission(ctx context.Context, client *kc.Client, k8s *kubernetes.Clientset, config *rest.Config, target *corev1.Cluster, nodes map[string]corev1.Node, probe ociWorkload, _ func(func())) {
	timeout := framework.NewTimeoutContextWithDefaults()
	create := waitForOCIOperation(ctx, client, target.UID, corev1.OperationCreateCluster, "", timeout.CommonTimeout)
	node := nodes[target.Masters[0].ID]

	ginkgo.By("[3-11] Matching candidate CLI operation diagnostics to the real operation and its Task logs")
	list := runOCICommand(ctx, "operation", "list", "--cluster", target.Name)
	Expect(list).To(ContainSubstring(create.Name))
	Expect(list).To(ContainSubstring(target.Name))
	detail := runOCICommand(ctx, "operation", "describe", create.Name)
	Expect(detail).To(ContainSubstring(create.Name))
	Expect(detail).To(ContainSubstring(corev1.OperationCreateCluster))
	Expect(detail).To(ContainSubstring(string(operations.OperationSucceeded)))
	Expect(detail).To(ContainSubstring(node.Name + " (" + string(node.UID) + ")"))
	tasks, err := client.ListOperationTasks(ctx, string(create.UID))
	Expect(err).NotTo(HaveOccurred())

	// Keep raw task output out of the test report.
	logCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	output, err := exec.CommandContext(logCtx, "kcctl", "operation", "logs", create.Name, "--max-length=0").Output()
	Expect(err).NotTo(HaveOccurred(), "read logs for operation %s", create.Name)
	logs := string(output)
	matchedTaskLog := false
	for _, task := range tasks.Items {
		if task.Spec.NodeRef.Name != node.Name || task.Spec.NodeRef.UID != node.UID || task.Status.Phase != operations.TaskSucceeded {
			continue
		}
		log, err := client.GetOperationTaskLog(ctx, task.Name, 0)
		Expect(err).NotTo(HaveOccurred())
		for _, line := range strings.Split(log.Content, "\n") {
			line = strings.TrimSpace(line)
			if len(line) >= 24 && strings.Contains(logs, line) {
				Expect(strings.Contains(logs, "Step: "+task.Spec.StepID+" Node: "+node.Name)).To(BeTrue(), "CLI must identify the Task's step and node")
				matchedTaskLog = true
				break
			}
		}
		if matchedTaskLog {
			break
		}
	}
	Expect(matchedTaskLog).To(BeTrue(), "candidate CLI must display real log content for the selected operation")

	ginkgo.By("[2.3-08] Sending invalid versions and missing maintenance confirmation to the actual upgrade endpoint")
	pod := probe.readyPod(ctx, k8s, "probe-0")
	cases := []struct {
		version, reason string
		maintenance     bool
	}{
		{"v1.36.4", "already at version", true},
		{"v1.35.8", "cannot downgrade", true},
		{"v1.38.0", "too high", true},
		{"invalid", "wrong version format", true},
		{"v1.37.0", "singleNodeMaintenance=true", false},
	}
	for _, test := range cases {
		err := client.UpgradeCluster(ctx, target.Name, &coreapi.ClusterUpgrade{Version: test.version, SingleNodeMaintenance: test.maintenance})
		Expect(apierror.CodeForError(err)).To(Equal(int32(http.StatusBadRequest)), "upgrade to %s", test.version)
		Expect(err.Error()).To(ContainSubstring(test.reason))
		ops, err := client.ListOperations(ctx, kc.OperationListOptions{FieldSelector: "spec.targetRef.uid=" + string(target.UID)})
		Expect(err).NotTo(HaveOccurred())
		for _, op := range ops.Items {
			Expect(op.Spec.Action).NotTo(Equal(corev1.OperationUpgradeCluster), "rejected upgrade to %s created an operation", test.version)
		}
	}
	actual, err := client.DescribeCluster(ctx, target.Name)
	Expect(err).NotTo(HaveOccurred())
	Expect(actual.Items).To(HaveLen(1))
	Expect(actual.Items[0].UID).To(Equal(target.UID))
	Expect(actual.Items[0].KubernetesVersion).To(Equal(target.KubernetesVersion))
	Expect(actual.Items[0].Status.Phase).To(Equal(corev1.ClusterRunning))
	Expect(actual.Items[0].Status.PackagePlan).NotTo(BeNil())
	Expect(actual.Items[0].Status.PackagePlan.Raw).To(MatchJSON(target.Status.PackagePlan.Raw))
	checkOCINodes(ctx, k8s, nodes, target.Masters.GetNodeIDs(), target.KubernetesVersion, target.ContainerRuntime.Version)
	Expect(probe.readyPod(ctx, k8s, "probe-0").UID).To(Equal(pod.UID))
	probe.check(ctx, k8s, config, "before")
}

func checkOCIWorkerLifecycle(ctx context.Context, client *kc.Client, k8s *kubernetes.Clientset, config *rest.Config, target *corev1.Cluster, nodes map[string]corev1.Node, probe ociWorkload, _ func(func())) {
	id := strings.TrimSpace(os.Getenv("KC_E2E_SPARE_WORKER"))
	Expect(nodes).NotTo(HaveKey(id), "spare Worker must be outside the initial topology")
	result, err := client.DescribeNode(ctx, id)
	Expect(err).NotTo(HaveOccurred())
	Expect(result.Items).To(HaveLen(1))
	spare := result.Items[0]
	host := spare.Status.NodeInfo.Hostname
	Expect(host).NotTo(BeEmpty())
	Expect(spare.Labels[common.LabelNodeRole]).To(BeEmpty())
	Expect(spare.Labels[common.LabelClusterName]).To(BeEmpty())
	Expect(spare.Labels[common.LabelNodeDisable]).To(BeEmpty())
	for _, assigned := range nodes {
		Expect(host).NotTo(Equal(assigned.Status.NodeInfo.Hostname))
	}
	Expect(target.Status.PackagePlan).NotTo(BeNil())

	timeout := framework.NewTimeoutContextWithDefaults()
	assertTargetTask := func(op *operations.Operation) {
		tasks, err := client.ListOperationTasks(ctx, string(op.UID))
		Expect(err).NotTo(HaveOccurred())
		Expect(slices.ContainsFunc(tasks.Items, func(task operations.OperationTask) bool {
			return task.Spec.NodeRef.Name == id && task.Spec.NodeRef.UID == spare.UID && task.Status.Phase == operations.TaskSucceeded
		})).To(BeTrue(), "the successful operation must include successful work on the selected Agent")
	}
	ginkgo.By("[2.2-01] Adding the spare Agent as a Worker through the candidate API")
	_, err = client.AddOrRemoveNode(ctx, initPatchNode(clusteroperation.NodesOperationAdd, id), target.Name)
	Expect(err).NotTo(HaveOccurred())
	assertTargetTask(waitForOCIOperation(ctx, client, target.UID, corev1.OperationAddNodes, "", timeout.ClusterInstall))
	waitForOCIClusterRunning(ctx, client, target.Name, target.KubernetesVersion, timeout.CommonTimeout)
	latest, err := client.DescribeCluster(ctx, target.Name)
	Expect(err).NotTo(HaveOccurred())
	Expect(latest.Items).To(HaveLen(1))
	current := latest.Items[0]
	Expect(current.UID).To(Equal(target.UID))
	Expect(current.Workers.GetNodeIDs()).To(ConsistOf(append(target.Workers.GetNodeIDs(), id)))
	Expect(current.Status.PackagePlan).NotTo(BeNil())
	Expect(current.Status.PackagePlan.Raw).To(MatchJSON(target.Status.PackagePlan.Raw), "expansion must preserve the stored plan")

	nodes[id] = spare
	checkOCINodes(ctx, k8s, nodes, target.Masters.GetNodeIDs(), target.KubernetesVersion, target.ContainerRuntime.Version)
	checkOCICalico(ctx, k8s, target.CNI.Namespace, target.CNI.Version, len(nodes))
	name := probe.add(ctx, k8s, os.Getenv("KC_E2E_PROBE_IMAGE"), host, false)
	pod := probe.readyPod(ctx, k8s, name)
	probe.check(ctx, k8s, config, "before")

	ginkgo.By("[2.2-02] Removing the Worker with an unprotected workload still on it")
	_, err = client.AddOrRemoveNode(ctx, initPatchNode(clusteroperation.NodesOperationRemove, id), target.Name)
	Expect(err).NotTo(HaveOccurred())
	assertTargetTask(waitForOCIOperation(ctx, client, target.UID, corev1.OperationRemoveNodes, "", timeout.ClusterDelete))
	waitForOCIClusterRunning(ctx, client, target.Name, target.KubernetesVersion, timeout.CommonTimeout)
	Eventually(func() error {
		agent, err := client.DescribeNode(ctx, id)
		if err != nil {
			return err
		}
		if len(agent.Items) != 1 || agent.Items[0].UID != spare.UID ||
			agent.Items[0].Labels[common.LabelClusterName] != "" || agent.Items[0].Labels[common.LabelNodeRole] != "" {
			return fmt.Errorf("Agent %s was replaced or was not released", id)
		}
		_, err = k8s.CoreV1().Nodes().Get(ctx, host, metav1.GetOptions{})
		if !kubeerrors.IsNotFound(err) {
			return fmt.Errorf("expected removed Kubernetes node %s to be absent, got %v", host, err)
		}
		_, err = k8s.CoreV1().Pods(probe.namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if !kubeerrors.IsNotFound(err) {
			return fmt.Errorf("expected evicted Pod %s to be absent, got %v", pod.Name, err)
		}
		return nil
	}, timeout.CommonTimeout, 2*time.Second).Should(Succeed())

	latest, err = client.DescribeCluster(ctx, target.Name)
	Expect(err).NotTo(HaveOccurred())
	Expect(latest.Items).To(HaveLen(1))
	Expect(latest.Items[0].UID).To(Equal(target.UID))
	Expect(latest.Items[0].Workers.GetNodeIDs()).To(Equal(target.Workers.GetNodeIDs()))
	Expect(k8s.AppsV1().Deployments(probe.namespace).Delete(ctx, name, metav1.DeleteOptions{})).To(Succeed())
	delete(nodes, id)
	delete(probe.hosts, name)
	probe.check(ctx, k8s, config, "before")
}

func checkOCICertificates(ctx context.Context, client *kc.Client, k8s *kubernetes.Clientset, config *rest.Config, target *corev1.Cluster, nodes map[string]corev1.Node, probe ociWorkload, _ func(func())) {
	endpoint, err := url.Parse(config.Host)
	Expect(err).NotTo(HaveOccurred())
	Expect(endpoint.Scheme).To(Equal("https"))
	port := endpoint.Port()
	if port == "" {
		port = "443"
	}
	address := net.JoinHostPort(endpoint.Hostname(), port)
	caFile := os.Getenv("KC_E2E_CONTROL_PLANE_CA_FILE")
	Expect(caFile).NotTo(BeEmpty(), "set KC_E2E_CONTROL_PLANE_CA_FILE to the test control plane's public CA file")
	ca, err := os.ReadFile(caFile)
	Expect(err).NotTo(HaveOccurred())
	roots := x509.NewCertPool()
	Expect(roots.AppendCertsFromPEM(ca)).To(BeTrue(), "the control plane CA must contain a PEM certificate")
	dialer := tls.Dialer{
		NetDialer: &net.Dialer{Timeout: 10 * time.Second},
		Config:    &tls.Config{ServerName: endpoint.Hostname(), RootCAs: roots, MinVersion: tls.VersionTLS12},
	}
	peer := func() (*x509.Certificate, error) {
		conn, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		state := conn.(*tls.Conn).ConnectionState()
		if len(state.VerifiedChains) == 0 {
			return nil, fmt.Errorf("API server did not present a verifiable certificate chain")
		}
		return state.VerifiedChains[0][0], nil
	}

	ginkgo.By("[2.3-04] Comparing the live TLS endpoint before and after certificate renewal")
	before, err := peer()
	Expect(err).NotTo(HaveOccurred())
	_, err = client.UpdateCert(ctx, target.Name)
	Expect(err).NotTo(HaveOccurred())
	timeout := framework.NewTimeoutContextWithDefaults()
	waitForOCIOperation(ctx, client, target.UID, corev1.OperationUpdateCertification, "", timeout.ClusterInstall)
	waitForOCIClusterRunning(ctx, client, target.Name, target.KubernetesVersion, timeout.CommonTimeout)
	Eventually(func() error {
		after, err := peer()
		if err != nil {
			return err
		}
		if after.SerialNumber.Cmp(before.SerialNumber) == 0 || !after.NotAfter.After(before.NotAfter) {
			return fmt.Errorf("API server is not yet serving the renewed leaf certificate")
		}
		fmt.Fprintf(ginkgo.GinkgoWriter, "verified new leaf with the original CA: expiration %s -> %s\n", before.NotAfter.UTC(), after.NotAfter.UTC())
		return nil
	}, timeout.CommonTimeout, 2*time.Second).Should(Succeed())
	checkOCINodes(ctx, k8s, nodes, target.Masters.GetNodeIDs(), target.KubernetesVersion, target.ContainerRuntime.Version)
	probe.check(ctx, k8s, config, "before")
}
