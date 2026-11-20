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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	kubev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	delivery "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	apierror "github.com/kubeclipper/kubeclipper/pkg/errors"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
	"github.com/kubeclipper/kubeclipper/test/framework"
	"github.com/kubeclipper/kubeclipper/test/framework/cluster"
)

var (
	_ = registerOCILifecycle("[OCI-Lifecycle]", "creates, probes and deletes a cluster on the assigned nodes", nil)
	_ = registerOCILifecycle("[OCI-Worker]", "[2.2-01, 2.2-02, 2.2-08] adds and removes a Worker while preserving the running cluster", checkOCIWorkerLifecycle)
	_ = registerOCILifecycle("[OCI-Certificates]", "[2.3-04] renews the certificate served by the control plane", checkOCICertificates)
	_ = registerOCILifecycle("[OCI-BackupReplay]", "[5-13] replays concurrent backup requests against one real FS snapshot", checkOCIBackupReplay)
	_ = registerOCILifecycle("[OCI-UpgradeAdmission]", "[2.3-08, 3-11] rejects invalid upgrades and reads the original operation through kcctl", checkOCIUpgradeAdmission)
	_ = registerOCILifecycle("[OCI-ClusterAccess]", "[4-12, 4-08b] authorizes real cluster access and rejects the viewer's downloads and mutation", checkOCIClusterAccess)
	_ = registerOCILifecycle("[OCI-BackupDeleteRetry]", "[2.5-04, 2.5-10] reports FS backup and deletion failures and retries after repair", checkOCIBackupDeleteRetry)
	_ = registerOCILifecycle("[OCI-CronBackup]", "[2.5-05, 2.5-06, 2.5-07, 2.5-08] schedules real FS snapshots, pauses, resumes and rotates them", checkOCICronBackup)
	_ = registerOCILifecycle("[OCI-ClusterSettings]", "[2.1-11, 2.2-05, 3-14, 4-12b] verifies node admission, custom networking, endpoint settings and Pod exec", checkOCIClusterSettings)
	_ = registerOCILifecycle("[OCI-DNSSync]", "[4-10] propagates and removes one domain in CoreDNS while preserving another", checkOCIDNSSync)
)

type ociScenario func(context.Context, *kc.Client, *kubernetes.Clientset, *rest.Config, *corev1.Cluster, map[string]corev1.Node, ociWorkload, func(func()))

func registerOCILifecycle(label, description string, scenario ociScenario) bool {
	return ginkgo.Describe(label+" [Serial]", func() {
		describeOCILifecycle(label, description, scenario)
	})
}

func describeOCILifecycle(label, description string, scenario ociScenario) {
	f := framework.NewDefaultFramework("oci-lifecycle")
	var backupCleanup, clusterCleanup func()
	var clusterCleanupHandle framework.CleanupActionHandle
	ginkgo.JustAfterEach(func() {
		if backupCleanup != nil {
			cleanup := backupCleanup
			backupCleanup = nil
			cleanup()
		}
	})
	ginkgo.AfterEach(func() {
		if clusterCleanup != nil {
			cleanup := clusterCleanup
			clusterCleanup = nil
			framework.RemoveCleanupAction(clusterCleanupHandle)
			cleanup()
		}
	})

	ginkgo.It(description, func() {
		masterIDs := strings.Fields(os.Getenv("KC_E2E_MASTERS"))
		workerIDs := strings.Fields(os.Getenv("KC_E2E_WORKERS"))
		startVersion := os.Getenv("KC_E2E_KUBERNETES_VERSION")
		image := os.Getenv("KC_E2E_PROBE_IMAGE")
		revision := os.Getenv("KC_E2E_BACKEND_REVISION")
		Expect(masterIDs).To(Or(HaveLen(1), HaveLen(3)), "set KC_E2E_MASTERS to the assigned Agent IDs")
		Expect(startVersion).NotTo(BeEmpty(), "set KC_E2E_KUBERNETES_VERSION")
		Expect(image).NotTo(BeEmpty(), "set KC_E2E_PROBE_IMAGE to a BusyBox image the test nodes can pull")
		Expect(revision).To(MatchRegexp("^[0-9a-f]{40}$"), "set KC_E2E_BACKEND_REVISION to the candidate commit")
		if label == "[OCI-Worker]" {
			Expect(strings.Fields(os.Getenv("KC_E2E_SPARE_WORKER"))).To(HaveLen(1), "assign one unused Agent via KC_E2E_SPARE_WORKER")
		}
		if label == "[OCI-Certificates]" || label == "[OCI-BackupReplay]" || label == "[OCI-BackupDeleteRetry]" || label == "[OCI-CronBackup]" {
			Expect(masterIDs).To(HaveLen(1))
			Expect(workerIDs).To(BeEmpty(), "run this scenario on the AIO fixture")
		}
		if label == "[OCI-UpgradeAdmission]" {
			Expect(masterIDs).To(HaveLen(1))
			Expect(workerIDs).To(BeEmpty(), "run upgrade admission on the AIO fixture")
			Expect(startVersion).To(Equal("v1.36.4"), "this scenario checks the release's v1.36.4 version boundaries")
		}
		if label == "[OCI-ClusterSettings]" {
			Expect(masterIDs).To(HaveLen(1))
			Expect(workerIDs).To(BeEmpty(), "run cluster settings on the AIO fixture")
		}

		timeout := framework.NewTimeoutContextWithDefaults()
		ctx, cancel := context.WithTimeout(context.Background(), 3*timeout.ClusterInstall+2*timeout.ClusterDelete)
		defer cancel()

		version, err := f.Client.Version(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(version.GitCommit).To(Equal(revision))

		ids := append(append([]string{}, masterIDs...), workerIDs...)
		nodes := make(map[string]corev1.Node, len(ids))
		for _, id := range ids {
			Expect(nodes).NotTo(HaveKey(id), "each assigned Agent must appear once")
			result, err := f.Client.DescribeNode(ctx, id)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Items).To(HaveLen(1))
			node := result.Items[0]
			Expect(node.Labels[common.LabelNodeRole]).To(BeEmpty(), "Agent %s is already assigned", id)
			Expect(node.Labels[common.LabelClusterName]).To(BeEmpty(), "Agent %s is already assigned", id)
			Expect(node.Labels[common.LabelNodeDisable]).To(BeEmpty(), "Agent %s is disabled", id)
			Expect(node.Status.NodeInfo.Hostname).NotTo(BeEmpty())
			Expect(slices.ContainsFunc(node.Status.Conditions, func(condition corev1.NodeCondition) bool {
				return condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue
			})).To(BeTrue(), "Agent %s is not Ready", id)
			nodes[id] = node
		}

		name := "oci-e2e-" + string(uuid.NewUUID())[:12]
		fmt.Fprintf(ginkgo.GinkgoWriter, "cluster=%s masters=%v workers=%v kubernetes=%s\n", name, masterIDs, workerIDs, startVersion)
		_, err = f.Client.DescribeCluster(ctx, name)
		Expect(apierror.IsNotFound(err)).To(BeTrue(), "the generated test cluster name must be unused")

		// Register cleanup before submission so an interrupted CLI response cannot leave a cluster behind.
		clusterCleanup = func() {
			cleanupCtx, done := context.WithTimeout(context.Background(), timeout.ClusterInstall+timeout.ClusterDelete)
			defer done()
			Expect(cleanupOCICluster(cleanupCtx, f.Client, name, timeout.ClusterInstall+timeout.ClusterDelete)).
				To(Succeed(), "cleanup of %s", name)
		}
		clusterCleanupHandle = framework.AddCleanupAction(clusterCleanup)

		aio := len(masterIDs) == 1 && len(workerIDs) == 0
		args := []string{"create", "cluster", "--name", name, "--master", strings.Join(masterIDs, ","),
			"--k8s-version", startVersion, "--cri", corev1.CRIContainerd, "--cni", "calico",
			"--calico.net-mode", "Overlay-Vxlan-All"}
		if len(workerIDs) != 0 {
			args = append(args, "--worker", strings.Join(workerIDs, ","))
		}
		if aio {
			args = append(args, "--untaint-master")
		}
		if registry := os.Getenv("KC_E2E_IMAGE_REGISTRY"); registry != "" {
			args = append(args, "--image-registry", registry)
		}
		ipDetection := os.Getenv("KC_E2E_CALICO_IPV4_DETECTION")
		if ipDetection != "" {
			args = append(args, "--calico.ipv4-auto-detection", ipDetection)
		}
		if label == "[OCI-ClusterSettings]" {
			args = append(args, "--pod-subnet", ociCustomPodSubnet, "--service-subnet", ociCustomServiceSubnet, "--cluster-dns-domain", ociCustomDNSDomain)
			checkOCIDisabledAdmission(ctx, f.Client, nodes[masterIDs[0]], name, args)
		}
		ginkgo.By("[3-05] Submitting the OCI cluster through kcctl")
		runOCICommand(ctx, args...)
		created, err := f.Client.DescribeCluster(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(created.Items).To(HaveLen(1))
		target := created.Items[0]
		Expect(target.UID).NotTo(BeEmpty())
		Expect(target.Masters.GetNodeIDs()).To(Equal(masterIDs))
		Expect(target.Workers.GetNodeIDs()).To(ConsistOf(workerIDs))
		Expect(target.KubernetesVersion).To(Equal(startVersion))
		Expect(target.ContainerRuntime.Type).To(Equal(corev1.CRIContainerd))
		Expect(target.CNI.Calico).NotTo(BeNil())
		Expect(target.CNI.Calico.Mode).To(Equal("Overlay-Vxlan-All"))
		if ipDetection != "" {
			Expect(target.CNI.Calico.IPv4AutoDetection).To(Equal(ipDetection))
		}
		if registry := os.Getenv("KC_E2E_IMAGE_REGISTRY"); registry != "" {
			Expect(target.ImageRegistry).To(Equal(registry))
		}
		ginkgo.By("[2.6-01] Checking the persisted OCI plan")
		checkOCIPlan(&target, nodes, startVersion)

		ginkgo.By("[5-01] Observing the new cluster's Operation and Tasks")
		waitForOCIOperation(ctx, f.Client, target.UID, corev1.OperationCreateCluster, "", timeout.ClusterInstall)
		waitForOCIClusterRunning(ctx, f.Client, name, startVersion, timeout.ClusterInstallShort)

		ginkgo.By("[2.1-02, 2.1-24, 2.1-31] Reading the live Kubernetes API and probing workload networking")
		k8s, config := ociKubernetesClient(ctx, f.Client, name, timeout.CommonTimeout)
		checkOCINodes(ctx, k8s, nodes, masterIDs, startVersion, target.ContainerRuntime.Version)
		ginkgo.By("[2.1-04] Checking the installed Calico DaemonSet against the selected version")
		checkOCICalico(ctx, k8s, target.CNI.Namespace, target.CNI.Version, len(nodes))
		probe := startOCIWorkload(ctx, k8s, name, image, target.Networking.DNSDomain, nodes, masterIDs[0], workerIDs)
		probe.check(ctx, k8s, config, "before")
		fmt.Fprintf(ginkgo.GinkgoWriter, "verified kubeconfig, Ready nodes, workload, Pod IP and Service DNS for %s\n", name)

		ginkgo.By("[4-12] Reading the real workload marker through the platform proxy")
		proxyErr := checkOCIProxy(ctx, f.Client, name, config.Host)
		if proxyErr != nil {
			fmt.Fprintf(ginkgo.GinkgoWriter, "[4-12] proxy kubeconfig: %v\n", proxyErr)
		}

		if scenario != nil {
			scenario(ctx, f.Client, k8s, config, &target, nodes, probe, func(cleanup func()) {
				backupCleanup = cleanup
			})
		} else if upgradeVersion := os.Getenv("KC_E2E_UPGRADE_VERSION"); upgradeVersion != "" {
			Expect(aio).To(BeTrue(), "only the single-node maintenance path is exercised by this upgrade check")
			upgradeOCICluster(ctx, f.Client, k8s, config, &target, probe, nodes, masterIDs, upgradeVersion, timeout.ClusterInstall)
		}
		if scenario == nil && os.Getenv("KC_E2E_FS_BACKUP") == "1" {
			Expect(aio).To(BeTrue(), "FS snapshot verification runs on the AIO Agent")
			checkOCIFSRecovery(ctx, f.Client, k8s, config, &target, nodes[masterIDs[0]], probe, ociBackupRestore, func(cleanup func()) {
				backupCleanup = cleanup
			})
		}

		ginkgo.By("[3-08] Submitting normal deletion and verifying the target Agents are released")
		selector := kc.OperationListOptions{FieldSelector: "spec.targetRef.uid=" + string(target.UID)}
		priorOperations, err := f.Client.ListOperations(ctx, selector)
		Expect(err).NotTo(HaveOccurred())
		Expect(priorOperations.Items).NotTo(BeEmpty())
		deletion := deleteOCICluster(ctx, f.Client, &target, timeout.ClusterDelete)
		Expect(cluster.WaitForClusterNotFound(f.Client, name, timeout.ClusterDelete)).To(Succeed())
		for _, id := range ids {
			id := id
			Eventually(func() error {
				result, err := f.Client.DescribeNode(ctx, id)
				if err != nil {
					return err
				}
				node := result.Items[0]
				if node.UID != nodes[id].UID || node.Labels[common.LabelNodeRole] != "" || node.Labels[common.LabelClusterName] != "" {
					return fmt.Errorf("Agent %s was replaced or still assigned", id)
				}
				return nil
			}, timeout.ClusterDelete, 2*time.Second).Should(Succeed(), "Agent %s was not released", id)
		}
		ginkgo.By("[2.4-05] Checking that scoped Operations and their Tasks were finalized")
		Eventually(func() error {
			remaining, err := f.Client.ListOperations(ctx, selector)
			if err != nil {
				return err
			}
			if len(remaining.Items) != 0 {
				return fmt.Errorf("%d Operations remain for deleted cluster UID %s", len(remaining.Items), target.UID)
			}
			for _, op := range append(priorOperations.Items, deletion) {
				tasks, err := f.Client.ListOperationTasks(ctx, string(op.UID))
				if err != nil {
					return err
				}
				if len(tasks.Items) != 0 {
					return fmt.Errorf("%d Tasks remain for deleted Operation UID %s", len(tasks.Items), op.UID)
				}
			}
			return nil
		}, timeout.CommonTimeout, 2*time.Second).Should(Succeed())
		// Independent lifecycle checks and ordinary deletion should still run if the proxy fails.
		Expect(proxyErr).NotTo(HaveOccurred(), "[4-12] proxy kubeconfig did not reach the workload")
	})
}

func runOCICommand(ctx context.Context, args ...string) string {
	callCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	output, err := exec.CommandContext(callCtx, "kcctl", args...).CombinedOutput()
	Expect(err).NotTo(HaveOccurred(), "kcctl %s: %s", strings.Join(args, " "), output)
	return string(output)
}

func cleanupOCICluster(ctx context.Context, client *kc.Client, name string, timeout time.Duration) error {
	submitted := false
	return wait.PollImmediateWithContext(ctx, 2*time.Second, timeout, func(ctx context.Context) (bool, error) {
		result, err := client.DescribeCluster(ctx, name)
		if apierror.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		switch result.Items[0].Status.Phase {
		case corev1.ClusterInstalling, corev1.ClusterUpdating, corev1.ClusterUpgrading, corev1.ClusterBackingUp, corev1.ClusterRestoring:
			return false, nil
		case corev1.ClusterTerminateFailed:
			if submitted {
				return false, fmt.Errorf("normal deletion of %s entered TerminateFailed", name)
			}
		}
		if !submitted {
			if err := client.DeleteClusterWithQuery(ctx, name, nil); err != nil && !apierror.IsNotFound(err) {
				return false, err
			}
			submitted = true
		}
		return false, nil
	})
}

func waitForOCIClusterRunning(ctx context.Context, client *kc.Client, name, version string, timeout time.Duration) {
	err := wait.PollImmediateWithContext(ctx, 2*time.Second, timeout, func(ctx context.Context) (bool, error) {
		result, err := client.DescribeCluster(ctx, name)
		if err != nil {
			return false, err
		}
		cluster := result.Items[0]
		if strings.HasSuffix(string(cluster.Status.Phase), "Failed") {
			return false, fmt.Errorf("cluster %s entered %s", name, cluster.Status.Phase)
		}
		return cluster.Status.Phase == corev1.ClusterRunning && cluster.KubernetesVersion == version, nil
	})
	Expect(err).NotTo(HaveOccurred(), "waiting for %s to be Running on %s", name, version)
}

func deleteOCICluster(ctx context.Context, client *kc.Client, target *corev1.Cluster, timeout time.Duration) operations.Operation {
	watchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// The cluster finalizer removes its Operations. Subscribe before submitting
	// deletion to observe the terminal event even when finalization is fast.
	stream, err := client.StreamList(watchCtx, "/api/operations.kubeclipper.io/v1alpha1/operations", kc.Queries{
		Watch:         true,
		FieldSelector: "spec.targetRef.uid=" + string(target.UID),
	})
	Expect(err).NotTo(HaveOccurred())
	defer stream.Close()
	Expect(runOCICommand(ctx, "delete", "cluster", target.Name)).To(ContainSubstring("deletion requested"))
	decoder := json.NewDecoder(stream)
	for {
		var event struct {
			Type   string               `json:"type"`
			Object operations.Operation `json:"object"`
		}
		Expect(decoder.Decode(&event)).To(Succeed(), "waiting for the DeleteCluster watch event")
		Expect(event.Type).NotTo(Equal("ERROR"), "the Operation watch returned an error")
		op := event.Object
		if op.Spec.Action != corev1.OperationDeleteCluster || !op.Status.Phase.IsTerminal() {
			continue
		}
		Expect(op.Spec.TargetRef.UID).To(Equal(target.UID))
		Expect(op.UID).NotTo(BeEmpty())
		fmt.Fprintf(ginkgo.GinkgoWriter, "action=%s operation=%s operationUID=%s clusterUID=%s phase=%s\n",
			op.Spec.Action, op.Name, op.UID, target.UID, op.Status.Phase)
		Expect(op.Status.Phase).To(Equal(operations.OperationSucceeded), "%s", op.Status.Reason)
		return op
	}
}

func waitForOCIOperation(ctx context.Context, client *kc.Client, targetUID types.UID, action, name string, timeout time.Duration) *operations.Operation {
	selector := "spec.targetRef.uid=" + string(targetUID)
	var current *operations.Operation
	err := wait.PollImmediateWithContext(ctx, 2*time.Second, timeout, func(ctx context.Context) (bool, error) {
		result, err := client.ListOperations(ctx, kc.OperationListOptions{FieldSelector: selector})
		if err != nil {
			return false, err
		}
		for i := range result.Items {
			op := &result.Items[i]
			if op.Spec.Action != action || (name != "" && op.Name != name) {
				continue
			}
			if current != nil && current.UID != op.UID {
				return false, fmt.Errorf("ambiguous %s for cluster UID %s", action, targetUID)
			}
			current = op
		}
		return current != nil && current.Status.Phase.IsTerminal(), nil
	})
	Expect(err).NotTo(HaveOccurred(), "waiting for %s on cluster UID %s", action, targetUID)
	Expect(current).NotTo(BeNil())
	Expect(current.UID).NotTo(BeEmpty())
	Expect(current.Spec.TargetRef.UID).To(Equal(targetUID))
	tasks, err := client.ListOperationTasks(ctx, string(current.UID))
	Expect(err).NotTo(HaveOccurred())
	Expect(tasks.Items).NotTo(BeEmpty())
	fmt.Fprintf(ginkgo.GinkgoWriter, "action=%s operation=%s operationUID=%s clusterUID=%s phase=%s\n",
		action, current.Name, current.UID, targetUID, current.Status.Phase)
	for _, task := range tasks.Items {
		Expect(task.Spec.OperationRef.UID).To(Equal(current.UID))
		Expect(task.Spec.NodeRef.UID).NotTo(BeEmpty())
		Expect(task.Spec.StepID).NotTo(BeEmpty())
		Expect(task.Status.Phase.IsTerminal()).To(BeTrue())
		fmt.Fprintf(ginkgo.GinkgoWriter, "taskUID=%s step=%s node=%s attempt=%d phase=%s\n",
			task.UID, task.Spec.StepID, task.Spec.NodeRef.Name, task.Spec.Attempt, task.Status.Phase)
	}
	Expect(current.Status.Phase).To(Equal(operations.OperationSucceeded), "%s: %s", action, current.Status.Reason)
	return current
}

func checkOCIPlan(target *corev1.Cluster, nodes map[string]corev1.Node, version string) {
	plan, err := delivery.DecodeResolvedArtifactPlan(target.Status.PackagePlan)
	Expect(err).NotTo(HaveOccurred())
	Expect(plan).NotTo(BeNil(), "the admission path must persist its resolved OCI plan")
	Expect(plan.KubernetesVersion).To(Equal(version))
	for id, node := range nodes {
		Expect(plan.Arch).To(Equal(node.Labels[common.LabelArchStable]), "Agent %s architecture", id)
	}
	Expect(plan.Components).NotTo(BeEmpty())
	slots := make(map[string]string, len(plan.Components))
	for _, component := range plan.Components {
		Expect(component.Transport.Type).To(Equal(delivery.TransportOCI))
		Expect(component.Transport.Digest).To(MatchRegexp("^sha256:[0-9a-f]{64}$"))
		ref, digest, qualified := strings.Cut(component.Transport.Ref, "@")
		Expect(ref).NotTo(BeEmpty())
		if qualified {
			Expect(digest).To(Equal(component.Transport.Digest))
		}
		slots[component.Slot] = component.Version
		fmt.Fprintf(ginkgo.GinkgoWriter, "slot=%s version=%s digest=%s\n", component.Slot, component.Version, component.Transport.Digest)
	}
	Expect(slots).To(HaveKeyWithValue("k8s", version))
	Expect(slots).To(HaveKeyWithValue("cri", target.ContainerRuntime.Version))
	Expect(slots).To(HaveKeyWithValue("cni", target.CNI.Version))
}

func loadOCIKubeconfig(ctx context.Context, client *kc.Client, name string, proxy bool) (*rest.Config, error) {
	data, err := client.ClusterKubeconfig(ctx, name, proxy)
	if err != nil {
		return nil, err
	}
	config, err := clientcmd.RESTConfigFromKubeConfig(data)
	if err != nil {
		return nil, fmt.Errorf("parse returned kubeconfig: %w", err)
	}
	config.Timeout = 10 * time.Second
	return config, nil
}

func ociKubernetesClient(ctx context.Context, client *kc.Client, name string, timeout time.Duration) (*kubernetes.Clientset, *rest.Config) {
	var config *rest.Config
	err := wait.PollImmediateWithContext(ctx, 2*time.Second, timeout, func(ctx context.Context) (bool, error) {
		current, err := loadOCIKubeconfig(ctx, client, name, false)
		if apierror.CodeForError(err) == http.StatusServiceUnavailable {
			// The cluster controller publishes kubeconfig asynchronously after creation.
			return false, nil
		}
		config = current
		return err == nil, err
	})
	Expect(err).NotTo(HaveOccurred(), "waiting for kubeconfig for %s after cluster creation", name)
	k8s, err := kubernetes.NewForConfig(config)
	Expect(err).NotTo(HaveOccurred())
	return k8s, config
}

func checkOCIProxy(ctx context.Context, client *kc.Client, name, directURL string) error {
	config, err := loadOCIKubeconfig(ctx, client, name, true)
	if err != nil {
		return err
	}
	if config.Host == directURL {
		return fmt.Errorf("proxy and direct kubeconfig use the same API server URL")
	}
	proxy, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	marker, err := proxy.CoreV1().ConfigMaps(name).Get(ctx, "marker", metav1.GetOptions{})
	if err != nil {
		return err
	}
	if marker.Data["value"] != "before" {
		return fmt.Errorf("proxy read an unexpected workload marker")
	}
	return nil
}

func checkOCINodes(ctx context.Context, k8s *kubernetes.Clientset, agents map[string]corev1.Node, masterIDs []string, version, containerdVersion string) {
	containerd := "containerd://" + strings.TrimPrefix(containerdVersion, "v")
	Eventually(func() error {
		server, err := k8s.Discovery().ServerVersion()
		if err != nil {
			return err
		}
		if server.GitVersion != version {
			return fmt.Errorf("API server version = %s, expected %s", server.GitVersion, version)
		}
		if _, err := k8s.CoreV1().RESTClient().Get().AbsPath("/readyz").DoRaw(ctx); err != nil {
			return err
		}
		for _, name := range []string{"coredns", "kc-kubectl"} {
			deployment, err := k8s.AppsV1().Deployments("kube-system").Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if deployment.Status.AvailableReplicas == 0 || deployment.Status.AvailableReplicas != deployment.Status.Replicas {
				return fmt.Errorf("kube-system deployment %s has %d available of %d replicas", name, deployment.Status.AvailableReplicas, deployment.Status.Replicas)
			}
		}
		nodes, err := k8s.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return err
		}
		if len(nodes.Items) != len(agents) {
			return fmt.Errorf("registered %d of %d expected Kubernetes nodes", len(nodes.Items), len(agents))
		}
		for id, agent := range agents {
			node, err := k8s.CoreV1().Nodes().Get(ctx, agent.Status.NodeInfo.Hostname, metav1.GetOptions{})
			if err != nil {
				return err
			}
			if !slices.ContainsFunc(node.Status.Conditions, func(condition kubev1.NodeCondition) bool {
				return condition.Type == kubev1.NodeReady && condition.Status == kubev1.ConditionTrue
			}) {
				return fmt.Errorf("Kubernetes node %s is not Ready", node.Name)
			}
			if node.Status.NodeInfo.KubeletVersion != version {
				return fmt.Errorf("kubelet on %s reports %s, expected %s", node.Name, node.Status.NodeInfo.KubeletVersion, version)
			}
			if runtime := node.Status.NodeInfo.ContainerRuntimeVersion; runtime != containerd {
				return fmt.Errorf("container runtime on %s reports %s, expected %s", node.Name, runtime, containerd)
			}
			_, controlPlane := node.Labels["node-role.kubernetes.io/control-plane"]
			if controlPlane != slices.Contains(masterIDs, id) {
				return fmt.Errorf("control-plane label on %s does not match the assigned role", node.Name)
			}
		}
		return nil
	}, 4*time.Minute, 3*time.Second).Should(Succeed())
}

func checkOCICalico(ctx context.Context, k8s *kubernetes.Clientset, namespace, version string, nodeCount int) {
	Eventually(func() error {
		daemonSet, err := k8s.AppsV1().DaemonSets(namespace).Get(ctx, "calico-node", metav1.GetOptions{})
		if err != nil {
			return err
		}
		if !slices.ContainsFunc(daemonSet.Spec.Template.Spec.Containers, func(container kubev1.Container) bool {
			return container.Name == "calico-node" && strings.HasSuffix(container.Image, ":"+version)
		}) {
			return fmt.Errorf("the calico-node DaemonSet does not select Calico %s", version)
		}
		if int(daemonSet.Status.DesiredNumberScheduled) != nodeCount || int(daemonSet.Status.NumberReady) != nodeCount {
			return fmt.Errorf("Calico is Ready on %d of %d assigned nodes", daemonSet.Status.NumberReady, nodeCount)
		}
		return nil
	}, 4*time.Minute, 3*time.Second).Should(Succeed())
}
