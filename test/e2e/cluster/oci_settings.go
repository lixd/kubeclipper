package cluster

import (
	"context"
	"fmt"
	"net/http"
	"net/netip"
	"os/exec"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	apierror "github.com/kubeclipper/kubeclipper/pkg/errors"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

const (
	ociCustomPodSubnet     = "172.23.0.0/16"
	ociCustomServiceSubnet = "172.24.0.0/16"
	ociCustomDNSDomain     = "oci.cluster.test"
)

func checkOCIDisabledAdmission(ctx context.Context, client *kc.Client, node corev1.Node, name string, args []string) {
	ginkgo.By("[2.2-05] Trying to create on a disabled Agent and enabling the same Agent for the real cluster")
	path := client.Scheme() + "://" + client.Host() + "/api/core.kubeclipper.io/v1/nodes/" + node.Name
	toggle := func(ctx context.Context, state string) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPatch, path+"/"+state, nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+client.Token())
		req.Header.Set("Content-Type", "application/json")
		res, err := client.HTTPClient().Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("node %s returned HTTP %d", state, res.StatusCode)
		}
		return nil
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		result, err := client.DescribeNode(cleanup, node.Name)
		Expect(err).NotTo(HaveOccurred())
		if result.Items[0].Labels[common.LabelNodeDisable] == "true" {
			Expect(toggle(cleanup, "enable")).To(Succeed())
		}
	}()
	Expect(toggle(ctx, "disable")).To(Succeed())
	result, err := client.DescribeNode(ctx, node.Name)
	Expect(err).NotTo(HaveOccurred())
	Expect(result.Items[0].UID).To(Equal(node.UID))
	Expect(result.Items[0].Labels).To(HaveKeyWithValue(common.LabelNodeDisable, "true"))
	call, done := context.WithTimeout(ctx, time.Minute)
	defer done()
	output, err := exec.CommandContext(call, "kcctl", args...).CombinedOutput()
	Expect(err).To(BeAssignableToTypeOf(&exec.ExitError{}))
	Expect(string(output)).To(ContainSubstring("disabled"))
	_, err = client.DescribeCluster(ctx, name)
	Expect(apierror.IsNotFound(err)).To(BeTrue())
	operations, err := client.ListOperations(ctx, kc.OperationListOptions{LabelSelector: common.LabelClusterName + "=" + name})
	Expect(err).NotTo(HaveOccurred())
	Expect(operations.Items).To(BeEmpty())
	Expect(toggle(ctx, "enable")).To(Succeed())
	result, err = client.DescribeNode(ctx, node.Name)
	Expect(err).NotTo(HaveOccurred())
	Expect(result.Items[0].UID).To(Equal(node.UID))
	Expect(result.Items[0].Labels).NotTo(HaveKey(common.LabelNodeDisable))
}

func checkOCIClusterSettings(ctx context.Context, client *kc.Client, k8s *kubernetes.Clientset, config *rest.Config, target *corev1.Cluster, _ map[string]corev1.Node, probe ociWorkload, _ func(func())) {
	ginkgo.By("[2.1-11] Verifying live Pod and Service addresses and the configured DNS domain")
	Expect(target.Networking.Pods.CIDRBlocks).To(Equal([]string{ociCustomPodSubnet}))
	Expect(target.Networking.Services.CIDRBlocks).To(Equal([]string{ociCustomServiceSubnet}))
	Expect(target.Networking.DNSDomain).To(Equal(ociCustomDNSDomain))
	pod := probe.readyPod(ctx, k8s, "probe-0")
	podIP, err := netip.ParseAddr(pod.Status.PodIP)
	Expect(err).NotTo(HaveOccurred())
	Expect(netip.MustParsePrefix(ociCustomPodSubnet).Contains(podIP)).To(BeTrue())
	service, err := k8s.CoreV1().Services(probe.namespace).Get(ctx, "echo", metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())
	serviceIP, err := netip.ParseAddr(service.Spec.ClusterIP)
	Expect(err).NotTo(HaveOccurred())
	Expect(netip.MustParsePrefix(ociCustomServiceSubnet).Contains(serviceIP)).To(BeTrue())

	ginkgo.By("[3-14] Setting and clearing individual external endpoint properties without changing the cluster plan")
	get := func() corev1.Cluster {
		result, err := client.DescribeCluster(ctx, target.Name)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.Items).To(HaveLen(1))
		value := result.Items[0]
		Expect(value.UID).To(Equal(target.UID))
		return value
	}
	runOCICommand(ctx, "set", "cluster", target.Name, "--external-ip", "192.0.2.8", "--external-domain", "api.example.test",
		"--external-port", "16443", "--external-domain-port", "26443")
	saved := get()
	Expect(saved.Labels).To(HaveKeyWithValue(common.LabelExternalIP, "192.0.2.8"))
	Expect(saved.Labels).To(HaveKeyWithValue(common.LabelExternalDomain, "api.example.test"))
	Expect(saved.Labels).To(HaveKeyWithValue(common.LabelExternalPort, "16443"))
	Expect(saved.Labels).To(HaveKeyWithValue(common.LabelExternalDomainPort, "26443"))
	called, done := context.WithTimeout(ctx, time.Minute)
	defer done()
	output, err := exec.CommandContext(called, "kcctl", "set", "cluster", target.Name, "--external-port", "65536").CombinedOutput()
	Expect(err).To(BeAssignableToTypeOf(&exec.ExitError{}))
	Expect(string(output)).To(ContainSubstring("invalid external port"))
	Expect(get().Labels).To(Equal(saved.Labels))
	runOCICommand(ctx, "set", "cluster", target.Name, "--external-ip=")
	current := get()
	Expect(current.Labels).NotTo(HaveKey(common.LabelExternalIP))
	Expect(current.Labels).To(HaveKeyWithValue(common.LabelExternalDomain, "api.example.test"))
	Expect(current.Labels).To(HaveKeyWithValue(common.LabelExternalPort, "16443"))
	runOCICommand(ctx, "set", "cluster", target.Name, "--external-domain=", "--external-port=", "--external-domain-port=")
	current = get()
	for _, key := range []string{common.LabelExternalIP, common.LabelExternalDomain, common.LabelExternalPort, common.LabelExternalDomainPort} {
		Expect(current.Labels).NotTo(HaveKey(key))
	}
	Expect(current.Status.PackagePlan).NotTo(BeNil())
	Expect(current.Status.PackagePlan.Raw).To(MatchJSON(target.Status.PackagePlan.Raw))
	probe.check(ctx, k8s, config, "before")
	checkOCIPodExec(ctx, client, k8s, target.Name, probe.namespace)
}
