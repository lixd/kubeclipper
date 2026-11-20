package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	kubev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"

	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

func checkOCIDNSSync(ctx context.Context, client *kc.Client, k8s *kubernetes.Clientset, config *rest.Config, target *corev1.Cluster, _ map[string]corev1.Node, probe ociWorkload, _ func(func())) {
	ginkgo.By("[4-10] Synchronizing two actual domains to CoreDNS and resolving them from a Pod")
	base := client.Scheme() + "://" + client.Host() + "/api/core.kubeclipper.io/v1/domains"
	request := func(ctx context.Context, method, path string, body any) (int, error) {
		var payload []byte
		if body != nil {
			var err error
			payload, err = json.Marshal(body)
			if err != nil {
				return 0, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, base+path, bytes.NewReader(payload))
		if err != nil {
			return 0, err
		}
		req.Header.Set("Authorization", "Bearer "+client.Token())
		req.Header.Set("Content-Type", "application/json")
		res, err := client.HTTPClient().Do(req)
		if err != nil {
			return 0, err
		}
		defer res.Body.Close()
		_, err = io.Copy(io.Discard, res.Body)
		return res.StatusCode, err
	}
	first := "selected." + target.Name + ".test"
	other := "control." + target.Name + ".test"
	for _, fixture := range []struct{ domain, ip string }{{first, "192.0.2.42"}, {other, "192.0.2.43"}} {
		domain := corev1.Domain{
			ObjectMeta: metav1.ObjectMeta{Name: fixture.domain},
			Spec:       corev1.DomainSpec{SyncCluster: []string{target.Name}},
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			status, err := request(cleanup, http.MethodDelete, "/"+fixture.domain, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(status).To(BeElementOf(http.StatusOK, http.StatusNotFound))
		}()
		status, err := request(ctx, http.MethodPost, "", &domain)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(http.StatusOK))
		record := corev1.Record{Domain: fixture.domain, RR: "api", ParseRecord: []corev1.ParseRecord{{Type: "A", IP: fixture.ip}}}
		status, err = request(ctx, http.MethodPost, "/"+fixture.domain+"/records", &record)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(http.StatusOK))
	}

	pod := probe.readyPod(ctx, k8s, "probe-0")
	resolve := func(domain string) (string, error) {
		check, cancel := context.WithTimeout(ctx, 8*time.Second)
		defer cancel()
		req := k8s.CoreV1().RESTClient().Post().Namespace(probe.namespace).Resource("pods").Name(pod.Name).SubResource("exec").
			VersionedParams(&kubev1.PodExecOptions{
				Container: "echo",
				Command:   []string{"/bin/busybox", "nslookup", "-type=A", "api." + domain + "."},
				Stdout:    true,
				Stderr:    true,
			}, scheme.ParameterCodec)
		cmd, err := remotecommand.NewSPDYExecutor(config, http.MethodPost, req.URL())
		if err != nil {
			return "", err
		}
		var stdout, stderr bytes.Buffer
		err = cmd.StreamWithContext(check, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
		return stdout.String() + stderr.String(), err
	}
	corefile := func() (string, error) {
		cm, err := k8s.CoreV1().ConfigMaps("kube-system").Get(ctx, "coredns", metav1.GetOptions{})
		if err != nil {
			return "", err
		}
		return cm.Data["Corefile"], nil
	}
	checkResolves := func(domain, address string) {
		Eventually(func() error {
			output, err := resolve(domain)
			if err != nil {
				return fmt.Errorf("query for %s: %w (%s)", domain, err, output)
			}
			if !strings.Contains(output, address) {
				return fmt.Errorf("query for %s returned %s, expected %s", domain, output, address)
			}
			return nil
		}, 3*time.Minute, 5*time.Second).Should(Succeed())
	}
	Eventually(corefile, time.Minute, 2*time.Second).Should(And(ContainSubstring(first+":53"), ContainSubstring(other+":53")))
	checkResolves(first, "192.0.2.42")
	checkResolves(other, "192.0.2.43")

	ginkgo.By("[4-10] Deleting one domain and observing its removal without affecting the control domain")
	status, err := request(ctx, http.MethodDelete, "/"+first, nil)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Eventually(corefile, time.Minute, 2*time.Second).Should(And(Not(ContainSubstring(first)), ContainSubstring(other+":53")))
	Eventually(func() string {
		output, err := resolve(first)
		if err == nil {
			return ""
		}
		return output
	}, 3*time.Minute, 5*time.Second).Should(ContainSubstring("NXDOMAIN"))
	checkResolves(other, "192.0.2.43")
}
