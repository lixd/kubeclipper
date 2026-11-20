package cluster

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	kubev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"

	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

func checkOCIPodExec(ctx context.Context, client *kc.Client, k8s *kubernetes.Clientset, clusterName, namespace string) {
	ginkgo.By("[4-12b] Executing a command in either container through the candidate Pod WebSocket")
	podName := "exec-" + string(uuid.NewUUID())[:8]
	pods := k8s.CoreV1().Pods(namespace)
	spec := &kubev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName}}
	image := os.Getenv("KC_E2E_PROBE_IMAGE")
	for index, name := range []string{"first", "second"} {
		spec.Spec.Containers = append(spec.Spec.Containers, kubev1.Container{
			Name:    name,
			Image:   image,
			Command: []string{"httpd", "-f", "-p", strconv.Itoa(18080 + index), "-h", "/tmp"},
			Env:     []kubev1.EnvVar{{Name: "OCI_CONTAINER_ID", Value: podName + "-" + name}},
		})
	}
	_, err := pods.Create(ctx, spec, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())
	Eventually(func() error {
		pod, err := pods.Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if len(pod.Status.ContainerStatuses) != 2 {
			return fmt.Errorf("waiting for both containers in %s", podName)
		}
		for _, status := range pod.Status.ContainerStatuses {
			if !status.Ready {
				return fmt.Errorf("waiting for container %s: state=%+v", status.Name, status.State)
			}
		}
		return nil
	}, 2*time.Minute, 2*time.Second).Should(Succeed())

	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second
	if transport, ok := client.HTTPClient().Transport.(*http.Transport); ok {
		dialer.TLSClientConfig = transport.TLSClientConfig
	}
	scheme := "ws"
	if client.Scheme() == "https" {
		scheme = "wss"
	}
	connect := func(pod, container, token string) (*websocket.Conn, int, error) {
		// Pod exec splits on spaces. Keep the shell script in one argument.
		params := url.Values{"container": {container}, "command": {`sh -c echo${IFS}"$OCI_CONTAINER_ID"`}}
		endpoint := url.URL{
			Scheme:   scheme,
			Host:     client.Host(),
			Path:     "/api/core.kubeclipper.io/v1/clusters/" + clusterName + "/namespace/" + namespace + "/pods/" + pod + "/exec",
			RawQuery: params.Encode(),
		}
		headers := http.Header{}
		if token != "" {
			headers.Set("Authorization", "Bearer "+token)
		}
		conn, res, err := dialer.DialContext(ctx, endpoint.String(), headers)
		if res != nil {
			defer res.Body.Close()
			return conn, res.StatusCode, err
		}
		return conn, 0, err
	}
	for _, name := range []string{"first", "second"} {
		func() {
			conn, status, err := connect(podName, name, client.Token())
			Expect(err).NotTo(HaveOccurred(), "opening WebSocket to %s", name)
			defer conn.Close()
			Expect(status).To(Equal(http.StatusSwitchingProtocols))
			Expect(conn.SetReadDeadline(time.Now().Add(20 * time.Second))).To(Succeed())
			var output strings.Builder
			for !strings.Contains(output.String(), podName+"-"+name) {
				_, message, err := conn.ReadMessage()
				Expect(err).NotTo(HaveOccurred(), "reading actual output from %s", name)
				_, err = output.Write(message)
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(strings.TrimSpace(output.String())).To(Equal(podName + "-" + name))
		}()
	}
	for _, check := range []struct {
		pod, container, token string
		status                int
	}{
		{podName, "", client.Token(), http.StatusBadRequest},
		{podName, "unknown", client.Token(), http.StatusBadRequest},
		{podName + "-missing", "first", client.Token(), http.StatusNotFound},
		{podName, "first", "", http.StatusForbidden},
	} {
		conn, code, err := connect(check.pod, check.container, check.token)
		if conn != nil {
			_ = conn.Close()
		}
		Expect(err).NotTo(BeNil())
		Expect(conn == nil).To(BeTrue(), "rejected requests must not open a WebSocket")
		Expect(code).To(Equal(check.status), "Pod %s container %s", check.pod, check.container)
	}
}
