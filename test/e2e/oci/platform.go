package oci

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/prometheus/common/expfmt"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	apierror "github.com/kubeclipper/kubeclipper/pkg/errors"
	"github.com/kubeclipper/kubeclipper/pkg/platformstatus"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
	"github.com/kubeclipper/kubeclipper/test/framework"
)

const (
	corePath   = "/api/core.kubeclipper.io/v1"
	configPath = "/api/config.kubeclipper.io/v1"
	auditPath  = "/api/audit.kubeclipper.io/v1"
)

func platformRequest(ctx context.Context, client *kc.Client, method, path string, body any) (int, []byte, error) {
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
	}
	endpoint := client.Scheme() + "://" + client.Host()
	req, err := http.NewRequestWithContext(ctx, method, endpoint+path, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+client.Token())
	req.Header.Set("Content-Type", "application/json")
	res, err := client.HTTPClient().Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	return res.StatusCode, data, err
}

func requirePlatformCandidate(ctx context.Context, client *kc.Client) {
	revision := os.Getenv("KC_E2E_BACKEND_REVISION")
	Expect(revision).To(MatchRegexp("^[0-9a-f]{40}$"))
	version, err := client.Version(ctx)
	Expect(err).NotTo(HaveOccurred())
	Expect(version.GitCommit).To(Equal(revision))
}

var _ = ginkgo.Describe("[OCI-RegistryAudit] [Serial]", func() {
	f := framework.NewDefaultFramework("oci-registry-audit")

	ginkgo.It("[3-07, 4-14] uses candidate CLI and API to update a Registry and observes its real audit event", func() {
		ctx, done := context.WithTimeout(context.Background(), 2*time.Minute)
		defer done()
		requirePlatformCandidate(ctx, f.Client)
		name := "oci-registry-" + string(uuid.NewUUID())[:8]
		run := func(args ...string) {
			out, err := exec.CommandContext(ctx, "kcctl", args...).CombinedOutput()
			Expect(err).NotTo(HaveOccurred(), "kcctl %s %s: %s", args[0], args[1], out)
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			err := f.Client.DeleteRegistry(cleanup, name)
			if !apierror.IsNotFound(err) {
				Expect(err).NotTo(HaveOccurred())
			}
		}()
		ginkgo.By("[3-07] Creating, reading, updating, and deleting one Registry by UID")
		run("create", "registry", "--name", name, "--address", "https://registry.example.test/initial")
		before, err := f.Client.DescribeRegistries(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(before.Items).To(HaveLen(1))
		reg := before.Items[0]
		Expect(reg.UID).NotTo(BeEmpty())
		Expect(reg.Host).To(Equal("registry.example.test"))
		Expect(reg.RepositoryPrefix).To(Equal("initial"))
		updatedAt := time.Now()
		reg.RepositoryPrefix = "updated"
		path := corePath + "/registries/" + name
		status, _, err := platformRequest(ctx, f.Client, http.MethodPut, path, &reg)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(http.StatusOK))
		after, err := f.Client.DescribeRegistries(ctx, name)
		Expect(err).NotTo(HaveOccurred())
		Expect(after.Items).To(HaveLen(1))
		Expect(after.Items[0].UID).To(Equal(reg.UID))
		Expect(after.Items[0].RepositoryPrefix).To(Equal("updated"))
		Expect(after.Items[0].ResourceVersion).NotTo(Equal(reg.ResourceVersion))
		run("delete", "registry", name)
		_, err = f.Client.DescribeRegistries(ctx, name)
		Expect(apierror.IsNotFound(err)).To(BeTrue())

		ginkgo.By("[4-14] Matching the audit list and detail to the successful authenticated write")
		var matched corev1.Event
		Eventually(func() error {
			code, data, err := platformRequest(ctx, f.Client, http.MethodGet, auditPath+"/events?paging=limit%3D100%2Cpage%3D1", nil)
			if err != nil {
				return err
			}
			if code != http.StatusOK {
				return fmt.Errorf("audit event list returned HTTP %d", code)
			}
			var events corev1.EventList
			if err := json.Unmarshal(data, &events); err != nil {
				return err
			}
			for _, event := range events.Items {
				if event.RequestURI == path && event.Verb == "update" && event.ResourceName == name {
					matched = event
					return nil
				}
			}
			return fmt.Errorf("no audit event for the test Registry update")
		}, time.Minute, 2*time.Second).Should(Succeed())
		Expect(matched.Username).To(Equal("admin"))
		Expect(matched.Success).To(BeTrue())
		Expect(matched.Resource).To(Equal("registries"))
		Expect(matched.AuditID).NotTo(BeEmpty())
		Expect(matched.Name).NotTo(BeEmpty())
		Expect(matched.RequestReceivedTimestamp.Time).To(BeTemporally(">=", updatedAt.Add(-5*time.Second)))
		Expect(matched.StageTimestamp.Time).To(BeTemporally(">=", matched.RequestReceivedTimestamp.Time))
		code, data, err := platformRequest(ctx, f.Client, http.MethodGet, auditPath+"/events/"+matched.Name, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(code).To(Equal(http.StatusOK))
		var detail corev1.Event
		Expect(json.Unmarshal(data, &detail)).To(Succeed())
		Expect(detail.UID).To(Equal(matched.UID))
		Expect(detail.AuditID).To(Equal(matched.AuditID))
		Expect(detail.RequestURI).To(Equal(matched.RequestURI))
		Expect(detail.Username).To(Equal(matched.Username))

		ginkgo.By("[3-07] Rejecting an invalid Registry address scheme without creating an object")
		reg.Name += "-invalid"
		reg.ResourceVersion = ""
		reg.UID = ""
		reg.Scheme = "ftp"
		code, _, err = platformRequest(ctx, f.Client, http.MethodPost, corePath+"/registries", &reg)
		Expect(err).NotTo(HaveOccurred())
		Expect(code).To(Equal(http.StatusBadRequest))
		_, err = f.Client.DescribeRegistries(ctx, reg.Name)
		Expect(apierror.IsNotFound(err)).To(BeTrue())
	})
})

var _ = ginkgo.Describe("[OCI-DNSRecords] [Serial]", func() {
	f := framework.NewDefaultFramework("oci-dns-records")

	ginkgo.It("[4-10] persists and validates real DNS domains and A/AAAA records", func() {
		ctx, done := context.WithTimeout(context.Background(), 2*time.Minute)
		defer done()
		requirePlatformCandidate(ctx, f.Client)
		name := "oci-dns-" + string(uuid.NewUUID())[:8] + ".example.test"
		domains := corePath + "/domains"
		path := domains + "/" + name
		check := func(method, path string, body any, want int, out any) {
			code, data, err := platformRequest(ctx, f.Client, method, path, body)
			Expect(err).NotTo(HaveOccurred())
			Expect(code).To(Equal(want), "%s %s returned %s", method, path, data)
			if out != nil {
				Expect(json.Unmarshal(data, out)).To(Succeed())
			}
		}
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			code, _, err := platformRequest(cleanup, f.Client, http.MethodDelete, path, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(code).To(BeElementOf(http.StatusOK, http.StatusNotFound))
		}()
		domain := corev1.Domain{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       corev1.DomainSpec{Description: "initial"},
		}
		var current corev1.Domain
		check(http.MethodPost, domains, &domain, http.StatusOK, &current)
		uid := current.UID
		Expect(uid).NotTo(BeEmpty())
		current.Spec.Description = "updated"
		check(http.MethodPut, path, &current, http.StatusOK, nil)
		check(http.MethodGet, path, nil, http.StatusOK, &current)
		Expect(current.UID).To(Equal(uid))
		Expect(current.Spec.Description).To(Equal("updated"))

		records := path + "/records"
		recordPath := records + "/api." + name
		original := corev1.Record{
			Domain: name,
			RR:     "api",
			ParseRecord: []corev1.ParseRecord{
				{Type: "A", IP: "192.0.2.10"},
				{Type: "AAAA", IP: "2001:db8::10"},
			},
		}
		check(http.MethodPost, records, &original, http.StatusOK, nil)
		var record corev1.Record
		check(http.MethodGet, recordPath, nil, http.StatusOK, &record)
		Expect(record.ParseRecord).To(ConsistOf(original.ParseRecord))
		record.ParseRecord = []corev1.ParseRecord{{Type: "A", IP: "192.0.2.20"}, {Type: "AAAA", IP: "2001:db8::20"}}
		check(http.MethodPut, records, &record, http.StatusOK, nil)
		expected := record.ParseRecord
		for _, invalid := range []corev1.ParseRecord{
			{Type: "A", IP: "invalid"},
			{Type: "A", IP: "2001:db8::30"},
			{Type: "AAAA", IP: "192.0.2.30"},
			{Type: "TXT", IP: "192.0.2.30"},
		} {
			rejected := record
			rejected.ParseRecord = []corev1.ParseRecord{invalid}
			check(http.MethodPut, records, &rejected, http.StatusBadRequest, nil)
			check(http.MethodGet, recordPath, nil, http.StatusOK, &record)
			Expect(record.ParseRecord).To(ConsistOf(expected))
		}
		check(http.MethodDelete, recordPath, nil, http.StatusOK, nil)
		check(http.MethodGet, recordPath, nil, http.StatusNotFound, nil)
		check(http.MethodDelete, path, nil, http.StatusOK, nil)
		check(http.MethodGet, path, nil, http.StatusNotFound, nil)
	})
})

var _ = ginkgo.Describe("[OCI-PlatformDiagnostics] [Serial]", func() {
	f := framework.NewDefaultFramework("oci-platform-diagnostics")

	ginkgo.It("[3-26, 4-13, 4-17] checks actual health, Prometheus metrics, and platform component status", func() {
		ctx, done := context.WithTimeout(context.Background(), 2*time.Minute)
		defer done()
		requirePlatformCandidate(ctx, f.Client)
		endpoint := os.Getenv("KC_E2E_SERVER_ENDPOINT")
		Expect(endpoint).To(HavePrefix("https://"), "use the TLS endpoint of kc-server for healthz and metrics")
		ca, err := os.ReadFile(os.Getenv("KC_E2E_SERVER_CA_FILE"))
		Expect(err).NotTo(HaveOccurred())
		server, err := kc.NewClientWithOpts(kc.WithEndpoint(endpoint), kc.WithCAData(ca), kc.WithServerName(options.KCServerAltName))
		Expect(err).NotTo(HaveOccurred())
		requirePlatformCandidate(ctx, server)
		status, health, err := platformRequest(ctx, server, http.MethodGet, "/healthz", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(http.StatusOK))
		Expect(strings.TrimSpace(string(health))).To(Equal("ok"))

		status, payload, err := platformRequest(ctx, server, http.MethodGet, "/metrics", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(http.StatusOK))
		var parser expfmt.TextParser
		families, err := parser.TextToMetricFamilies(bytes.NewReader(payload))
		Expect(err).NotTo(HaveOccurred())
		Expect(families).To(HaveKey("go_goroutines"))
		Expect(families["go_goroutines"].Metric).NotTo(BeEmpty())
		Expect(families["go_goroutines"].Metric[0].GetGauge().GetValue()).To(BeNumerically(">", 0))

		agents, err := f.Client.ListNodes(ctx, kc.Queries{})
		Expect(err).NotTo(HaveOccurred())
		Expect(agents.Items).NotTo(BeEmpty())
		for _, agent := range agents.Items {
			Expect(slices.ContainsFunc(agent.Status.Conditions, func(condition corev1.NodeCondition) bool {
				return condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue
			})).To(BeTrue(), "fixture Agent %s is not ready", agent.Name)
		}
		status, payload, err = platformRequest(ctx, f.Client, http.MethodGet, configPath+"/status", nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(http.StatusOK))
		var platform platformstatus.PlatformStatus
		Expect(json.Unmarshal(payload, &platform)).To(Succeed())
		Expect(platform.Validate()).To(Succeed())
		for _, component := range platform.Components {
			Expect(component.Status).To(Equal(platformstatus.Healthy), "%s: %s", component.Name, component.Message)
		}
		ginkgo.By("[3-26] Comparing candidate kcctl status to the platform API and live Agent")
		output, err := exec.CommandContext(ctx, "kcctl", "status", "--output=json").Output()
		Expect(err).NotTo(HaveOccurred())
		var cliStatus platformstatus.PlatformStatus
		Expect(json.Unmarshal(output, &cliStatus)).To(Succeed())
		Expect(cliStatus.Validate()).To(Succeed())
		Expect(cliStatus.Status).To(Equal(platform.Status))
		components := make(map[string]platformstatus.Status, len(platform.Components))
		for _, component := range platform.Components {
			components[component.Name] = component.Status
		}
		Expect(cliStatus.Components).To(HaveLen(len(components)))
		for _, component := range cliStatus.Components {
			Expect(components).To(HaveKeyWithValue(component.Name, component.Status))
		}
		for _, path := range []string{"/configz", "/components", "/componentmeta"} {
			status, payload, err := platformRequest(ctx, f.Client, http.MethodGet, configPath+path, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(status).To(Equal(http.StatusOK), "%s", path)
			Expect(json.Valid(payload)).To(BeTrue(), "%s must expose valid JSON", path)
		}
	})
})
