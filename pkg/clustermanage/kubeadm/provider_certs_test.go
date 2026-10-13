package kubeadm

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	certutils "github.com/kubeclipper/kubeclipper/pkg/utils/certs"
)

func TestSignAgentCertsUsesServerCA(t *testing.T) {
	pki := filepath.Join(t.TempDir(), "server", "pki")
	ca, key, err := certutils.NewCaCertAndKey(certutils.Config{Path: pki, BaseName: "ca", CommonName: "platform-ca", Year: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = certutils.WriteCertAndKey(pki, "ca", ca, key); err != nil {
		t.Fatal(err)
	}
	// No CLI PKI exists: certificates must be signed using the server's CA.
	sources, err := signAgentCerts(pki, "172.16.131.208", "external-node")
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 3 || sources[0] != filepath.Join(pki, "ca.crt") {
		t.Fatalf("unexpected sources: %v", sources)
	}
	if _, err = tls.LoadX509KeyPair(sources[1], sources[2]); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(sources[1])
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatal("missing certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if err = cert.CheckSignatureFrom(ca); err != nil {
		t.Fatal(err)
	}
	if err = cert.VerifyHostname("172.16.131.208"); err != nil {
		t.Fatal(err)
	}
	if cert.Subject.CommonName != "system:kc-agent:external-node" {
		t.Fatalf("unexpected subject: %s", cert.Subject.CommonName)
	}
	for _, source := range sources {
		if !strings.HasPrefix(source, pki+string(os.PathSeparator)) {
			t.Fatalf("certificate outside server PKI: %s", source)
		}
	}
}

func TestSignAgentCertsMissingServerCA(t *testing.T) {
	_, err := signAgentCerts(t.TempDir(), "172.16.131.208", "external-node")
	if err == nil || !strings.Contains(err.Error(), "load KubeClipper CA") {
		t.Fatalf("expected explicit missing CA error, got %v", err)
	}
}
