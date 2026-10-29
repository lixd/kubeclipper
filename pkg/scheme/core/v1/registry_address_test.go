package v1

import "testing"

func TestParseRegistryAddress(t *testing.T) {
	for _, address := range []string{"https://ghcr.io/kubeclipper/kubeclipper", "http://172.16.131.146:5249/offline-shipped", "https://registry.example.com"} {
		spec, err := ParseRegistryAddress(address)
		if err != nil {
			t.Fatal(err)
		}
		if spec.Scheme+"://"+spec.ImageRepository() != address {
			t.Fatalf("address changed: %s", address)
		}
	}
	for _, address := range []string{"registry.example.com/team", "ftp://registry.example.com", "https://user:pass@registry.example.com/team", "https://registry.example.com/a/../b", "https://registry.example.com/team/", "https://registry.example.com/team?tag=x", "https://registry.example.com/team#tag", "https://registry.example.com/a//b"} {
		if _, err := ParseRegistryAddress(address); err == nil {
			t.Errorf("accepted %s", address)
		}
	}
}
