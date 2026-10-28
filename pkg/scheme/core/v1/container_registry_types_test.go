package v1

import (
	"encoding/json"
	"testing"
)

func TestRegistryImageRepository(t *testing.T) {
	for _, prefix := range []string{"", "qualified", "qualified/team"} {
		r := RegistrySpec{Host: "registry.example.com:5251", RepositoryPrefix: prefix}
		if err := r.ValidateImageRepository(); err != nil {
			t.Fatal(err)
		}
		want := r.Host
		if prefix != "" {
			want += "/" + prefix
		}
		if r.ImageRepository() != want {
			t.Fatalf("repository = %q, want %q", r.ImageRepository(), want)
		}
		data, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		var round RegistrySpec
		if err = json.Unmarshal(data, &round); err != nil {
			t.Fatal(err)
		}
		if round != r {
			t.Fatal("registry JSON roundtrip changed fields")
		}
	}
	for _, host := range []string{"localhost", "[2001:db8::1]:5000", "registry.example.com:5251"} {
		if err := (&RegistrySpec{Host: host, RepositoryPrefix: "team"}).ValidateImageRepository(); err != nil {
			t.Fatal(err)
		}
	}
	for _, host := range []string{"", "https://registry.example.com", "registry.example.com/path", "registry.example.com:tag", "user@registry.example.com"} {
		if err := (&RegistrySpec{Host: host}).ValidateImageRepository(); err == nil {
			t.Fatalf("accepted invalid host %q", host)
		}
	}
	for _, prefix := range []string{"/team", "team/", "team//sub", ".", "..", "team/../sub", "team/./sub", "https://team", "team:tag", "team@sha256:abcdef", "TEAM", "team with space"} {
		if err := (&RegistrySpec{Host: "registry.example.com", RepositoryPrefix: prefix}).ValidateImageRepository(); err == nil {
			t.Fatalf("accepted invalid prefix %q", prefix)
		}
	}
}
