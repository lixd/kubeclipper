package utils

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/kubeclipper/kubeclipper/pkg/constatns"
	mockcluster "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	"github.com/kubeclipper/kubeclipper/pkg/models/core"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

type defaultRegistryConfig struct {
	core.ConfigMapReader
	value string
}

func (c defaultRegistryConfig) GetConfigMap(context.Context, string) (*v1.ConfigMap, error) {
	return &v1.ConfigMap{Data: map[string]string{constatns.DeployConfigConfigMapKey: c.value}}, nil
}

func TestCompleteClusterImageRegistry(t *testing.T) {
	for _, tt := range []struct {
		name, selected, config, want string
		fail                         bool
	}{
		{name: "platform default", config: "defaultImageRegistry: official", want: "official"},
		{name: "explicit choice", selected: "internal", config: "defaultImageRegistry: official", want: "internal"},
		{name: "explicit choice without platform default", selected: "internal", want: "internal"},
		{name: "no default", fail: true},
		{name: "invalid default config", config: "[", fail: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			op := mockcluster.NewMockOperator(gomock.NewController(t))
			if !tt.fail {
				op.EXPECT().GetRegistry(gomock.Any(), tt.want).Return(&v1.Registry{RegistrySpec: v1.RegistrySpec{Scheme: "https", Host: "registry.example.com", RepositoryPrefix: "release/team"}}, nil)
			}
			c := &v1.Cluster{ImageRegistry: tt.selected}
			err := CompleteClusterImageRegistry(context.Background(), c, defaultRegistryConfig{value: tt.config}, op)
			if tt.fail {
				if err == nil {
					t.Fatal("expected explicit error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if c.ImageRegistry != tt.want || c.ResolvedImageRegistry != "registry.example.com/release/team" {
				t.Fatalf("unexpected source: %s %s", c.ImageRegistry, c.ResolvedImageRegistry)
			}
		})
	}
}
