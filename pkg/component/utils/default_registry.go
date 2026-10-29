package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/kubeclipper/kubeclipper/pkg/constatns"
	"github.com/kubeclipper/kubeclipper/pkg/models/cluster"
	"github.com/kubeclipper/kubeclipper/pkg/models/core"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"sigs.k8s.io/yaml"
)

// DefaultImageRegistryName reads the explicit platform default. A missing default
// must be corrected by the administrator rather than inferred from registry count.
func DefaultImageRegistryName(ctx context.Context, op core.ConfigMapReader) (string, error) {
	cm, err := op.GetConfigMap(ctx, constatns.DeployConfigConfigMapName)
	if err != nil {
		return "", fmt.Errorf("read platform default image registry: %w", err)
	}
	var config struct {
		DefaultImageRegistry string `json:"defaultImageRegistry"`
	}
	if err := yaml.Unmarshal([]byte(cm.Data[constatns.DeployConfigConfigMapKey]), &config); err != nil {
		return "", fmt.Errorf("read platform default image registry: %w", err)
	}
	name := strings.TrimSpace(config.DefaultImageRegistry)
	return name, nil
}

// CompleteClusterImageRegistry resolves and persists an explicit choice for a new cluster.
func CompleteClusterImageRegistry(ctx context.Context, c *v1.Cluster, config core.ConfigMapReader, registries cluster.Operator) error {
	name := strings.TrimSpace(c.ImageRegistry)
	if name == "" {
		var err error
		name, err = DefaultImageRegistryName(ctx, config)
		if err != nil {
			return err
		}
		if name == "" {
			return fmt.Errorf("platform default image registry is not configured; select a Registry explicitly or configure the platform default")
		}
	}
	registry, err := ResolveImageRegistry(ctx, name, registries)
	if err != nil {
		return err
	}
	c.ImageRegistry = name
	c.ResolvedImageRegistry = registry.ImageRepository()
	return nil
}
