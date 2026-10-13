package validation

import (
	"fmt"

	utilversion "k8s.io/apimachinery/pkg/util/version"
)

// ValidateKubernetesUpgradeVersion enforces the version transition supported
// by kubeadm: upgrades must move forward within the same major version and by
// at most one minor version at a time.
func ValidateKubernetesUpgradeVersion(clusterName, currentVersion, targetVersion string) error {
	current, err := utilversion.ParseSemantic(currentVersion)
	if err != nil {
		return fmt.Errorf("invalid current Kubernetes version %q: %w", currentVersion, err)
	}
	target, err := utilversion.ParseSemantic(targetVersion)
	if err != nil {
		return fmt.Errorf("wrong version format: %w", err)
	}

	comparison, err := current.Compare(targetVersion)
	if err != nil {
		return fmt.Errorf("wrong version format: %w", err)
	}
	if comparison == 0 {
		return fmt.Errorf("cluster %s is already at version [%s], a different version is required to upgrade", clusterName, currentVersion)
	}
	if comparison > 0 {
		return fmt.Errorf("cannot downgrade: cluster %s is at version [%s], downgrade to [%s] is not supported", clusterName, currentVersion, targetVersion)
	}
	if target.Major() != current.Major() || target.Minor() > current.Minor()+1 {
		return fmt.Errorf("version [%s] you specify is too high", targetVersion)
	}
	return nil
}
