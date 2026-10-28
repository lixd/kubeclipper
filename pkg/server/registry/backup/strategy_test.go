package backup

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	operationbuilder "github.com/kubeclipper/kubeclipper/pkg/operationv2"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

func TestAdmittedBackupIdentityIsImmutable(t *testing.T) {
	before := &corev1.Backup{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{operationbuilder.BackupPlanAnnotation: "plan", operationbuilder.BackupRequestAnnotation: "digest"}}}
	for _, key := range []string{operationbuilder.BackupPlanAnnotation, operationbuilder.BackupRequestAnnotation} {
		after := before.DeepCopy()
		delete(after.Annotations, key)
		if len((BackupStrategy{}).ValidateUpdate(context.Background(), after, before)) == 0 {
			t.Fatalf("admission metadata %s could be removed", key)
		}
	}
	after := before.DeepCopy()
	after.Status.ClusterBackupStatus = corev1.ClusterBackupAvailable
	if errs := (BackupStrategy{}).ValidateUpdate(context.Background(), after, before); len(errs) != 0 {
		t.Fatalf("status update forbidden: %v", errs)
	}
}
