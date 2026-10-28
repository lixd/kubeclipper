package v1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"

	"github.com/kubeclipper/kubeclipper/pkg/operationv2"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

const backupIdempotencyHeader = "Idempotency-Key"

func backupRequestIdentity(uid types.UID, key string, b *corev1.Backup) (name, fingerprint string, err error) {
	if len(utilvalidation.IsDNS1123Label(b.Name)) != 0 {
		return "", "", fmt.Errorf("backup name must be a DNS label of at most 63 characters")
	}
	if len(key) > 128 || strings.IndexFunc(key, func(r rune) bool { return r < 33 || r > 126 }) >= 0 {
		return "", "", fmt.Errorf("Idempotency-Key must contain 1 to 128 printable ASCII characters without spaces")
	}
	input := struct {
		ClusterUID                       types.UID
		Name, PreferredNode, Description string
	}{uid, b.Name, b.PreferredNode, b.Annotations[common.AnnotationDescription]}
	data, err := json.Marshal(input)
	if err != nil {
		return "", "", err
	}
	digest := sha256.Sum256(data)
	if key == "" {
		return "", hex.EncodeToString(digest[:]), nil
	}
	id := sha256.Sum256([]byte(string(uid) + "\x00" + key))
	return "backup-" + hex.EncodeToString(id[:24]), hex.EncodeToString(digest[:]), nil
}

func (h *handler) replayBackup(ctx context.Context, cluster, name, digest string, uid types.UID) (*corev1.Backup, error) {
	b, err := h.clusterOperator.GetBackup(ctx, cluster, name)
	if apierrors.IsNotFound(err) {
		history, historyErr := h.operationV2Store.ListOperations(ctx, uid, "")
		if historyErr != nil {
			return nil, historyErr
		}
		for i := range history.Items {
			if history.Items[i].Name == name {
				return nil, apierrors.NewGone("backup for this request has been deleted; use a new Idempotency-Key")
			}
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if b.Annotations[operationv2.BackupRequestAnnotation] != digest {
		return nil, apierrors.NewConflict(corev1.Resource("backups"), name, fmt.Errorf("idempotency key was used with different parameters"))
	}
	return b, nil
}

func (h *handler) persistBackupAdmission(ctx context.Context, b *corev1.Backup) (*corev1.Backup, error) {
	result, err := h.clusterOperator.CreateBackup(ctx, b)
	if err == nil {
		return result, nil
	}
	// Readback is also required after a timeout: the record may already have
	// committed. Keep the same name on every attempt and never start an operation
	// from this request, including after the caller cancels it.
	var plan operations.Operation
	if decodeErr := json.Unmarshal([]byte(b.Annotations[operationv2.BackupPlanAnnotation]), &plan); decodeErr != nil {
		return nil, err
	}
	existing, readErr := h.replayBackup(ctx, b.Labels[common.LabelClusterName], b.Name,
		b.Annotations[operationv2.BackupRequestAnnotation], plan.Spec.TargetRef.UID)
	if readErr != nil {
		return nil, readErr
	}
	if existing != nil {
		return existing, nil
	}
	return nil, err
}
