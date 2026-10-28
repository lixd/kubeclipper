package operationv2

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

// This fake models storage CAS and ambiguous committed writes. It does not
// simulate execution or claim that a real snapshot has been produced.
type admissionStorage struct {
	mu                                                    sync.Mutex
	cluster                                               *corev1.Cluster
	ops                                                   map[string]*operations.Operation
	writes, creates                                       int
	conflict, clusterTimeout, opTimeout, opFail, readFail bool
}

func (s *admissionStorage) GetClusterEx(ctx context.Context, _, _ string) (*corev1.Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.cluster.DeepCopy(), nil
}
func (s *admissionStorage) UpdateCluster(ctx context.Context, c *corev1.Cluster) (*corev1.Cluster, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.conflict {
		s.conflict = false
		s.cluster.ResourceVersion = "2"
		s.cluster.Labels = map[string]string{"concurrent": "keep"}
		return nil, apierrors.NewConflict(corev1.Resource("clusters"), c.Name, fmt.Errorf("concurrent write"))
	}
	if c.ResourceVersion != s.cluster.ResourceVersion {
		return nil, apierrors.NewConflict(corev1.Resource("clusters"), c.Name, fmt.Errorf("stale version"))
	}
	s.writes++
	s.cluster = c.DeepCopy()
	s.cluster.ResourceVersion = strconv.Itoa(s.writes + 2)
	if s.clusterTimeout {
		s.clusterTimeout = false
		return nil, fmt.Errorf("committed Cluster update response lost")
	}
	return s.cluster.DeepCopy(), nil
}
func (s *admissionStorage) ListOperations(ctx context.Context, _ types.UID, _ string) (*operations.OperationList, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.readFail {
		return nil, fmt.Errorf("storage unavailable")
	}
	list := &operations.OperationList{}
	for _, o := range s.ops {
		list.Items = append(list.Items, *o.DeepCopy())
	}
	return list, nil
}
func (s *admissionStorage) CreateOperation(ctx context.Context, o *operations.Operation) (*operations.Operation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.opFail {
		return nil, fmt.Errorf("operation storage unavailable")
	}
	if _, ok := s.ops[o.Name]; ok {
		return nil, apierrors.NewAlreadyExists(operations.Resource("operations"), o.Name)
	}
	s.creates++
	o = o.DeepCopy()
	o.UID = "operation-uid"
	s.ops[o.Name] = o
	if s.opTimeout {
		s.opTimeout = false
		return nil, fmt.Errorf("committed Operation response lost")
	}
	return o.DeepCopy(), nil
}
func admissionFixture(t *testing.T) (*corev1.Backup, *admissionStorage) {
	t.Helper()
	b := &corev1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "snapshot", UID: "backup-uid", Labels: map[string]string{common.LabelClusterName: "cluster", common.LabelOperationName: "backup-operation"}}}
	op := &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "backup-operation", Labels: map[string]string{common.LabelBackupName: b.Name}}, Spec: operations.OperationSpec{TargetRef: operations.ObjectReference{Kind: corev1.KindCluster, Name: "cluster", UID: "cluster-uid"}, Action: corev1.OperationBackupCluster, DesiredState: operations.OperationDesiredStateActive, Timeout: metav1.Duration{Duration: time.Minute}, Steps: []operations.OperationStep{{ID: "snapshot", Executor: "custom", Targets: []operations.NodeReference{{Name: "node", UID: "node-uid"}}, Payload: runtime.RawExtension{Raw: []byte(`{}`)}}}}, Status: operations.OperationStatus{Phase: operations.OperationPending}}
	if err := AttachBackupPlan(b, op); err != nil {
		t.Fatal(err)
	}
	return b, &admissionStorage{cluster: &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid", ResourceVersion: "1"}, Status: corev1.ClusterStatus{Phase: corev1.ClusterRunning}}, ops: map[string]*operations.Operation{}}
}

func TestBackupAdmissionRestartAtEachWriteBoundary(t *testing.T) {
	for _, boundary := range []string{"conflict", "cluster-committed-timeout", "operation-not-written", "operation-committed-timeout", "read-failure", "canceled-request"} {
		t.Run(boundary, func(t *testing.T) {
			b, s := admissionFixture(t)
			ctx := configureAdmissionFailure(boundary, s)
			_, firstErr := EnsureBackupOperation(ctx, b, s, s)
			if boundary != "conflict" && boundary != "operation-committed-timeout" && firstErr == nil {
				t.Fatal("real failure was hidden")
			}
			s.opFail = false
			s.readFail = false
			// A fresh call has only the persisted Backup, Cluster and Operation facts.
			op, err := EnsureBackupOperation(context.Background(), b.DeepCopy(), s, s)
			if err != nil || op == nil {
				t.Fatalf("restart: op=%v err=%v", op, err)
			}
			if s.creates != 1 || len(s.ops) != 1 || s.cluster.Status.Phase != corev1.ClusterBackingUp {
				t.Fatalf("unexpected persisted result creates=%d phase=%s", s.creates, s.cluster.Status.Phase)
			}
			if boundary == "conflict" && s.cluster.Labels["concurrent"] != "keep" {
				t.Fatal("concurrent metadata overwritten")
			}
		})
	}
}
func TestConcurrentBackupPublishersCreateOnlyOneOperation(t *testing.T) {
	b, s := admissionFixture(t)
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for range 24 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := EnsureBackupOperation(context.Background(), b.DeepCopy(), s, s)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if s.creates != 1 || s.writes != 1 {
		t.Fatalf("creates=%d reservation writes=%d", s.creates, s.writes)
	}
}
func TestBackupAdmissionDoesNotOverwriteTerminalOrNewOwner(t *testing.T) {
	b, s := admissionFixture(t)
	op, err := EnsureBackupOperation(context.Background(), b, s, s)
	if err != nil {
		t.Fatal(err)
	}
	s.ops[op.Name].Status.Phase = operations.OperationSucceeded
	s.cluster.Status.Phase = corev1.ClusterRunning
	s.cluster.Annotations[BackupOwnerAnnotation] = "next-backup"
	writes := s.writes
	for range 3 {
		if _, err := EnsureBackupOperation(context.Background(), b, s, s); err != nil {
			t.Fatal(err)
		}
	}
	if s.writes != writes || s.cluster.Status.Phase != corev1.ClusterRunning || s.cluster.Annotations[BackupOwnerAnnotation] != "next-backup" {
		t.Fatal("old publisher overwrote terminal/new owner")
	}
}
func TestBackupAdmissionWaitsWithoutPublishingWhileClusterBusy(t *testing.T) {
	b, s := admissionFixture(t)
	s.cluster.Status.Phase = corev1.ClusterUpdating
	op, err := EnsureBackupOperation(context.Background(), b, s, s)
	if err != nil || op != nil || s.creates != 0 || s.writes != 0 {
		t.Fatalf("busy Cluster was modified: op=%v err=%v", op, err)
	}
}
func TestBackupAdmissionRejectsRecreatedTargetAndForeignOperation(t *testing.T) {
	for _, kind := range []string{"target", "operation"} {
		t.Run(kind, func(t *testing.T) {
			b, s := admissionFixture(t)
			if kind == "target" {
				s.cluster.UID = "replacement"
			} else {
				s.ops["backup-operation"] = &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "backup-operation"}}
			}
			if _, err := EnsureBackupOperation(context.Background(), b, s, s); err == nil {
				t.Fatal("foreign identity accepted")
			}
			if s.creates != 0 || s.writes != 0 {
				t.Fatal("side effect after identity mismatch")
			}
		})
	}
}

func configureAdmissionFailure(boundary string, s *admissionStorage) context.Context {
	ctx := context.Background()

	switch boundary {
	case "conflict":
		s.conflict = true
	case "cluster-committed-timeout":
		s.clusterTimeout = true
	case "operation-not-written":
		s.opFail = true
	case "operation-committed-timeout":
		s.opTimeout = true
	case "read-failure":
		s.readFail = true
	case "canceled-request":
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	}
	return ctx
}
