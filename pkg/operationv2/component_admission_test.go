package operationv2

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

type componentStore struct {
	op           *operations.Operation
	writes       int
	lostResponse bool
	rejectWrite  bool
}

func (s *componentStore) GetOperation(_ context.Context, name, _ string) (*operations.Operation, error) {
	if s.op == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "operations"}, name)
	}
	return s.op.DeepCopy(), nil
}
func (s *componentStore) CreateOperation(_ context.Context, op *operations.Operation) (*operations.Operation, error) {
	s.writes++
	if s.rejectWrite {
		return nil, errors.New("storage unavailable")
	}
	s.op = op.DeepCopy()
	if s.lostResponse {
		return nil, errors.New("response lost")
	}
	return s.op.DeepCopy(), nil
}

func TestComponentAdmissionRetriesUncommittedWrite(t *testing.T) {
	c, op := validComponentPlan()
	if err := AttachComponentPlan(c, op); err != nil {
		t.Fatal(err)
	}
	store := &componentStore{rejectWrite: true}
	if err := PublishComponentPlan(context.Background(), c, store); err == nil || store.op != nil {
		t.Fatal("failed publication was hidden or committed")
	}
	store.rejectWrite = false
	if err := PublishComponentPlan(context.Background(), c.DeepCopy(), store); err != nil {
		t.Fatalf("publication was not recovered: %v", err)
	}
	if store.writes != 2 || store.op.Name != op.Name {
		t.Fatal("recovery did not reuse the stable operation identity")
	}
}
func validComponentPlan() (*corev1.Cluster, *operations.Operation) {
	c := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", UID: "cluster-uid"}, Status: corev1.ClusterStatus{Phase: corev1.ClusterRunning}}
	op := &operations.Operation{ObjectMeta: metav1.ObjectMeta{Name: "op1"}, Spec: operations.OperationSpec{TargetRef: operations.ObjectReference{Kind: corev1.KindCluster, Name: c.Name, UID: c.UID}, Action: corev1.OperationInstallComponents, DesiredState: operations.OperationDesiredStateActive, Timeout: metav1.Duration{Duration: time.Hour}, Steps: []operations.OperationStep{{ID: "step", Executor: "noop", Targets: []operations.NodeReference{{Name: "n1", UID: "node-uid"}}, Payload: runtime.RawExtension{Raw: []byte(`{"ok":true}`)}}}}}
	return c, op
}

func TestComponentAdmissionRecoversLostPublishResponse(t *testing.T) {
	c, op := validComponentPlan()
	if err := AttachComponentPlan(c, op); err != nil {
		t.Fatal(err)
	}
	store := &componentStore{lostResponse: true}
	if err := PublishComponentPlan(context.Background(), c.DeepCopy(), store); err == nil {
		t.Fatal("lost response was hidden")
	}
	if err := PublishComponentPlan(context.Background(), c.DeepCopy(), store); err != nil {
		t.Fatalf("restart did not recover: %v", err)
	}
	if store.writes != 1 {
		t.Fatalf("operation created %d times", store.writes)
	}
}

func TestComponentAdmissionRejectsChangedIdentityAndPlan(t *testing.T) {
	c, op := validComponentPlan()
	if err := AttachComponentPlan(c, op); err != nil {
		t.Fatal(err)
	}
	store := &componentStore{op: op.DeepCopy()}
	store.op.Spec.Steps[0].Executor = "different"
	if err := PublishComponentPlan(context.Background(), c, store); err == nil {
		t.Fatal("different published plan accepted")
	}
	store.op = op.DeepCopy()
	c.UID = "recreated-cluster"
	if err := PublishComponentPlan(context.Background(), c, store); err == nil {
		t.Fatal("recreated Cluster accepted")
	}
	if store.writes != 0 {
		t.Fatal("created an operation for a mismatched reservation")
	}
}

func TestComponentAdmissionRejectsInvalidBeforeReservation(t *testing.T) {
	c, op := validComponentPlan()
	op.Spec.Steps = nil
	if err := AttachComponentPlan(c, op); err == nil {
		t.Fatal("invalid plan accepted")
	}
	if c.Status.Phase != corev1.ClusterRunning || c.Annotations[ComponentOwnerAnnotation] != "" {
		t.Fatal("invalid plan reserved cluster")
	}
}

type componentClusters struct {
	current      *corev1.Cluster
	conflictOnce bool
	writes       int
}

func (s *componentClusters) GetClusterEx(context.Context, string, string) (*corev1.Cluster, error) {
	return s.current.DeepCopy(), nil
}
func (s *componentClusters) UpdateCluster(_ context.Context, c *corev1.Cluster) (*corev1.Cluster, error) {
	s.writes++
	if s.conflictOnce {
		s.conflictOnce = false
		s.current.Labels = map[string]string{"concurrent": "preserved"}
		return nil, apierrors.NewConflict(schema.GroupResource{Resource: "clusters"}, c.Name, errors.New("concurrent update"))
	}
	s.current = c.DeepCopy()
	return s.current.DeepCopy(), nil
}
func TestComponentPublicationErrorIsVisibleAndClearedAfterRecovery(t *testing.T) {
	c, op := validComponentPlan()
	if err := AttachComponentPlan(c, op); err != nil {
		t.Fatal(err)
	}
	clusters := &componentClusters{current: c.DeepCopy(), conflictOnce: true}
	publishErr := errors.New("storage unavailable")
	if err := RecordComponentPublication(context.Background(), clusters, c, publishErr); err != nil {
		t.Fatal(err)
	}
	if clusters.current.Annotations[ComponentErrorAnnotation] != publishErr.Error() || clusters.current.Labels["concurrent"] != "preserved" || clusters.current.Status.Phase != corev1.ClusterUpdating {
		t.Fatalf("publication failure not recorded safely: %#v", clusters.current)
	}
	if err := RecordComponentPublication(context.Background(), clusters, c, nil); err != nil {
		t.Fatal(err)
	}
	if clusters.current.Annotations[ComponentErrorAnnotation] != "" || clusters.current.Annotations[ComponentPlanAnnotation] == "" {
		t.Fatal("recovery did not clear only the error")
	}
}
func TestComponentPublicationErrorCannotOverwriteNewAdmission(t *testing.T) {
	c, op := validComponentPlan()
	if err := AttachComponentPlan(c, op); err != nil {
		t.Fatal(err)
	}
	current := c.DeepCopy()
	current.Annotations[ComponentOwnerAnnotation] = "new-operation"
	clusters := &componentClusters{current: current}
	if err := RecordComponentPublication(context.Background(), clusters, c, errors.New("old error")); err != nil {
		t.Fatal(err)
	}
	if clusters.writes != 0 {
		t.Fatal("old admission changed a new admission")
	}
}
