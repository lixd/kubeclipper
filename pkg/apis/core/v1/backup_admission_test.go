package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	operationv2builder "github.com/kubeclipper/kubeclipper/pkg/operationv2"

	"github.com/emicklei/go-restful"
	"github.com/golang/mock/gomock"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	mock "github.com/kubeclipper/kubeclipper/pkg/models/cluster/mock"
	operationstore "github.com/kubeclipper/kubeclipper/pkg/models/operationv2"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	corev1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/generic"
)

type backupTestOperations struct {
	operationstore.Store
	creates int
}

func (s *backupTestOperations) CreateOperation(_ context.Context, op *operations.Operation) (*operations.Operation, error) {
	s.creates++
	return op, nil
}

func (*backupTestOperations) ListOperations(context.Context, types.UID, string) (*operations.OperationList, error) {
	return &operations.OperationList{}, nil
}

func TestBackupAdmissionDoesNotWriteClusterAfterStartingTask(t *testing.T) {
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:6443")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	operator := mock.NewMockOperator(gomock.NewController(t))
	c := &corev1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", UID: "cluster-uid", ResourceVersion: "1", Labels: map[string]string{common.LabelBackupPoint: "point"}}, Status: corev1.ClusterStatus{Phase: corev1.ClusterRunning}, Masters: []corev1.WorkerNode{{ID: "node", Labels: map[string]string{common.LabelTopologyRegion: "region"}}}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node", UID: "node-uid"}, Status: corev1.NodeStatus{Ipv4DefaultIP: "127.0.0.1", NodeInfo: corev1.NodeSystemInfo{Hostname: "node"}}}
	point := &corev1.BackupPoint{ObjectMeta: metav1.ObjectMeta{Name: "point"}, StorageType: "fs", FsConfig: &corev1.FsConfig{BackupRootDir: "/tmp/test-backups"}}
	operator.EXPECT().GetClusterEx(gomock.Any(), "cluster", gomock.Any()).Return(c, nil).AnyTimes()
	var mu sync.Mutex
	records := map[string]*corev1.Backup{}
	creates := 0
	operator.EXPECT().GetBackup(gomock.Any(), "cluster", gomock.Any()).DoAndReturn(func(_ context.Context, _, name string) (*corev1.Backup, error) {
		mu.Lock()
		defer mu.Unlock()
		if b, ok := records[name]; ok {
			return b.DeepCopy(), nil
		}
		return nil, apierrors.NewNotFound(corev1.Resource("backup"), name)
	}).AnyTimes()
	operator.EXPECT().GetBackupPoint(gomock.Any(), "point", gomock.Any()).Return(point, nil).AnyTimes()
	operator.EXPECT().GetBackupPointEx(gomock.Any(), "point", gomock.Any()).Return(point, nil).AnyTimes()
	operator.EXPECT().GetNodeEx(gomock.Any(), "node", gomock.Any()).Return(node, nil).AnyTimes()
	operator.EXPECT().ListNodes(gomock.Any(), gomock.Any()).Return(&corev1.NodeList{Items: []corev1.Node{*node}}, nil).AnyTimes()
	operator.EXPECT().CreateBackup(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, b *corev1.Backup) (*corev1.Backup, error) {
		mu.Lock()
		defer mu.Unlock()
		if _, exists := records[b.Name]; exists {
			return nil, apierrors.NewAlreadyExists(corev1.Resource("backups"), b.Name)
		}
		creates++
		b = b.DeepCopy()
		b.UID = "backup-uid"
		records[b.Name] = b.DeepCopy()
		return b, nil
	}).AnyTimes()
	operator.EXPECT().UpdateCluster(gomock.Any(), gomock.Any()).Return(nil, apierrors.NewConflict(corev1.Resource("clusters"), "cluster", nil)).AnyTimes()
	store := &backupTestOperations{}
	h := &handler{clusterOperator: operator, operationV2Store: store, genericConfig: &generic.ServerRunOptions{}}
	ws := new(restful.WebService).Path("/").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)
	ws.Route(ws.POST("/clusters/{name}/backups").To(h.CreateBackup))
	container := restful.NewContainer()
	container.Add(ws)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/clusters/cluster/backups", strings.NewReader(`{"metadata":{"name":"snapshot"}}`))
	req.Header.Set("Content-Type", restful.MIME_JSON)
	req.Header.Set(backupIdempotencyHeader, "stable-request")
	response := httptest.NewRecorder()
	container.ServeHTTP(response, req)
	if response.Code != http.StatusOK {
		t.Fatalf("accepted backup returned %d: %s", response.Code, response.Body.String())
	}
	var first corev1.Backup
	if err := json.Unmarshal(response.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first.Labels[common.LabelOperationName] == "" || response.Header().Get("X-Operation-Name") == "" {
		t.Fatal("missing durable operation association")
	}
	exerciseBackupReplays(t, container, c)
	if creates != 1 {
		t.Fatalf("replay or dry-run created %d backups", creates)
	}

	if store.creates != 0 {
		t.Fatalf("API started %d operations before durable admission recovery", store.creates)
	}
}

func TestBackupRequestIdentityBindsParametersAndClusterUID(t *testing.T) {
	b := &corev1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "snapshot", Annotations: map[string]string{common.AnnotationDescription: "before"}}}
	name, digest, err := backupRequestIdentity("cluster-uid", "request-123", b)
	if err != nil {
		t.Fatal(err)
	}
	again, same, err := backupRequestIdentity("cluster-uid", "request-123", b.DeepCopy())
	if err != nil || again != name || same != digest {
		t.Fatal("identical request changed identity")
	}
	b.PreferredNode = "other-node"
	otherName, otherDigest, otherErr := backupRequestIdentity("cluster-uid", "request-123", b)
	if otherErr != nil || otherName != name || otherDigest == digest {
		t.Fatal("changed parameters must conflict on the same identity")
	}
	recreated, _, recreateErr := backupRequestIdentity("replacement-uid", "request-123", b)
	if recreateErr != nil || recreated == name {
		t.Fatal("key not isolated by Cluster UID")
	}
	for _, key := range []string{"has space", strings.Repeat("a", 129), "line\nbreak"} {
		if _, _, err := backupRequestIdentity("cluster", key, b); err == nil {
			t.Fatalf("invalid key %q accepted", key)
		}
	}
}

func TestBackupPersistReadbackAfterAmbiguousWriteAndConflict(t *testing.T) {
	for _, kind := range []string{"committed-timeout", "not-committed", "different-request"} {
		t.Run(kind, func(t *testing.T) {
			operator := mock.NewMockOperator(gomock.NewController(t))
			b := &corev1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "stable-backup", Labels: map[string]string{common.LabelClusterName: "cluster"}, Annotations: map[string]string{operationv2builder.BackupRequestAnnotation: "digest"}}}
			b.Annotations[operationv2builder.BackupPlanAnnotation] = `{"spec":{"targetRef":{"uid":"cluster-uid"}}}`

			operator.EXPECT().CreateBackup(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("storage write response lost"))
			switch kind {
			case "committed-timeout":
				stored := b.DeepCopy()
				stored.UID = "stored-uid"
				operator.EXPECT().GetBackup(gomock.Any(), "cluster", b.Name).Return(stored, nil)
			case "not-committed":
				operator.EXPECT().GetBackup(gomock.Any(), "cluster", b.Name).Return(nil, apierrors.NewNotFound(corev1.Resource("backups"), b.Name))
			case "different-request":
				stored := b.DeepCopy()
				stored.Annotations[operationv2builder.BackupRequestAnnotation] = "other"
				operator.EXPECT().GetBackup(gomock.Any(), "cluster", b.Name).Return(stored, nil)
			}
			h := &handler{clusterOperator: operator, operationV2Store: &backupTestOperations{}}
			got, err := h.persistBackupAdmission(context.Background(), b)
			if kind == "committed-timeout" {
				if err != nil || got == nil || got.UID != "stored-uid" {
					t.Fatalf("readback failed got=%v err=%v", got, err)
				}
			} else if err == nil {
				t.Fatal("unknown or foreign result reported success")
			} else if kind == "different-request" && !apierrors.IsConflict(err) {
				t.Fatalf("want409 got %v", err)
			}
		})
	}
}

func exerciseBackupReplays(t *testing.T, container *restful.Container, c *corev1.Cluster) {
	t.Helper()

	// Replays must be recognized before the current Cluster phase check.
	c.Status.Phase = corev1.ClusterBackingUp
	invoke := func(body, path string) *httptest.ResponseRecorder {
		request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, path, strings.NewReader(body))
		request.Header.Set("Content-Type", restful.MIME_JSON)
		request.Header.Set(backupIdempotencyHeader, "stable-request")
		recorder := httptest.NewRecorder()
		container.ServeHTTP(recorder, request)
		return recorder
	}
	var wg sync.WaitGroup
	results := make(chan *httptest.ResponseRecorder, 12)
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- invoke(`{"metadata":{"name":"snapshot"}}`, "/clusters/cluster/backups")
		}()
	}
	wg.Wait()
	close(results)
	for replay := range results {
		if replay.Code != http.StatusOK {
			t.Fatalf("concurrent replay returned %d", replay.Code)
		}
	}
	conflict := invoke(`{"metadata":{"name":"different"}}`, "/clusters/cluster/backups")
	if conflict.Code != http.StatusConflict {
		t.Fatalf("changed parameters returned %d", conflict.Code)
	}
	c.Status.Phase = corev1.ClusterRunning
	dry := invoke(`{"metadata":{"name":"dry"}}`, "/clusters/cluster/backups?dryRun=true")
	if dry.Code != http.StatusOK {
		t.Fatalf("dry run returned %d: %s", dry.Code, dry.Body.String())
	}
}
