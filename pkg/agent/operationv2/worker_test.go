/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package operationv2

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/kubeclipper/kubeclipper/pkg/oplog"
	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

type executorFunc func(context.Context, *operations.OperationTask, io.Writer) (operations.TaskResult, error)

func (f executorFunc) Reconcile(ctx context.Context, task *operations.OperationTask, log io.Writer) (operations.TaskResult, error) {
	return f(ctx, task, log)
}

type fakeTaskClient struct {
	mu                   sync.Mutex
	task                 *operations.OperationTask
	phases               []operations.TaskPhase
	loseTerminalResponse bool
	// purgeOnTerminal models a server that finalizes the operation and
	// purges its task history right after the terminal status is recorded:
	// every later Get returns NotFound (R7/N8 incident).
	purgeOnTerminal bool
	purged          bool
}

var operationTaskResource = schema.GroupResource{Group: "operations.kubeclipper.io", Resource: "operationtasks"}

func (f *fakeTaskClient) Get(context.Context, string, metav1.GetOptions) (*operations.OperationTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.purged {
		return nil, apierrors.NewNotFound(operationTaskResource, f.task.Name)
	}
	return f.task.DeepCopy(), nil
}

func (f *fakeTaskClient) List(context.Context, *metav1.ListOptions) (*operations.OperationTaskList, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &operations.OperationTaskList{Items: []operations.OperationTask{*f.task.DeepCopy()}}, nil
}

func (*fakeTaskClient) Watch(context.Context, *metav1.ListOptions) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}

func (f *fakeTaskClient) UpdateStatus(
	_ context.Context,
	task *operations.OperationTask,
) (*operations.OperationTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if task.UID != f.task.UID || task.ResourceVersion != f.task.ResourceVersion {
		return nil, errors.New("conflict")
	}
	f.task.Status = *task.Status.DeepCopy()
	f.phases = append(f.phases, task.Status.Phase)
	if f.purgeOnTerminal && task.Status.Phase.IsTerminal() {
		f.purged = true
	}
	f.task.ResourceVersion = string(rune(f.task.ResourceVersion[0] + 1))
	updated := f.task.DeepCopy()
	if f.loseTerminalResponse && task.Status.Phase.IsTerminal() {
		return nil, errors.New("response lost")
	}
	return updated, nil
}

func newTestTask(phase operations.TaskPhase) *operations.OperationTask {
	return &operations.OperationTask{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "task-1",
			UID:               types.UID("task-uid"),
			ResourceVersion:   "1",
			CreationTimestamp: metav1.NewTime(time.Unix(1, 0)),
		},
		Spec: operations.OperationTaskSpec{
			NodeRef:  operations.NodeReference{Name: "agent-1", UID: types.UID("node-uid")},
			Executor: "test/v1",
			Payload:  runtime.RawExtension{Raw: []byte(`{}`)},
			Deadline: metav1.NewTime(time.Now().Add(time.Minute)),
		},
		Status: operations.OperationTaskStatus{Phase: phase},
	}
}

func newTestWorker(t *testing.T, client TaskClient, executor Executor) *Worker {
	t.Helper()
	registry := NewRegistry()
	if err := registry.Register("test/v1", executor); err != nil {
		t.Fatal(err)
	}
	logStore, err := oplog.NewOperationLog(&oplog.Options{Dir: t.TempDir(), OplogThreshold: oplog.DefaultThreshold})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewWorker(&WorkerOptions{
		AgentID:  "agent-1",
		NodeUID:  types.UID("node-uid"),
		Client:   client,
		Registry: registry,
		OpLog:    logStore,
		LockFile: t.TempDir() + "/worker.lock",
	})
	if err != nil {
		t.Fatal(err)
	}
	seed, ok := client.(interface{ seedTask() *operations.OperationTask })
	if !ok {
		t.Fatalf("client %T must expose its seed task", client)
	}
	if err := worker.informer.GetStore().Add(seed.seedTask().DeepCopy()); err != nil {
		t.Fatal(err)
	}
	return worker
}

func (f *fakeTaskClient) seedTask() *operations.OperationTask {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.task.DeepCopy()
}

func (f *fakeMidwayPurgeClient) seedTask() *operations.OperationTask {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.task.DeepCopy()
}

func (f *fakeStaleTaskClient) seedTask() *operations.OperationTask {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.task.DeepCopy()
}

func TestWorkerClaimsAndCompletesTask(t *testing.T) {
	client := &fakeTaskClient{task: newTestTask(operations.TaskPending)}
	worker := newTestWorker(
		t,
		client,
		executorFunc(func(context.Context, *operations.OperationTask, io.Writer) (operations.TaskResult, error) {
			return operations.TaskResult{Outputs: map[string]string{"token": "small-value"}}, nil
		}),
	)

	if err := worker.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := client.phases, []operations.TaskPhase{operations.TaskRunning, operations.TaskSucceeded}; len(got) != len(want) ||
		got[0] != want[0] ||
		got[1] != want[1] {
		t.Fatalf("unexpected phase updates: %#v", got)
	}
	if client.task.Status.Result.Outputs["token"] != "small-value" {
		t.Fatalf("output was not persisted: %#v", client.task.Status.Result)
	}
}

func TestWorkerAcceptsPersistedTerminalAfterLostResponse(t *testing.T) {
	client := &fakeTaskClient{task: newTestTask(operations.TaskRunning), loseTerminalResponse: true}
	worker := newTestWorker(
		t,
		client,
		executorFunc(func(context.Context, *operations.OperationTask, io.Writer) (operations.TaskResult, error) {
			return operations.TaskResult{}, nil
		}),
	)

	if err := worker.sync(context.Background()); err != nil {
		t.Fatalf("persisted terminal status should make a lost response successful: %v", err)
	}
	if client.task.Status.Phase != operations.TaskSucceeded {
		t.Fatalf("task phase = %s, want Succeeded", client.task.Status.Phase)
	}
}

func TestWorkerShutdownCancelsExecutorWithoutFailingTask(t *testing.T) {
	client := &fakeTaskClient{task: newTestTask(operations.TaskRunning)}
	started := make(chan struct{})
	worker := newTestWorker(
		t,
		client,
		executorFunc(func(ctx context.Context, _ *operations.OperationTask, _ io.Writer) (operations.TaskResult, error) {
			close(started)
			<-ctx.Done()
			return operations.TaskResult{}, ctx.Err()
		}),
	)

	result := make(chan error, 1)
	go func() { result <- worker.sync(worker.runCtx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("executor did not start")
	}
	worker.cancel()

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("sync error = %v, want context.Canceled", err)
	}
	if got := client.task.Status.Phase; got != operations.TaskRunning {
		t.Fatalf("task phase = %s, want Running", got)
	}
	for _, phase := range client.phases {
		if phase.IsTerminal() {
			t.Fatalf("shutdown wrote terminal phase %s", phase)
		}
	}
}

// fakeMidwayPurgeClient models a task purged while its executor runs: the
// dispatch Get succeeds and the Running update is accepted, but everything
// after (the terminal watcher's polls, the terminal status update) hits
// NotFound because the operation was finalized and its history purged.
type fakeMidwayPurgeClient struct {
	mu       sync.Mutex
	task     *operations.OperationTask
	getCalls int
	phases   []operations.TaskPhase
}

func (f *fakeMidwayPurgeClient) Get(_ context.Context, name string, _ metav1.GetOptions) (*operations.OperationTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.getCalls > 1 {
		return nil, apierrors.NewNotFound(operationTaskResource, name)
	}
	return f.task.DeepCopy(), nil
}

func (f *fakeMidwayPurgeClient) List(_ context.Context, _ *metav1.ListOptions) (*operations.OperationTaskList, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &operations.OperationTaskList{Items: []operations.OperationTask{*f.task.DeepCopy()}}, nil
}

func (*fakeMidwayPurgeClient) Watch(context.Context, *metav1.ListOptions) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}

func (f *fakeMidwayPurgeClient) UpdateStatus(
	_ context.Context,
	task *operations.OperationTask,
) (*operations.OperationTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getCalls > 1 {
		return nil, apierrors.NewNotFound(operationTaskResource, task.Name)
	}
	f.task.Status = *task.Status.DeepCopy()
	f.phases = append(f.phases, task.Status.Phase)
	f.task.ResourceVersion = string(rune(f.task.ResourceVersion[0] + 1))
	return f.task.DeepCopy(), nil
}

// fakeStaleTaskClient serves a live task and answers NotFound for a deleted
// one that a broken watch left behind in the informer store.
type fakeStaleTaskClient struct {
	mu        sync.Mutex
	task      *operations.OperationTask
	staleName string
	phases    []operations.TaskPhase
}

func (f *fakeStaleTaskClient) Get(_ context.Context, name string, _ metav1.GetOptions) (*operations.OperationTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if name == f.staleName {
		return nil, apierrors.NewNotFound(operationTaskResource, name)
	}
	return f.task.DeepCopy(), nil
}

func (f *fakeStaleTaskClient) List(_ context.Context, _ *metav1.ListOptions) (*operations.OperationTaskList, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &operations.OperationTaskList{Items: []operations.OperationTask{*f.task.DeepCopy()}}, nil
}

func (*fakeStaleTaskClient) Watch(context.Context, *metav1.ListOptions) (watch.Interface, error) {
	return watch.NewEmptyWatch(), nil
}

func (f *fakeStaleTaskClient) UpdateStatus(
	_ context.Context,
	task *operations.OperationTask,
) (*operations.OperationTask, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if task.UID != f.task.UID || task.ResourceVersion != f.task.ResourceVersion {
		return nil, apierrors.NewNotFound(operationTaskResource, task.Name)
	}
	f.task.Status = *task.Status.DeepCopy()
	f.phases = append(f.phases, task.Status.Phase)
	f.task.ResourceVersion = string(rune(f.task.ResourceVersion[0] + 1))
	return f.task.DeepCopy(), nil
}

func TestWorkerReturnsPromptlyWhenTaskPurgedAfterTerminalUpdate(t *testing.T) {
	client := &fakeTaskClient{task: newTestTask(operations.TaskPending), purgeOnTerminal: true}
	// Keep the pre-fix hang bounded: without the shutdown handoff fix,
	// execute waits for the terminal watcher until the task deadline.
	client.task.Spec.Deadline = metav1.NewTime(time.Now().Add(30 * time.Second))
	worker := newTestWorker(
		t,
		client,
		executorFunc(func(context.Context, *operations.OperationTask, io.Writer) (operations.TaskResult, error) {
			return operations.TaskResult{}, nil
		}),
	)

	done := make(chan error, 1)
	go func() { done <- worker.sync(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sync = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("sync blocked after the terminal update; execute must stop the terminal watcher instead of waiting for its poll")
	}
	if got, want := client.phases, []operations.TaskPhase{operations.TaskRunning, operations.TaskSucceeded}; len(got) != len(want) ||
		got[0] != want[0] ||
		got[1] != want[1] {
		t.Fatalf("unexpected phase updates: %#v", got)
	}
}

func TestWatchTerminalCancelsExecutorWhenTaskPurgedMidExecution(t *testing.T) {
	client := &fakeMidwayPurgeClient{task: newTestTask(operations.TaskRunning)}
	worker := newTestWorker(
		t,
		client,
		executorFunc(func(ctx context.Context, _ *operations.OperationTask, _ io.Writer) (operations.TaskResult, error) {
			<-ctx.Done()
			return operations.TaskResult{}, ctx.Err()
		}),
	)
	worker.terminalPollInterval = 20 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- worker.sync(context.Background()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sync = %v, want nil; a purged task has nothing left to report", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executor was not cancelled after the task was purged server-side")
	}
	if len(client.phases) != 0 {
		t.Fatalf("unexpected phase updates: %#v, want none after the purge", client.phases)
	}
}

func TestSyncDropsStaleSelectedTaskAndDispatchesNext(t *testing.T) {
	stale := newTestTask(operations.TaskPending)
	stale.Name = "task-stale"
	stale.UID = types.UID("task-uid-stale")
	stale.CreationTimestamp = metav1.NewTime(time.Unix(1, 0))
	live := newTestTask(operations.TaskPending)
	live.Name = "task-live"
	live.UID = types.UID("task-uid-live")
	live.CreationTimestamp = metav1.NewTime(time.Unix(2, 0))
	client := &fakeStaleTaskClient{task: live, staleName: stale.Name}
	worker := newTestWorker(
		t,
		client,
		executorFunc(func(context.Context, *operations.OperationTask, io.Writer) (operations.TaskResult, error) {
			return operations.TaskResult{}, nil
		}),
	)
	if err := worker.informer.GetStore().Add(stale.DeepCopy()); err != nil {
		t.Fatal(err)
	}

	// First sync selects the stale task (older), learns it is gone and drops
	// it from the store; the second sync dispatches the real task.
	if err := worker.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := worker.sync(context.Background()); err != nil {
		t.Fatal(err)
	}
	keys := worker.informer.GetStore().ListKeys()
	if len(keys) != 1 || keys[0] != live.Name {
		t.Fatalf("informer store keys = %#v, want only %q", keys, live.Name)
	}
	if got, want := client.phases, []operations.TaskPhase{operations.TaskRunning, operations.TaskSucceeded}; len(got) != len(want) ||
		got[0] != want[0] ||
		got[1] != want[1] {
		t.Fatalf("unexpected phase updates: %#v", got)
	}
}

func TestSelectTaskFailsClosedWithMultipleRunning(t *testing.T) {	first := newTestTask(operations.TaskRunning)
	second := newTestTask(operations.TaskRunning)
	second.Name = "task-2"
	second.UID = types.UID("task-uid-2")
	if _, err := selectTask([]*operations.OperationTask{first, second}); err == nil {
		t.Fatal("multiple Running tasks were accepted")
	}
}

func TestSelectTaskResumesRunningBeforePending(t *testing.T) {
	pending := newTestTask(operations.TaskPending)
	running := newTestTask(operations.TaskRunning)
	running.Name = "running"
	running.UID = types.UID("running")
	selected, err := selectTask([]*operations.OperationTask{pending, running})
	if err != nil {
		t.Fatal(err)
	}
	if selected.UID != running.UID {
		t.Fatalf("selected %s, want Running task %s", selected.UID, running.UID)
	}
}

func TestSelectTaskOrdersSameSecondByResourceVersion(t *testing.T) {
	first := newTestTask(operations.TaskPending)
	first.Name = "first"
	first.UID = types.UID("z-first")
	first.ResourceVersion = "2"
	second := newTestTask(operations.TaskPending)
	second.Name = "second"
	second.UID = types.UID("a-second")
	second.ResourceVersion = "3"

	selected, err := selectTask([]*operations.OperationTask{second, first})
	if err != nil {
		t.Fatal(err)
	}
	if selected.UID != first.UID {
		t.Fatalf("selected %s, want lower resourceVersion task %s", selected.UID, first.UID)
	}
}

// deadlineRecordingClient captures the contexts the worker uses for its
// one-shot List and long-lived Watch round trips.
type deadlineRecordingClient struct {
	listCtx  context.Context
	watchCtx context.Context
}

func (*deadlineRecordingClient) Get(_ context.Context, _ string, _ metav1.GetOptions) (*operations.OperationTask, error) {
	return &operations.OperationTask{}, nil
}

func (c *deadlineRecordingClient) List(ctx context.Context, _ *metav1.ListOptions) (*operations.OperationTaskList, error) {
	c.listCtx = ctx
	return &operations.OperationTaskList{}, nil
}

func (c *deadlineRecordingClient) Watch(ctx context.Context, _ *metav1.ListOptions) (watch.Interface, error) {
	c.watchCtx = ctx
	return watch.NewEmptyWatch(), nil
}

func (*deadlineRecordingClient) UpdateStatus(_ context.Context, task *operations.OperationTask) (*operations.OperationTask, error) {
	return task, nil
}

// The rest config no longer sets a client-wide timeout because it would sever
// the informer's long-lived watch on every interval. Instead the one-shot List
// carries its own deadline and the Watch inherits only the worker run context.
func TestTaskListIsBoundedAndWatchIsNot(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(NoopExecutorName, NoopExecutor{}); err != nil {
		t.Fatal(err)
	}
	logStore, err := oplog.NewOperationLog(&oplog.Options{Dir: t.TempDir(), OplogThreshold: oplog.DefaultThreshold})
	if err != nil {
		t.Fatal(err)
	}
	client := &deadlineRecordingClient{}
	w, err := NewWorker(&WorkerOptions{
		AgentID:  "agent-1",
		NodeUID:  types.UID("node-uid"),
		Client:   client,
		Registry: registry,
		OpLog:    logStore,
		LockFile: t.TempDir() + "/worker.lock",
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := w.listTasks(&metav1.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline, ok := client.listCtx.Deadline()
	if !ok {
		t.Fatal("task List context carries no deadline; a hung server would block the reflector forever")
	}
	if remaining := time.Until(deadline); remaining <= 0 || remaining > 2*serverCallTimeout {
		t.Fatalf("task List deadline remaining = %v, want within %v", remaining, 2*serverCallTimeout)
	}

	if _, err := w.watchTasks(&metav1.ListOptions{}); err != nil {
		t.Fatal(err)
	}
	if d, ok := client.watchCtx.Deadline(); ok {
		t.Fatalf("task Watch context must not carry a deadline (would sever the watch on every interval), got %v", d)
	}
	w.cancel()
	select {
	case <-client.watchCtx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("task Watch context does not follow the worker run context")
	}
}

func TestBoundedMessageKeepsValidUTF8(t *testing.T) {
	// Three-byte rune (€) truncated mid-sequence must not leak invalid bytes.
	message := "ok ok €€€€"
	bounded := boundedMessage(message)
	if bounded != message {
		t.Fatalf("short message truncated: %q", bounded)
	}
	long := strings.Repeat("€", operations.MaxMessageSize/3+10)
	bounded = boundedMessage(long)
	if utf8.ValidString(bounded) == false {
		t.Fatalf("bounded message is not valid UTF-8")
	}
	if len(bounded) > operations.MaxMessageSize {
		t.Fatalf("bounded message length = %d, want <= %d", len(bounded), operations.MaxMessageSize)
	}
}

func TestTaskResultMessageIncludesLogError(t *testing.T) {
	if got, want := taskResultMessage("command failed", errors.New("permission denied")),
		"command failed; agent task log unavailable: permission denied"; got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
	if got, want := taskResultMessage("done", nil), "done"; got != want {
		t.Fatalf("message without log error = %q, want %q", got, want)
	}
}
