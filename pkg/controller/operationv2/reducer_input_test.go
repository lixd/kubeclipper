/*
 *
 *  * Copyright 2021 KubeClipper Authors.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package operationv2

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/types"

	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

func srcStep(id string, succeeded bool) operations.OperationStep {
	return operations.OperationStep{ID: id}
}

func dependentStep(fromStep string) operations.OperationStep {
	return operations.OperationStep{
		ID: fromStep + "-dep",
		Inputs: []operations.StepInput{{
			Field: "lastTaskReply", FromStepID: fromStep, FromNodeUID: types.UID("node-1"), OutputKey: "response",
		}},
	}
}

func taskFor(stepID string, phase operations.TaskPhase, outputs map[string]string) operations.OperationTask {
	t := operations.OperationTask{}
	t.Spec.StepID = stepID
	t.Spec.NodeRef = operations.NodeReference{Name: "node-1", UID: "node-1"}
	t.Status.Phase = phase
	if outputs != nil {
		t.Status.Result = &operations.TaskResult{Outputs: outputs}
	}
	return t
}

func TestMaterializePayloadErrIgnoreSourceFailure(t *testing.T) {
	failed := taskFor("s1", operations.TaskFailed, nil)

	src := operations.OperationStep{ID: "s1", ErrIgnore: true}
	dep := dependentStep("s1")
	got, err := materializePayload(&dep, []operations.OperationStep{src, dep}, []operations.OperationTask{failed})
	if err != nil {
		t.Fatalf("ErrIgnore source must not fail materialization: %v", err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(got.Raw, &payload); err != nil {
		t.Fatal(err)
	}
	if string(payload["lastTaskReply"]) != `""` {
		t.Fatalf("want empty string reply, got %s", payload["lastTaskReply"])
	}

	strict := dependentStep("s1") // source without ErrIgnore
	if _, err := materializePayload(&strict, []operations.OperationStep{{ID: "s1"}, strict}, []operations.OperationTask{failed}); err == nil {
		t.Fatal("non-ErrIgnore failed source must still error")
	}

	ok := taskFor("s1", operations.TaskSucceeded, map[string]string{"response": "/tmp/x.yaml"})
	pass := dependentStep("s1")
	got, err = materializePayload(&pass, []operations.OperationStep{src, pass}, []operations.OperationTask{ok})
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(got.Raw, &payload); err != nil {
		t.Fatal(err)
	}
	if string(payload["lastTaskReply"]) != `"/tmp/x.yaml"` {
		t.Fatalf("successful source output must be used, got %s", payload["lastTaskReply"])
	}
}
