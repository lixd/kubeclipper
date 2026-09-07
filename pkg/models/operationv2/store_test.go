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
	"sort"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	operations "github.com/kubeclipper/kubeclipper/pkg/scheme/operations/v1alpha1"
)

func TestOperationOrderingAndContinueToken(t *testing.T) {
	first := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	second := first.Add(time.Minute)
	items := []operations.Operation{
		{ObjectMeta: metav1.ObjectMeta{Name: "old", UID: types.UID("uid-old"), CreationTimestamp: metav1.NewTime(first)}},
		{ObjectMeta: metav1.ObjectMeta{Name: "newer-b", UID: types.UID("uid-newer-b"), CreationTimestamp: metav1.NewTime(second)}},
		{ObjectMeta: metav1.ObjectMeta{Name: "newer-a", UID: types.UID("uid-newer-a"), CreationTimestamp: metav1.NewTime(second)}},
	}

	sort.SliceStable(items, func(i, j int) bool {
		return operationNewer(&items[i], &items[j])
	})
	if got := []string{items[0].Name, items[1].Name, items[2].Name}; got[0] != "newer-b" || got[1] != "newer-a" || got[2] != "old" {
		t.Fatalf("ordered operations = %v", got)
	}

	tokenValue, err := encodeOperationContinue(operationContinueToken{
		Version:           operationContinueTokenVersion,
		ResourceVersion:   "42",
		CreationTimestamp: items[1].CreationTimestamp.Time,
		UID:               items[1].UID,
	})
	if err != nil {
		t.Fatal(err)
	}
	token, err := decodeOperationContinue(tokenValue)
	if err != nil {
		t.Fatal(err)
	}
	start := sort.Search(len(items), func(i int) bool {
		return operationAfterCursor(&items[i], token)
	})
	if start != 2 || items[start].Name != "old" {
		t.Fatalf("next page starts at %d with %q", start, items[start].Name)
	}

	if _, err := decodeOperationContinue("not-a-token"); !apierrors.IsBadRequest(err) {
		t.Fatalf("invalid token error = %v, want BadRequest", err)
	}
}
