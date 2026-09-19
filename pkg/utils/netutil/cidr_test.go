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

package netutil

import (
	"strings"
	"testing"
)

func TestValidateSubnetOverlap(t *testing.T) {
	cases := []struct {
		name    string
		pods    []string
		svcs    []string
		wantErr string
	}{
		{
			name: "disjoint defaults",
			pods: []string{"172.25.0.0/16"},
			svcs: []string{"10.96.0.0/12"},
		},
		{
			name: "identical subnets",
			pods: []string{"10.96.0.0/16"},
			svcs: []string{"10.96.0.0/12"},
			// 10.96.0.0/12 contains 10.96.0.0/16
			wantErr: "overlaps",
		},
		{
			name: "service inside pod range",
			pods: []string{"10.0.0.0/8"},
			svcs: []string{"10.96.0.0/12"},
			wantErr: "overlaps",
		},
		{
			name: "duplicate pod subnets",
			pods: []string{"172.25.0.0/16", "172.25.0.0/16"},
			svcs: []string{"10.96.0.0/12"},
			wantErr: "duplicate",
		},
		{
			name: "invalid cidr",
			pods: []string{"not-a-cidr"},
			svcs: []string{"10.96.0.0/12"},
			wantErr: "invalid pod subnet",
		},
		{
			name: "empty pods",
			pods: nil,
			svcs: []string{"10.96.0.0/12"},
			wantErr: "required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateSubnetOverlap(tc.pods, tc.svcs)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err, tc.wantErr)
			}
		})
	}
}
