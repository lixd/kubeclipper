/*
 *
 *  * Copyright 2026 KubeClipper Authors.
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

package create

import "testing"

func TestUseDeliveryPolicyResolution(t *testing.T) {
	tests := []struct {
		name          string
		offline       bool
		localRegistry string
		want          bool
	}{
		{name: "offline registry", offline: true, localRegistry: "registry.local:5000", want: true},
		{name: "online registry", offline: false, localRegistry: "registry.local:5000", want: false},
		{name: "offline without registry", offline: true, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := &CreateClusterOptions{Offline: test.offline, LocalRegistry: test.localRegistry}
			if got := opts.useDeliveryPolicyResolution(); got != test.want {
				t.Fatalf("useDeliveryPolicyResolution() = %v, want %v", got, test.want)
			}
		})
	}
}
