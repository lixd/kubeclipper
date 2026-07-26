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

package k8s

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRemoveKubeletConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("config"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := removeKubeletConfig(path, true); err != nil {
		t.Fatalf("dry-run remove: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("dry-run removed config: %v", err)
	}

	if err := removeKubeletConfig(path, false); err != nil {
		t.Fatalf("remove existing config: %v", err)
	}
	if err := removeKubeletConfig(path, false); err != nil {
		t.Fatalf("remove missing config should be idempotent: %v", err)
	}
}
