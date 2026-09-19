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

package utils

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteToFileCreatesWithTightPermissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "deploy-config.yaml")

	if err := WriteToFile(path, []byte("sensitive: true")); err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("file perm = %v, want 0600", got)
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat dir failed: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0700 {
		t.Fatalf("dir perm = %v, want 0700", got)
	}
}

func TestWriteToFileTightensExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deploy-config.yaml")

	if err := os.WriteFile(path, []byte("old"), 0644); err != nil {
		t.Fatalf("seed file failed: %v", err)
	}

	if err := WriteToFile(path, []byte("new")); err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("existing file perm = %v, want tightened to 0600", got)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(data) != "new" {
		t.Fatalf("content = %q, want %q", data, "new")
	}
}
