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
	"strings"
	"syscall"
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

func TestWriteToFileIgnoresUmask(t *testing.T) {
	// syscall.Umask is process-wide; the Chmod calls in WriteToFile make the
	// final mode umask-independent, and this package runs no parallel tests.
	old := syscall.Umask(000)
	defer syscall.Umask(old)

	dir := t.TempDir()
	path := filepath.Join(dir, "deploy-config.yaml")

	if err := WriteToFile(path, []byte("secret")); err != nil {
		t.Fatalf("WriteToFile failed: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat failed: %v", err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("umask 000 produced perm %v, want 0600", got)
	}
}

func TestWriteToFileRefusesSymlinkTarget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deploy-config.yaml")
	if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
		t.Fatalf("seed file failed: %v", err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatalf("symlink failed: %v", err)
	}

	err := WriteToFile(link, []byte("clobber"))
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("want symlink refusal, got %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read link target failed: %v", err)
	}
	if string(data) != "original" {
		t.Fatalf("link target was modified: content = %q", data)
	}
	assertNoTempLeftovers(t, dir)
}

func TestWriteToFileFailureKeepsOriginal(t *testing.T) {
	dir := t.TempDir()
	// The target is a directory: Lstat passes, the temp write succeeds, and
	// the final rename over the directory fails — the original must stay
	// intact and no temp file may be left behind.
	path := filepath.Join(dir, "deploy-config.yaml")
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(path, "sentinel")
	if err := os.WriteFile(sentinel, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := WriteToFile(path, []byte("never lands")); err == nil {
		t.Fatal("want error when target is a directory")
	}
	data, err := os.ReadFile(sentinel)
	if err != nil {
		t.Fatalf("read sentinel failed: %v", err)
	}
	if string(data) != "original" {
		t.Fatalf("original content lost: %q", data)
	}
	assertNoTempLeftovers(t, dir)
}

func assertNoTempLeftovers(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir failed: %v", err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".kc-write-") {
			t.Fatalf("temporary file leftover: %s", e.Name())
		}
	}
}
