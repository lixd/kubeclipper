/*
 *
 * Copyright 2021 KubeClipper Authors.
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
 *
 */

package utils

import (
	"errors"
	"os"
	"reflect"
	"testing"
)

type recordedModeCopy struct {
	host       string
	localPath  string
	remotePath string
	mode       os.FileMode
}

type recordingModeCopier struct {
	copies   []recordedModeCopy
	failHost string
}

func (r *recordingModeCopier) CopySudoMode(host, localPath, remotePath string, mode os.FileMode) error {
	r.copies = append(r.copies, recordedModeCopy{host: host, localPath: localPath, remotePath: remotePath, mode: mode})
	if host == r.failHost {
		return errors.New("copy failed")
	}
	return nil
}

func TestCopyFileToHostsWithMode(t *testing.T) {
	copier := &recordingModeCopier{}
	err := CopyFileToHostsWithMode(copier, "/tmp/ca.key", []string{"10.0.0.1", "10.0.0.2"}, "/etc/kubeclipper/pki", PrivateFileMode)
	if err != nil {
		t.Fatalf("CopyFileToHostsWithMode() error = %v", err)
	}
	want := []recordedModeCopy{
		{host: "10.0.0.1", localPath: "/tmp/ca.key", remotePath: "/etc/kubeclipper/pki/ca.key", mode: 0600},
		{host: "10.0.0.2", localPath: "/tmp/ca.key", remotePath: "/etc/kubeclipper/pki/ca.key", mode: 0600},
	}
	if !reflect.DeepEqual(copier.copies, want) {
		t.Fatalf("copies = %#v, want %#v", copier.copies, want)
	}
}

func TestCopyFileToHostsWithModeStopsAfterFailure(t *testing.T) {
	copier := &recordingModeCopier{failHost: "10.0.0.2"}
	err := CopyFileToHostsWithMode(copier, "/tmp/client.key", []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}, "/etc/pki", PrivateFileMode)
	if err == nil {
		t.Fatal("CopyFileToHostsWithMode() expected error")
	}
	if len(copier.copies) != 2 {
		t.Fatalf("copy attempts = %d, want 2", len(copier.copies))
	}
}
