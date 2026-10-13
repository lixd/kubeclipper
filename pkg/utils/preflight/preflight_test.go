/*
 * Copyright 2026 KubeClipper Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package preflight

import (
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDiskCommand(t *testing.T) {
	for _, path := range []string{filepath.Join(t.TempDir(), "not-created"), filepath.Join(t.TempDir(), "quote' space")} {
		output, err := exec.Command("bash", "-c", DiskCommand(path)).CombinedOutput()
		if err != nil {
			t.Fatalf("existing parent filesystem check: %v: %s", err, output)
		}
	}
	output, err := exec.Command("bash", "-c", DiskCommand("relative")).CombinedOutput()
	if err == nil || !strings.Contains(string(output), "absolute path") {
		t.Fatalf("relative path was not rejected: %v %s", err, output)
	}
}

func TestTimeSyncCommandChecksSynchronization(t *testing.T) {
	if !strings.Contains(TimeSyncCommand, "chronyc waitsync 1 5") || !strings.Contains(TimeSyncCommand, "NTPSynchronized") || !strings.Contains(TimeSyncCommand, "timeout 3") {
		t.Fatal("time check must verify provider synchronization with a bounded timeout")
	}
}
