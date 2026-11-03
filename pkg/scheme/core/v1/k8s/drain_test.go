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
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubeclipper/kubeclipper/pkg/component"
)

func TestDrainReadyNodePreservesEvictionProtection(t *testing.T) {
	for _, blocked := range []bool{false, true} {
		name := "successful eviction"
		if blocked {
			name = "disruption budget blocks eviction"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			log := filepath.Join(dir, "commands")
			t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
			t.Setenv("DRAIN_COMMAND_LOG", log)
			t.Setenv("DRAIN_REJECT", "false")
			if blocked {
				t.Setenv("DRAIN_REJECT", "true")
			}
			script := `#!/bin/sh
printf '%s\n' "$*" >> "$DRAIN_COMMAND_LOG"
case "$1" in
get) if [ "$4" = "-o" ]; then printf True; fi ;;
drain) if [ "$DRAIN_REJECT" = true ]; then
  printf '%s\n' 'Cannot evict pod as it would violate the disruption budget.' >&2
  exit 1
fi ;;
esac
`
			if err := os.WriteFile(filepath.Join(dir, "kubectl"), []byte(script), 0755); err != nil {
				t.Fatal(err)
			}
			drain := (&Drain{}).InitStepper("worker-1", []string{"--ignore-daemonsets", "--timeout=5m"})
			_, err := drain.Uninstall(context.Background(), component.Options{})
			if blocked && (err == nil || !strings.Contains(err.Error(), "disruption budget")) {
				t.Fatalf("blocked eviction error = %v", err)
			}
			if !blocked && err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			commands := string(data)
			if strings.Contains(commands, "taint ") || strings.Contains(commands, "--disable-eviction") {
				t.Fatalf("drain must not bypass eviction protection:\n%s", commands)
			}
			if !strings.Contains(commands, "drain worker-1 ") {
				t.Fatalf("missing drain command:\n%s", commands)
			}
			if deleted := strings.Contains(commands, "delete node worker-1"); deleted == blocked {
				t.Fatalf("node deletion after blocked=%t:\n%s", blocked, commands)
			}
		})
	}
}
