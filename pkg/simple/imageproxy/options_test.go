/*
 * Copyright 2026 KubeClipper Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package imageproxy

import "testing"

func TestValidateRepositoryPrefix(t *testing.T) {
	for _, mirror := range []string{
		"", "registry.example.com", "registry.example.com:5000",
		"172.16.131.146:5249/qualified", "172.16.131.146:5249",
		"registry.example.com/team/project", "localhost:5000/team",
		"[2001:db8::1]:5000/team",
	} {
		t.Run(mirror, func(t *testing.T) {
			opts := &Options{KcImageRepoMirror: mirror}
			if errs := opts.Validate(); len(errs) != 0 {
				t.Fatalf("Validate(%q) = %v", mirror, errs)
			}
			if opts.KcImageRepoMirror != mirror {
				t.Fatalf("Validate changed repository prefix to %q", opts.KcImageRepoMirror)
			}
		})
	}
}

func TestValidateRejectsInvalidRepositoryPrefix(t *testing.T) {
	for _, mirror := range []string{
		"http://registry.example.com/team", "https://registry.example.com/team",
		"registry.example.com/team:latest", "registry.example.com/team@sha256:abcd",
		" ", " registry.example.com", "registry.example.com/team project",
		"registry.example.com:bad/team", "registry.example.com/team?query=1",
		"registry.example.com/team#fragment", "registry.example.com/UPPER",
		"registry.example.com//team", "registry.example.com/../team",
	} {
		t.Run(mirror, func(t *testing.T) {
			if errs := (&Options{KcImageRepoMirror: mirror}).Validate(); len(errs) == 0 {
				t.Fatalf("Validate(%q) accepted invalid repository prefix", mirror)
			}
		})
	}
}
