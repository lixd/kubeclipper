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

package cni

import (
	"regexp"
	"strings"
)

// iptables-save quotes comments. Treat quoted strings as single tokens so a
// comment containing "-j KUBE-..." cannot turn a host rule into a cluster rule.
var ruleTokens = regexp.MustCompile(`"(?:\\.|[^"\\])*"|[^\s"]+`)

func clusterChain(name string) bool {
	return strings.HasPrefix(name, "KUBE-") || strings.HasPrefix(name, "cali-")
}

// Build a deletion-only restore transaction. --noflush retains host rules,
// policies and chains, including rules installed since the save snapshot.
func clusterRuleDeletion(saved string) string {
	var result, table strings.Builder
	var chains []string
	for _, line := range strings.Split(saved, "\n") {
		if strings.HasPrefix(line, "*") {
			table.Reset()
			table.WriteString(line + "\n")
			chains = nil
		} else if strings.HasPrefix(line, ":") {
			fields := strings.Fields(line)
			if clusterChain(strings.TrimPrefix(fields[0], ":")) {
				chains = append(chains, strings.TrimPrefix(fields[0], ":"))
			}
		} else if strings.HasPrefix(line, "-A ") {
			fields := ruleTokens.FindAllString(line, -1)
			if len(fields) < 2 || clusterChain(fields[1]) {
				continue
			}
			for i := 2; i+1 < len(fields); i++ {
				if (fields[i] == "-j" || fields[i] == "-g") && clusterChain(fields[i+1]) {
					table.WriteString("-D" + line[2:] + "\n")
					break
				}
			}
		} else if line == "COMMIT" && len(chains) > 0 {
			for _, chain := range chains {
				table.WriteString("-F " + chain + "\n")
			}
			for _, chain := range chains {
				table.WriteString("-X " + chain + "\n")
			}
			table.WriteString("COMMIT\n")
			result.WriteString(table.String())
		}
	}
	return result.String()
}
