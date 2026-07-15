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

package autodetection

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

func TestFilteredEnumeration(t *testing.T) {
	enumeration, c, err := FilteredEnumeration(nil, nil, nil, 4)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(*enumeration)
	fmt.Println(*c)
}

func TestDefaultInterfacesExcludeContainerBridges(t *testing.T) {
	excluded := regexp.MustCompile("(" + strings.Join(DefaultInterfacesToExclude, ")|(") + ")")
	for _, name := range []string{"docker0", "br-19bb2f9f61b0", "cni0", "cbr0", "podman0", "nerdctl0", "virbr0"} {
		if !excluded.MatchString(name) {
			t.Errorf("container bridge %q is not excluded from first-found detection", name)
		}
	}
	for _, name := range []string{"eth0", "ens192", "bond0"} {
		if excluded.MatchString(name) {
			t.Errorf("host interface %q is unexpectedly excluded", name)
		}
	}
}
