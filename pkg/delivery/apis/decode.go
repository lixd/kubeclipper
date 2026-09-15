/*
 *
 *  * Copyright 2024 KubeClipper Authors.
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

package apis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"sigs.k8s.io/yaml"
)

func DecodeSupportPolicy(data []byte, policy *SupportPolicy) error {
	if policy == nil {
		return fmt.Errorf("support policy target is nil")
	}
	jsonData, err := yaml.YAMLToJSONStrict(data)
	if err != nil {
		return fmt.Errorf("decode support policy failed: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(jsonData))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(policy); err != nil {
		return fmt.Errorf("decode support policy failed: %w", err)
	}
	if err = decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("decode support policy failed: multiple JSON documents")
		}
		return fmt.Errorf("decode support policy failed: %w", err)
	}
	return nil
}
