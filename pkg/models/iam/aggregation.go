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

package iam

import (
	"context"
	"encoding/json"

	"go.uber.org/zap"
	apimachineryErrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/kubeclipper/kubeclipper/pkg/logger"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/common"
	iamv1 "github.com/kubeclipper/kubeclipper/pkg/scheme/iam/v1"
)

// FetchAggregationRoles resolves the template roles referenced by the role's
// kubeclipper.io/aggregation-roles annotation. Templates listed in the
// annotation but missing from storage are skipped with a warning; a non-nil
// error means either a malformed annotation or a storage failure.
func FetchAggregationRoles(ctx context.Context, reader RoleReaderEx, role *iamv1.GlobalRole) ([]*iamv1.GlobalRole, error) {
	roles := make([]*iamv1.GlobalRole, 0)

	if v := role.Annotations[common.AnnotationAggregationRoles]; v != "" {
		var roleNames []string
		if err := json.Unmarshal([]byte(v), &roleNames); err != nil {
			return nil, err
		}

		for _, roleName := range roleNames {
			r, err := reader.GetRoleEx(ctx, roleName, "0")
			if err != nil {
				if apimachineryErrors.IsNotFound(err) {
					logger.Warn("aggregation role invalid", zap.String("role", role.Name),
						zap.String("aggregation_role", roleName))
					continue
				}
				return nil, err
			}
			roles = append(roles, r)
		}
	}
	return roles, nil
}
