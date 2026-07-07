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

package apis

import "context"

const (
	DeliveryPolicyConfigMapName = "kubeclipper-delivery-policy"
	DeliveryPolicyConfigMapKey  = "policy.json"
)

type InventoryStore interface {
	Get(ctx context.Context) (*PackageInventory, error)
}

type PolicyStore interface {
	Get(ctx context.Context) (*SupportPolicy, error)
	Update(ctx context.Context, mutator func(*SupportPolicy) error) error
}
