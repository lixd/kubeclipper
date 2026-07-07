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

package common

import (
	"fmt"

	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
)

func ApplyResolvedComponent(target interface{}, component deliveryapis.ResolvedComponent) error {
	switch v := target.(type) {
	case *Imager:
		v.Kind = component.Kind
		v.PkgName = component.Name
		v.Version = component.Version
		v.Arch = component.Arch
		v.Transport = component.Transport
		v.Contents = component.Contents
		return nil
	case *Chart:
		v.Kind = component.Kind
		v.PkgName = component.Name
		v.Version = component.Version
		v.Arch = component.Arch
		v.Transport = component.Transport
		v.Contents = component.Contents
		return nil
	default:
		return fmt.Errorf("unsupported resolved component target %T", target)
	}
}

func FindResolvedComponent(plan interface{}, kind, name, version string) (deliveryapis.ResolvedComponent, bool) {
	resolvedPlan, ok := plan.(*deliveryapis.ResolvedArtifactPlan)
	if !ok || resolvedPlan == nil {
		return deliveryapis.ResolvedComponent{}, false
	}
	for _, component := range resolvedPlan.Components {
		if component.Kind == kind && component.Name == name && component.Version == version {
			return component, true
		}
	}
	return deliveryapis.ResolvedComponent{}, false
}

func RequireResolvedComponent(plan interface{}, kind, name, version string) (deliveryapis.ResolvedComponent, error) {
	resolved, ok := FindResolvedComponent(plan, kind, name, version)
	if !ok {
		return deliveryapis.ResolvedComponent{}, fmt.Errorf("resolved component %s/%s:%s is required", kind, name, version)
	}
	return resolved, nil
}
