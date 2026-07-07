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

package cni

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	componentcommon "github.com/kubeclipper/kubeclipper/pkg/component/common"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
)

var cniFactories = make(map[string]CniFactory)

type CniFactory interface {
	Type() string
	Create() Stepper
}

func Register(factory CniFactory) {
	cniFactories[factory.Type()] = factory
}

func Load(cniType string) (CniFactory, error) {
	if _, ok := cniFactories[cniType]; !ok {
		return nil, errors.New("this cni is not supported at this time")
	}
	return cniFactories[cniType], nil
}

const (
	version     = "v1"
	cniInfo     = "cniInfo"
	manifestDir = "/tmp/.cni"
)

type BaseCni struct {
	v1.CNI
	DualStack   bool                           `json:"dualStack"`
	PodIPv4CIDR string                         `json:"podIPv4CIDR"`
	PodIPv6CIDR string                         `json:"podIPv6CIDR"`
	Arch        string                         `json:"arch,omitempty"`
	Transport   deliveryapis.TransportRef      `json:"transport,omitempty"`
	Contents    []deliveryapis.ArtifactContent `json:"contents,omitempty"`
}

type Stepper interface {
	InitStep(metadata *component.ExtraMetadata, cni *v1.CNI, networking *v1.Networking) Stepper
	PrepareImages(ctx context.Context, nodes []v1.StepNode) ([]v1.Step, error)
	InstallSteps(nodes []v1.StepNode, kubeVersion string) ([]v1.Step, error)
	UninstallSteps(nodes []v1.StepNode) ([]v1.Step, error)
	CmdList(namespace string) map[string]string
}

func (runnable *BaseCni) NewInstance() component.ObjectMeta {
	return &BaseCni{}
}

func (runnable *BaseCni) Install(ctx context.Context, opts component.Options) ([]byte, error) {
	if runnable.Offline && strings.TrimSpace(runnable.LocalRegistry) == "" {
		return nil, fmt.Errorf("offline %s install requires localRegistry; image tarball loading has been removed", runnable.Type)
	}
	return nil, nil
}

func applyResolvedCNI(ctx context.Context, base *BaseCni, name string) {
	resolved, ok := componentcommon.FindResolvedComponent(component.GetResolvedArtifactPlan(ctx), "cni", name, base.Version)
	if ok {
		base.Arch = resolved.Arch
		base.Transport = resolved.Transport
		base.Contents = resolved.Contents
	}
}

func (runnable *BaseCni) Uninstall(ctx context.Context, opts component.Options) ([]byte, error) {
	return nil, nil
}

func archOrRuntime(arch string) string {
	if arch != "" {
		return deliveryapis.DefaultPackageOS + "-" + arch
	}
	return deliveryapis.DefaultPackageOS + "-" + runtime.GOARCH
}

// RecoveryCNICmd get recovery cni cmd
func RecoveryCNICmd(metadata *component.ExtraMetadata) (cmdList map[string]string, err error) {
	c, err := Load(metadata.CNI)
	if err != nil {
		return
	}
	if metadata.CNINamespace == "" {
		err = errors.New("the namespace of cni is empty")
		return
	}

	return c.Create().CmdList(metadata.CNINamespace), nil
}
