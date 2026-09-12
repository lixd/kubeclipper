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

package k8s

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	componentcommon "github.com/kubeclipper/kubeclipper/pkg/component/common"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryfetcher "github.com/kubeclipper/kubeclipper/pkg/delivery/fetcher"
	"github.com/kubeclipper/kubeclipper/pkg/logger"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
	"github.com/kubeclipper/kubeclipper/pkg/utils/cmdutil"
	"github.com/kubeclipper/kubeclipper/pkg/utils/strutil"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func init() {
	if err := component.RegisterAgentStep(fmt.Sprintf(component.RegisterStepKeyFormat, extension, extensionVersion, component.TypeStep), &Extension{}); err != nil {
		panic(err)
	}
}

var (
	_ component.StepRunnable = (*Extension)(nil)
)

const (
	k8sExtension     = "k8s-extension"
	extensionVersion = "v1"
	extension        = "extension"
)

// Extension is a step to install k8s extension,which include some useful tools
type Extension struct {
	Offline       bool                           `json:"offline"`
	Version       string                         `json:"version"`
	Arch          string                         `json:"arch,omitempty"`
	CriType       string                         `json:"criType"`
	ImageRegistry string                         `json:"imageRegistry"`
	Transport     deliveryapis.TransportRef      `json:"transport,omitempty"`
	Contents      []deliveryapis.ArtifactContent `json:"contents,omitempty"`
}

func (stepper *Extension) NewInstance() component.ObjectMeta {
	return &Extension{}
}

func (stepper *Extension) Install(ctx context.Context, opts component.Options) ([]byte, error) {
	if stepper.Transport.Type != "" {
		if err := stepper.installResolved(ctx, opts); err != nil {
			return nil, err
		}
		logger.Debug("k8s extension resolved install successfully")
		return nil, nil
	}
	if resolved, ok := componentcommon.FindResolvedComponent(component.GetResolvedArtifactPlan(ctx), k8sExtension, k8sExtension, stepper.Version); ok {
		stepper.Arch = resolved.Arch
		stepper.Transport = resolved.Transport
		stepper.Contents = resolved.Contents
		if err := stepper.installResolved(ctx, opts); err != nil {
			return nil, err
		}
		logger.Debug("k8s extension resolved install successfully")
		return nil, nil
	}
	return nil, fmt.Errorf("install k8s extension %s requires resolved OCI artifact transport", stepper.Version)
}

func (stepper *Extension) installResolved(ctx context.Context, opts component.Options) error {
	contents := packageConfigContents(stepper.Contents)
	result, err := deliveryfetcher.FetchComponent(ctx, runtime.GOARCH, deliveryapis.ResolvedComponent{
		Kind:      k8sExtension,
		Name:      k8sExtension,
		Version:   stepper.Version,
		Arch:      stepper.Arch,
		Transport: stepper.Transport,
		Contents:  contents,
	}, opts.DryRun)
	if err != nil {
		return err
	}
	configs := result.Files[deliveryapis.ContentConfigs]
	if configs == "" {
		return fmt.Errorf("resolved k8s extension configs content is missing")
	}
	_, err = cmdutil.RunCmdWithContext(ctx, opts.DryRun, "bash", "-c", fmt.Sprintf("tar -zxvf %s -C /", configs))
	return err
}

func (stepper *Extension) Uninstall(ctx context.Context, opts component.Options) ([]byte, error) {
	// remove related binary configuration files
	if err := downloader.CleanupPackage(k8sExtension, k8sExtension, stepper.Version, archOrRuntime(stepper.Arch), opts.DryRun); err != nil {
		logger.Error("remove k8s configs and images compressed files failed", zap.Error(err))
	}
	return nil, nil
}

func (stepper *Extension) InitStepper(c *v1.Cluster) *Extension {
	stepper.Offline = c.Offline()
	stepper.Version = extensionVersion
	stepper.CriType = c.ContainerRuntime.Type
	stepper.ImageRegistry = c.ResolvedImageRegistry
	return stepper
}

func (stepper *Extension) InstallSteps(nodes []v1.StepNode) ([]v1.Step, error) {
	return stepper.InstallStepsWithContext(context.TODO(), nodes)
}

func (stepper *Extension) InstallStepsWithContext(ctx context.Context, nodes []v1.StepNode) ([]v1.Step, error) {
	resolved, err := componentcommon.RequireResolvedComponent(
		component.GetResolvedArtifactPlan(ctx), k8sExtension, k8sExtension, stepper.Version,
	)
	if err != nil {
		return nil, err
	}
	stepper.Arch = resolved.Arch
	stepper.Transport = resolved.Transport
	stepper.Contents = resolved.Contents
	bytes, err := json.Marshal(&stepper)
	if err != nil {
		return nil, err
	}

	return []v1.Step{
		{
			ID:         strutil.GetUUID(),
			Name:       "installExtension",
			Timeout:    metav1.Duration{Duration: 10 * time.Minute},
			ErrIgnore:  true,
			RetryTimes: 1,
			Nodes:      nodes,
			Action:     v1.ActionInstall,
			Commands: []v1.Command{
				{
					Type:          v1.CommandCustom,
					Identity:      fmt.Sprintf(component.RegisterStepKeyFormat, extension, extensionVersion, component.TypeStep),
					CustomCommand: bytes,
				},
			},
		},
	}, nil
}

func (stepper *Extension) UninstallSteps(nodes []v1.StepNode) ([]v1.Step, error) {
	bytes, err := json.Marshal(stepper)
	if err != nil {
		return nil, err
	}

	return []v1.Step{
		{
			ID:         strutil.GetUUID(),
			Name:       "unInstallExtension",
			Timeout:    metav1.Duration{Duration: 10 * time.Minute},
			ErrIgnore:  true,
			RetryTimes: 1,
			Nodes:      nodes,
			Action:     v1.ActionUninstall,
			Commands: []v1.Command{
				{
					Type:          v1.CommandCustom,
					Identity:      fmt.Sprintf(component.RegisterStepKeyFormat, extension, extensionVersion, component.TypeStep),
					CustomCommand: bytes,
				},
			},
		},
	}, nil
}
