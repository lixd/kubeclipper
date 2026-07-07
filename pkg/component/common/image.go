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
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	"github.com/kubeclipper/kubeclipper/pkg/component/utils"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryfetcher "github.com/kubeclipper/kubeclipper/pkg/delivery/fetcher"
	"github.com/kubeclipper/kubeclipper/pkg/logger"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
	"github.com/kubeclipper/kubeclipper/pkg/utils/strutil"
)

const (
	imageName  = "image"
	AgentImage = "AgentImager"
)

func init() {
	if err := component.RegisterAgentStep(fmt.Sprintf(component.RegisterStepKeyFormat, imageName, version, AgentImage), &Imager{}); err != nil {
		panic(err)
	}
}

type Imager struct {
	Kind      string                         `json:"kind,omitempty"`
	PkgName   string                         `json:"pkgName"`
	Version   string                         `json:"version"`
	Arch      string                         `json:"arch,omitempty"`
	Offline   bool                           `json:"offline"`
	CriName   string                         `json:"criName"`
	Transport deliveryapis.TransportRef      `json:"transport,omitempty"`
	Contents  []deliveryapis.ArtifactContent `json:"contents,omitempty"`
}

func (i *Imager) Install(ctx context.Context, opts component.Options) ([]byte, error) {
	return nil, fmt.Errorf("install %s-%s image from tarball has been removed; pre-populate image registry instead", i.PkgName, i.Version)
}

func (i *Imager) downloadResolvedImage(ctx context.Context, opts component.Options) (string, error) {
	contents := i.Contents
	if len(contents) == 0 {
		return "", fmt.Errorf("resolved image contents are required")
	}
	result, err := deliveryfetcher.FetchComponent(ctx, runtime.GOARCH, deliveryapis.ResolvedComponent{
		Kind:      i.ArtifactKind(),
		Name:      i.PkgName,
		Version:   i.Version,
		Arch:      i.Arch,
		Transport: i.Transport,
		Contents:  contents,
	}, opts.DryRun)
	if err != nil {
		return "", err
	}
	path := result.Files[deliveryapis.ContentImages]
	if path == "" {
		return "", fmt.Errorf("resolved image content is missing")
	}
	return path, nil
}

func (i *Imager) Uninstall(ctx context.Context, opts component.Options) ([]byte, error) {
	if err := downloader.CleanupImages(i.ArtifactKind(), i.PkgName, i.Version, i.ArtifactPlatform(), opts.DryRun); err != nil {
		logger.Errorf("remove %s-%s images compressed file failed", i.PkgName, i.Version, zap.Error(err))
	}

	return nil, nil
}

func (i *Imager) ArtifactKind() string {
	if i.Kind != "" {
		return i.Kind
	}
	return imageName
}

func (i *Imager) ArtifactPlatform() string {
	if i.Kind != "" {
		return deliveryapis.DefaultPackageOS + "-" + archOrRuntime(i.Arch)
	}
	return archOrRuntime(i.Arch)
}

func archOrRuntime(arch string) string {
	if arch != "" {
		return arch
	}
	return runtime.GOARCH
}

func (i *Imager) NewInstance() component.ObjectMeta {
	return &Imager{}
}

func (i *Imager) InstallSteps(nodeList component.NodeList) ([]v1.Step, error) {
	customCommand, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}
	return []v1.Step{
		{
			ID:         strutil.GetUUID(),
			Name:       fmt.Sprintf("%s-imageLoad", i.PkgName),
			Timeout:    metav1.Duration{Duration: 30 * time.Minute},
			ErrIgnore:  false,
			RetryTimes: 0,
			Nodes:      utils.UnwrapNodeList(nodeList),
			Action:     v1.ActionInstall,
			Commands: []v1.Command{
				{
					Type:          v1.CommandCustom,
					Identity:      fmt.Sprintf(component.RegisterTemplateKeyFormat, imageName, version, AgentImage),
					CustomCommand: customCommand,
				},
			},
		},
	}, nil
}
