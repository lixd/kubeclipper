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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	"github.com/kubeclipper/kubeclipper/pkg/component/utils"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
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
	PkgName string `json:"pkgName"`
	Version string `json:"version"`
	Offline bool   `json:"offline"`
	CriName string `json:"criName"`
	// Optional. If the value of the change field is not empty, the DownloadCustomImages and RemoveCustomImages operations will be performed
	CustomImageList []string `json:"customConfig"`
}

func (i *Imager) Install(ctx context.Context, opts component.Options) ([]byte, error) {
	return nil, fmt.Errorf("image archive tasks were removed; configure an image registry for %s", i.PkgName)
}

func (i *Imager) Uninstall(ctx context.Context, opts component.Options) ([]byte, error) {
	return nil, nil
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
