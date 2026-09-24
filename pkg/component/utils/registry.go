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

package utils

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pelletier/go-toml"

	"github.com/kubeclipper/kubeclipper/pkg/utils/cmdutil"
)

var containerdDefaultConfig = "/etc/containerd/config.toml"

func AddOrRemoveInsecureRegistryToCRI(ctx context.Context, criType, registry string, add, dryRun bool) error {
	switch criType {
	case "containerd":
		return addOrRemoveContainerdInsecureRegistry(ctx, registry, add, dryRun)
	default:
		return fmt.Errorf("%s CRI is not supported", criType)
	}
}

func addOrRemoveContainerdInsecureRegistry(ctx context.Context, registry string, add, dryRun bool) (err error) {
	if dryRun {
		return
	}
	info, err := os.Stat(containerdDefaultConfig)
	if err != nil {
		return
	}
	// load toml config file
	conf, err := toml.LoadFile(containerdDefaultConfig)
	if err != nil {
		return
	}
	pluginPrefix := "io.containerd.grpc.v1.cri"
	if v := conf.Get("version"); v != nil {
		if n, ok := v.(int64); ok && int(n) >= 3 {
			pluginPrefix = "io.containerd.cri.v1.images"
		}
	}
	var logMsg string
	registryTree := conf.GetPath([]string{"plugins", pluginPrefix, "registry"})
	insecureRegistries, ok := registryTree.(*toml.Tree)
	if !ok || insecureRegistries == nil {
		return fmt.Errorf("registry configuration path 'plugins.%s.registry' not found or invalid in containerd config", pluginPrefix)
	}
	if add {
		// add registry table
		// if the key already exists, it will not be added again.
		insecureRegistries.SetPath([]string{"mirrors", registry, "endpoint"}, []string{fmt.Sprintf("http://%s", registry)})
		logMsg = fmt.Sprintf("write %s registry to %s", registry, containerdDefaultConfig)
	} else {
		// delete registry table
		if err = insecureRegistries.DeletePath([]string{"mirrors", registry}); err != nil {
			return
		}
		logMsg = fmt.Sprintf("delete %s registry from %s", registry, containerdDefaultConfig)
	}
	data, err := conf.ToTomlString()
	if err != nil {
		return
	}
	if err = os.WriteFile(containerdDefaultConfig, []byte(data), info.Mode()); err != nil {
		return
	}
	_, _ = cmdutil.CheckContextAndAppendStepLogFile(ctx, []byte(fmt.Sprintf("[%s] + %s \n", time.Now().Format(time.RFC3339), logMsg)))
	// Restart containerd by running systemctl, containerd does not restart existing containers.
	// Therefore, the normal running of existing containers is not affected.
	if _, err = cmdutil.RunCmdWithContext(ctx, dryRun, "bash", "-c", "systemctl daemon-reload && systemctl restart containerd"); err != nil {
		return
	}
	return
}
