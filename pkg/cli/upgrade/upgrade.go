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

package upgrade

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
)

const (
	longDescription = `
  Upgrade kubeclipper platform or components.

  The legacy upgrade package flow has been removed. Platform upgrade must use
	an OCI-native implementation so binaries and images are resolved from a
  registry instead of kc-upgrade tarballs.`
	upgradeExample = `
  # OCI-native upgrade is not implemented yet.
  kcctl upgrade all`
)

type UpgradeOptions struct {
	options.IOStreams
	component string
}

func NewUpgradeOptions(stream options.IOStreams) *UpgradeOptions {
	return &UpgradeOptions{IOStreams: stream}
}

func NewCmdUpgrade(stream options.IOStreams) *cobra.Command {
	o := NewUpgradeOptions(stream)
	cmd := &cobra.Command{
		Use:                   "upgrade ( component ) [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "upgrade kubeclipper platform or components",
		Long:                  longDescription,
		Example:               upgradeExample,
		Args:                  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Validate(cmd, args))
			utils.CheckErr(o.RunUpgrade())
		},
	}
	return cmd
}

func (o *UpgradeOptions) Validate(cmd *cobra.Command, args []string) error {
	o.component = args[0]
	switch o.component {
	case options.UpgradeAll, options.UpgradeKcctl, options.UpgradeAgent, options.UpgradeServer, options.UpgradeConsole:
		return nil
	default:
		return utils.UsageErrorf(cmd, "unsupported upgrade component, support [ all | kcctl | agent | server | console ] now")
	}
}

func (o *UpgradeOptions) RunUpgrade() error {
	return fmt.Errorf("kcctl upgrade %s is not available in pure OCI mode yet", o.component)
}
