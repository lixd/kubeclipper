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

package get

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"

	"github.com/kubeclipper/kubeclipper/pkg/cli/printer"

	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	"github.com/kubeclipper/kubeclipper/pkg/query"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

const (
	longDescription = `
  Display one or many resources

  Prints a table of the most important information about the specified resources.
  You can filter the list using a label selector and the --selector flag.

  Notice: You must run 'kcctl login' at first, you can get help to run 'kcctl login -h'`
	// get 命令示例
	getExample = `
  # List operations for a specific cluster
  kcctl operation list --cluster my-cluster

  # View operation details
  kcctl operation describe <OPERATION_ID>

  # List all cluster
  kcctl get cluster

  # List all node
  kcctl get node

  # List user in json output format
  kcctl get user -o json

  # List user with label-selector
  kcctl get user --selector foo=bar

  # List user with field-selector
  kcctl get user --field-selector metadata.name=foo

  # List roles
  kcctl get role
`
)

type GetOptions struct {
	PrintFlags *printer.PrintFlags
	cliOpts    *options.CliOptions
	options.IOStreams
	LabelSelector string
	FieldSelector string
	Watch         bool
	client        *kc.Client
	resource      string
	name          string
}

var (
	allowedResource = sets.NewString(
		options.ResourceUser, options.ResourceRole,
		options.ResourceNode, options.ResourceCluster,
		options.ResourceConfigMap, options.ResourceRegistry,
	)
)

func NewGetOptions(streams options.IOStreams) *GetOptions {
	return &GetOptions{
		PrintFlags: printer.NewPrintFlags(),
		IOStreams:  streams,
		cliOpts:    options.NewCliOptions(),
	}
}

func NewCmdGet(streams options.IOStreams) *cobra.Command {
	o := NewGetOptions(streams)
	cmd := &cobra.Command{
		Use:                   "get TYPE [NAME] [flags]",
		DisableFlagsInUseLine: true,
		Short:                 "Display one or multiple resources",
		Long:                  longDescription,
		Example:               getExample,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.ValidateArgs(cmd, args); err != nil {
				return err
			}
			if err := o.PrintFlags.Validate(); err != nil {
				return err
			}
			if err := o.Complete(o.cliOpts); err != nil {
				return err
			}
			return o.runGet(cmd.Context())
		},
		ValidArgsFunction: ValidArgsFunction(o),
	}
	o.cliOpts.AddFlags(cmd.Flags())
	cmd.Flags().BoolVarP(&o.Watch, "watch", "w", o.Watch, "After listing/getting the requested object, watch for changes.")
	cmd.Flags().StringVarP(&o.LabelSelector, "selector", "l", o.LabelSelector, "Selector (label query) to filter on, supports '=', '==', and '!='.(e.g. -l key1=value1,key2=value2)")
	cmd.Flags().StringVar(&o.FieldSelector, "field-selector", o.FieldSelector, "Selector (field query) to filter on, supports '=', '==', and '!='.(e.g. --field-selector key1=value1,key2=value2). The server only supports a limited number of field queries per type.")
	o.PrintFlags.AddFlags(cmd)
	return cmd
}

func (l *GetOptions) ValidateArgs(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return utils.UsageErrorf(cmd, "You must specify the type of resource to get,support %v now", allowedResource.List())
	}
	if len(args) > 2 {
		return utils.UsageErrorf(cmd, "expected TYPE and at most one NAME")
	}
	l.resource = args[0]
	args = args[1:]
	if len(args) > 0 {
		l.name = args[0]
	}
	if l.resource == options.ResourceOperation {
		return fmt.Errorf("use 'kcctl operation' command instead to manage operations, see 'kcctl operation -h' for details")
	}
	if !allowedResource.Has(l.resource) {
		return utils.UsageErrorf(cmd, "unsupported resource type,support %v now", allowedResource.List())
	}
	return nil
}

func (l *GetOptions) Complete(opts *options.CliOptions) error {
	if err := opts.Complete(); err != nil {
		return err
	}
	c, err := kc.FromConfig(opts.ToRawConfig())
	if err != nil {
		return err
	}
	l.client = c
	return nil
}

func (l *GetOptions) RunGet() error { return l.runGet(context.Background()) }

func (l *GetOptions) runGet(ctx context.Context) error {
	if l.Watch {
		return l.watch(ctx)
	}
	if l.name != "" {
		return l.describe(ctx)
	}
	return l.list(ctx)
}

// watchPathByResource maps get resources to their list/watch API paths.
var watchPathByResource = map[string]string{
	options.ResourceNode:      "/api/core.kubeclipper.io/v1/nodes",
	options.ResourceUser:      "/api/iam.kubeclipper.io/v1/users",
	options.ResourceRole:      "/api/iam.kubeclipper.io/v1/roles",
	options.ResourceCluster:   "/api/core.kubeclipper.io/v1/clusters",
	options.ResourceConfigMap: "/api/core.kubeclipper.io/v1/configmaps",
	options.ResourceRegistry:  "/api/core.kubeclipper.io/v1/registries",
}

// watch prints the current object list and then streams watch events
// (ADDED/MODIFIED/DELETED + object name) until interrupted. R4 found the
// previous implementation exited after one shot; the server already serves
// chunked WatchEvent streams, so consume them here.
func (l *GetOptions) watch(ctx context.Context) error {
	path, ok := watchPathByResource[l.resource]
	if !ok {
		return fmt.Errorf("unsupported watch resource %s", l.resource)
	}
	if l.name != "" {
		if err := l.describe(ctx); err != nil {
			return err
		}
	} else if err := l.list(ctx); err != nil {
		return err
	}
	q := query.New()
	q.LabelSelector = l.LabelSelector
	q.FieldSelector = l.FieldSelector
	if l.name != "" {
		if q.FieldSelector != "" {
			q.FieldSelector += ","
		}
		q.FieldSelector += "metadata.name=" + l.name
	}
	q.Watch = true
	for {
		body, err := l.client.StreamList(ctx, path, kc.Queries(*q))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		err = l.printWatchStream(ctx, body)
		_ = body.Close()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && err != io.EOF {
			return err
		}
		fmt.Fprintln(l.ErrOut, "watch stream ended; reconnecting...")
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

func (l *GetOptions) printWatchStream(ctx context.Context, body io.Reader) error {
	decoder := json.NewDecoder(body)
	for {
		var event struct {
			Type   string          `json:"type"`
			Object json.RawMessage `json:"object"`
		}
		if err := decoder.Decode(&event); err != nil {
			return err
		}
		if event.Type == "ERROR" {
			return fmt.Errorf("watch server error: %s", event.Object)
		}
		var object struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Status struct {
				Phase string `json:"phase"`
			} `json:"status"`
		}
		if err := json.Unmarshal(event.Object, &object); err != nil {
			return fmt.Errorf("decode watch object: %w", err)
		}
		if l.name != "" && object.Metadata.Name != l.name {
			continue
		}
		if l.PrintFlags.Format() != "table" {
			if l.PrintFlags.Format() == "yaml" {
				if _, err := fmt.Fprintln(l.Out, "---"); err != nil {
					return err
				}
			}
			if err := l.PrintFlags.PrintData(event, l.Out); err != nil {
				return err
			}
		} else if object.Metadata.Name != "" {
			if _, err := fmt.Fprintf(l.Out, "%-9s %s\t%s\n", event.Type, object.Metadata.Name, object.Status.Phase); err != nil {
				return err
			}
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (l *GetOptions) list(ctx context.Context) error {
	q := query.New()
	q.LabelSelector = l.LabelSelector
	q.FieldSelector = l.FieldSelector
	var (
		result printer.ResourcePrinter
		err    error
	)
	switch l.resource {
	case options.ResourceNode:
		result, err = l.client.ListNodes(ctx, kc.Queries(*q))
	case options.ResourceUser:
		result, err = l.client.ListUsers(ctx, kc.Queries(*q))
	case options.ResourceRole:
		result, err = l.client.ListRoles(ctx, kc.Queries(*q))
	case options.ResourceCluster:
		result, err = l.client.ListClusters(ctx, kc.Queries(*q))
	case options.ResourceConfigMap:
		result, err = l.client.ListConfigMaps(ctx, kc.Queries(*q))
	case options.ResourceRegistry:
		result, err = l.client.ListRegistries(ctx, kc.Queries(*q))

	default:
		return fmt.Errorf("unsupported resource,support %s now", allowedResource.List())
	}

	if err != nil {
		return err
	}
	return l.PrintFlags.PrintList(result, l.IOStreams.Out)
}

func (l *GetOptions) describe(ctx context.Context) error {
	var (
		result printer.ResourcePrinter
		err    error
	)
	switch l.resource {
	case options.ResourceNode:
		result, err = l.client.DescribeNode(ctx, l.name)
	case options.ResourceUser:
		result, err = l.client.DescribeUser(ctx, l.name)
	case options.ResourceRole:
		result, err = l.client.DescribeRole(ctx, l.name)
	case options.ResourceCluster:
		result, err = l.client.DescribeCluster(ctx, l.name)
	case options.ResourceConfigMap:
		result, err = l.client.DescribeConfigMap(ctx, l.name)
	case options.ResourceRegistry:
		result, err = l.client.DescribeRegistries(ctx, l.name)
	default:
		return fmt.Errorf("unsupported resource")
	}
	if err != nil {
		return err
	}
	return l.PrintFlags.Print(result, l.IOStreams.Out)
}

func ValidArgsFunction(o *GetOptions) func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if len(args) == 0 {
			return allowedResource.List(), cobra.ShellCompDirectiveNoFileComp
		}
		if err := o.Complete(o.cliOpts); err != nil {
			return nil, cobra.ShellCompDirectiveError
		}
		switch resource := args[0]; resource {
		case options.ResourceUser:
			return o.listUser(toComplete), cobra.ShellCompDirectiveNoSpace | cobra.ShellCompDirectiveNoFileComp
		case options.ResourceRole:
			return o.listRole(toComplete), cobra.ShellCompDirectiveNoSpace | cobra.ShellCompDirectiveNoFileComp
		case options.ResourceNode:
			return o.listNode(toComplete), cobra.ShellCompDirectiveNoSpace | cobra.ShellCompDirectiveNoFileComp
		case options.ResourceCluster:
			return o.listCluster(toComplete), cobra.ShellCompDirectiveNoSpace | cobra.ShellCompDirectiveNoFileComp
		case options.ResourceConfigMap:
			return o.listConfigMaps(toComplete), cobra.ShellCompDirectiveNoSpace | cobra.ShellCompDirectiveNoFileComp
		case options.ResourceRegistry:
			return o.listRegistry(toComplete), cobra.ShellCompDirectiveNoSpace | cobra.ShellCompDirectiveNoFileComp
		}
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
}

func (l *GetOptions) listUser(toComplete string) []string {
	list := make([]string, 0)
	q := query.New()
	q.LabelSelector = l.LabelSelector
	q.FieldSelector = l.FieldSelector
	users, err := l.client.ListUsers(context.TODO(), kc.Queries(*q))
	if err != nil {
		return nil
	}
	for _, v := range users.Items {
		if strings.HasPrefix(v.Name, toComplete) {
			list = append(list, v.Name)
		}
	}
	return list
}

func (l *GetOptions) listRole(toComplete string) []string {
	list := make([]string, 0)
	q := query.New()
	q.LabelSelector = l.LabelSelector
	q.FieldSelector = l.FieldSelector
	data, err := l.client.ListRoles(context.TODO(), kc.Queries(*q))
	if err != nil {
		return nil
	}
	for _, v := range data.Items {
		if strings.HasPrefix(v.Name, toComplete) {
			list = append(list, v.Name)
		}
	}
	return list
}

func (l *GetOptions) listNode(toComplete string) []string {
	list := make([]string, 0)
	q := query.New()
	q.LabelSelector = l.LabelSelector
	q.FieldSelector = l.FieldSelector
	data, err := l.client.ListNodes(context.TODO(), kc.Queries(*q))
	if err != nil {
		return nil
	}
	for _, v := range data.Items {
		if strings.HasPrefix(v.Name, toComplete) {
			list = append(list, v.Name)
		}
	}
	return list
}

func (l *GetOptions) listCluster(toComplete string) []string {
	list := make([]string, 0)
	q := query.New()
	q.LabelSelector = l.LabelSelector
	q.FieldSelector = l.FieldSelector
	data, err := l.client.ListClusters(context.TODO(), kc.Queries(*q))
	if err != nil {
		return nil
	}
	for _, v := range data.Items {
		if strings.HasPrefix(v.Name, toComplete) {
			list = append(list, v.Name)
		}
	}
	return list
}

func (l *GetOptions) listConfigMaps(toComplete string) []string {
	list := make([]string, 0)
	q := query.New()
	q.LabelSelector = l.LabelSelector
	q.FieldSelector = l.FieldSelector
	data, err := l.client.ListConfigMaps(context.TODO(), kc.Queries(*q))
	if err != nil {
		return nil
	}
	for _, v := range data.Items {
		if strings.HasPrefix(v.Name, toComplete) {
			list = append(list, v.Name)
		}
	}
	return list
}

func (l *GetOptions) listRegistry(toComplete string) []string {
	list := make([]string, 0)
	q := query.New()
	q.LabelSelector = l.LabelSelector
	q.FieldSelector = l.FieldSelector
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	data, err := l.client.ListRegistries(ctx, kc.Queries(*q))
	if err != nil {
		return nil
	}
	for i := range data.Items {
		if strings.HasPrefix(data.Items[i].Name, toComplete) {
			list = append(list, data.Items[i].Name)
		}
	}
	return list
}
