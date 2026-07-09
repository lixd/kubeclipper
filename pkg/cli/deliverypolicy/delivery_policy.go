package deliverypolicy

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/pmezard/go-difflib/difflib"
	"github.com/spf13/cobra"

	"github.com/kubeclipper/kubeclipper/cmd/kcctl/app/options"
	"github.com/kubeclipper/kubeclipper/pkg/cli/printer"
	"github.com/kubeclipper/kubeclipper/pkg/cli/utils"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	"github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

const (
	longDescription = `
  Manage support policy for OCI resource delivery.

  The policy is stored in KubeClipper control plane and used to resolve
  supported Kubernetes/component version combinations.`
	getExample = `
  # Get current delivery policy
  kcctl delivery-policy get -o yaml
`
	applyExample = `
  # Apply delivery policy from local file
  kcctl delivery-policy apply -f ./policy.json
`
	validateExample = `
  # Validate delivery policy file
  kcctl delivery-policy validate -f ./policy.yaml
`
	templateExample = `
  # Print a starter delivery policy template
  kcctl delivery-policy template -o yaml
`
	diffExample = `
  # Diff local policy file with current control-plane policy
  kcctl delivery-policy diff -f ./policy.yaml
`
)

type deliveryPolicyClient interface {
	GetDeliveryPolicy(ctx context.Context) (*deliveryapis.SupportPolicy, error)
	UpdateDeliveryPolicy(ctx context.Context, policy *deliveryapis.SupportPolicy) (*deliveryapis.SupportPolicy, error)
}

type DeliveryPolicyOptions struct {
	options.IOStreams
	PrintFlags *printer.PrintFlags
	cliOpts    *options.CliOptions
	client     deliveryPolicyClient

	File string
}

func NewDeliveryPolicyOptions(streams options.IOStreams) *DeliveryPolicyOptions {
	return &DeliveryPolicyOptions{
		IOStreams:  streams,
		PrintFlags: printer.NewPrintFlags(),
		cliOpts:    options.NewCliOptions(),
	}
}

func NewCmdDeliveryPolicy(streams options.IOStreams) *cobra.Command {
	o := NewDeliveryPolicyOptions(streams)
	cmd := &cobra.Command{
		Use:                   "delivery-policy",
		DisableFlagsInUseLine: true,
		Short:                 "Manage OCI delivery support policy",
		Long:                  longDescription,
	}

	cmd.AddCommand(NewCmdDeliveryPolicyGet(o))
	cmd.AddCommand(NewCmdDeliveryPolicyApply(o))
	cmd.AddCommand(NewCmdDeliveryPolicyValidate(o))
	cmd.AddCommand(NewCmdDeliveryPolicyTemplate(o))
	cmd.AddCommand(NewCmdDeliveryPolicyDiff(o))
	return cmd
}

func NewCmdDeliveryPolicyGet(o *DeliveryPolicyOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "get",
		DisableFlagsInUseLine: true,
		Short:                 "Get current delivery policy",
		Example:               getExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete())
			utils.CheckErr(o.RunGet())
		},
	}
	o.cliOpts.AddFlags(cmd.Flags())
	o.PrintFlags.AddFlags(cmd)
	return cmd
}

func NewCmdDeliveryPolicyApply(o *DeliveryPolicyOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "apply -f <policy-file>",
		DisableFlagsInUseLine: true,
		Short:                 "Apply delivery policy",
		Example:               applyExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete())
			utils.CheckErr(o.ValidateApply(cmd))
			utils.CheckErr(o.RunApply())
		},
	}
	o.cliOpts.AddFlags(cmd.Flags())
	o.PrintFlags.AddFlags(cmd)
	cmd.Flags().StringVarP(&o.File, "file", "f", o.File, "delivery policy file path")
	utils.CheckErr(cmd.MarkFlagRequired("file"))
	return cmd
}

func NewCmdDeliveryPolicyValidate(o *DeliveryPolicyOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "validate -f <policy-file>",
		DisableFlagsInUseLine: true,
		Short:                 "Validate delivery policy file",
		Example:               validateExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.ValidateApply(cmd))
			utils.CheckErr(o.RunValidate())
		},
	}
	cmd.Flags().StringVarP(&o.File, "file", "f", o.File, "delivery policy file path")
	utils.CheckErr(cmd.MarkFlagRequired("file"))
	return cmd
}

func NewCmdDeliveryPolicyTemplate(o *DeliveryPolicyOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "template",
		DisableFlagsInUseLine: true,
		Short:                 "Print a delivery policy template",
		Example:               templateExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.RunTemplate())
		},
	}
	o.PrintFlags.AddFlags(cmd)
	return cmd
}

func NewCmdDeliveryPolicyDiff(o *DeliveryPolicyOptions) *cobra.Command {
	cmd := &cobra.Command{
		Use:                   "diff -f <policy-file>",
		DisableFlagsInUseLine: true,
		Short:                 "Diff local delivery policy file against current control-plane policy",
		Example:               diffExample,
		Args:                  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			utils.CheckErr(o.Complete())
			utils.CheckErr(o.ValidateApply(cmd))
			utils.CheckErr(o.RunDiff())
		},
	}
	o.cliOpts.AddFlags(cmd.Flags())
	cmd.Flags().StringVarP(&o.File, "file", "f", o.File, "delivery policy file path")
	utils.CheckErr(cmd.MarkFlagRequired("file"))
	return cmd
}

func (o *DeliveryPolicyOptions) Complete() error {
	if o.client != nil {
		return nil
	}
	if err := o.cliOpts.Complete(); err != nil {
		return err
	}
	client, err := kc.FromConfig(o.cliOpts.ToRawConfig())
	if err != nil {
		return err
	}
	o.client = client
	return nil
}

func (o *DeliveryPolicyOptions) ValidateApply(cmd *cobra.Command) error {
	if o.File == "" {
		return utils.UsageErrorf(cmd, "policy file must be specified")
	}
	return nil
}

func (o *DeliveryPolicyOptions) RunGet() error {
	policy, err := o.client.GetDeliveryPolicy(context.Background())
	if err != nil {
		return err
	}
	return o.PrintFlags.Print(policyPrinter{policy: policy}, o.Out)
}

func (o *DeliveryPolicyOptions) RunApply() error {
	policy, err := loadPolicyFromFile(o.File)
	if err != nil {
		return err
	}
	updated, err := o.client.UpdateDeliveryPolicy(context.Background(), policy)
	if err != nil {
		return err
	}
	return o.PrintFlags.Print(policyPrinter{policy: updated}, o.Out)
}

func (o *DeliveryPolicyOptions) RunValidate() error {
	if _, err := loadPolicyFromFile(o.File); err != nil {
		return err
	}
	_, err := fmt.Fprintln(o.Out, "delivery support policy is valid")
	return err
}

func (o *DeliveryPolicyOptions) RunTemplate() error {
	return o.PrintFlags.Print(policyPrinter{policy: defaultPolicyTemplate()}, o.Out)
}

func (o *DeliveryPolicyOptions) RunDiff() error {
	desired, err := loadPolicyFromFile(o.File)
	if err != nil {
		return err
	}
	current, err := o.client.GetDeliveryPolicy(context.Background())
	if err != nil {
		return err
	}
	currentYAML, err := printer.YAMLPrinter(current)
	if err != nil {
		return err
	}
	desiredYAML, err := printer.YAMLPrinter(desired)
	if err != nil {
		return err
	}
	if bytes.Equal(currentYAML, desiredYAML) {
		_, err = fmt.Fprintln(o.Out, "delivery policy has no changes")
		return err
	}
	diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(string(currentYAML)),
		B:        difflib.SplitLines(string(desiredYAML)),
		FromFile: "current",
		ToFile:   o.File,
		Context:  3,
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(o.Out, diff)
	return err
}

func defaultPolicyTemplate() *deliveryapis.SupportPolicy {
	policy := deliveryapis.NewSupportPolicy("default")
	policy.Spec.Policies = []deliveryapis.KubernetesSupportPolicy{{
		Name:  "k8s-v1.36",
		Match: deliveryapis.PolicyMatch{KubernetesVersion: "v1.36.*"},
		ComponentSlots: []deliveryapis.ComponentSlotRule{
			{
				Slot:      "cri",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: "containerd", Version: "2.2.4"},
				Options: []deliveryapis.ComponentOption{
					{Kind: "cri", Name: "containerd", AllowedVersions: []string{"2.2.4"}},
				},
			},
			{
				Slot:      "cni",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: "calico", Version: "v3.31.5"},
				Options: []deliveryapis.ComponentOption{
					{Kind: "cni", Name: "calico", AllowedVersions: []string{"v3.31.5"}},
				},
			},
			{
				Slot:      "k8s-extension",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: "k8s-extension", Version: "v1"},
				Options: []deliveryapis.ComponentOption{
					{Kind: "k8s-extension", Name: "k8s-extension", AllowedVersions: []string{"v1"}},
				},
			},
			{
				Slot:      "bootstrap-kubeclipper",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: "kubeclipper", Version: "v1.8.0"},
				Options: []deliveryapis.ComponentOption{
					{Kind: "bootstrap", Name: "kubeclipper", AllowedVersions: []string{"v1.8.0"}},
				},
			},
			{
				Slot:      "bootstrap-etcd",
				Selection: deliveryapis.SelectionOneOf,
				Required:  true,
				Default:   deliveryapis.ComponentChoice{Name: "etcd", Version: "3.5.21"},
				Options: []deliveryapis.ComponentOption{
					{Kind: "bootstrap", Name: "etcd", AllowedVersions: []string{"3.5.21"}},
				},
			},
		},
	}}
	return policy
}

func loadPolicyFromFile(path string) (*deliveryapis.SupportPolicy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	policy := &deliveryapis.SupportPolicy{}
	if err = decodePolicyFile(data, policy); err != nil {
		return nil, err
	}
	if err = policy.Validate(); err != nil {
		return nil, err
	}
	return policy, nil
}

func decodePolicyFile(data []byte, policy *deliveryapis.SupportPolicy) error {
	if err := deliveryapis.DecodeSupportPolicy(data, policy); err != nil {
		return fmt.Errorf("decode policy file failed: %w", err)
	}
	return nil
}

type policyPrinter struct {
	policy *deliveryapis.SupportPolicy
}

func (p policyPrinter) JSONPrint() ([]byte, error) {
	return printer.JSONPrinter(p.policy)
}

func (p policyPrinter) YAMLPrint() ([]byte, error) {
	return printer.YAMLPrinter(p.policy)
}

func (p policyPrinter) TablePrint() ([]string, [][]string) {
	if p.policy == nil {
		return []string{"NAME", "KUBERNETES_RULES"}, nil
	}
	return []string{"NAME", "KUBERNETES_RULES"}, [][]string{{
		p.policy.Metadata.Name,
		fmt.Sprintf("%d", len(p.policy.Spec.Policies)),
	}}
}
