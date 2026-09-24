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

package k8s

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	"github.com/kubeclipper/kubeclipper/pkg/component/utils"
	"github.com/kubeclipper/kubeclipper/pkg/logger"
	v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
	"github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1/cni"
	"github.com/kubeclipper/kubeclipper/pkg/utils/cmdutil"
	"github.com/kubeclipper/kubeclipper/pkg/utils/strutil"
)

var (
	_ component.StepRunnable = (*JoinCmd)(nil)
	_ component.StepRunnable = (*Drain)(nil)
)

const (
	joinNodeCmd = "joinNodeCmd"
	drain       = "drain"
)

func init() {
	if err := component.RegisterAgentStep(fmt.Sprintf(component.RegisterStepKeyFormat, joinNodeCmd, version, component.TypeStep), &JoinCmd{}); err != nil {
		panic(err)
	}
	if err := component.RegisterAgentStep(fmt.Sprintf(component.RegisterStepKeyFormat, drain, version, component.TypeStep), &Drain{}); err != nil {
		panic(err)
	}
}

type GenNode struct {
	Nodes          component.NodeList `json:"nodes"`
	Cluster        *v1.Cluster
	role           string
	installSteps   []v1.Step
	uninstallSteps []v1.Step
}

type JoinCmd struct {
	ContainerRuntime string `json:"containerRuntime"`
	// ControlPlane indicates the join command is generated for a control-plane
	// node and therefore must carry a certificate key.
	ControlPlane bool `json:"controlPlane,omitempty"`
}

type KubeadmJoinUtil struct {
	ControlPlaneEndpoint string `json:"controlPlaneEndpoint"`
	Token                string `json:"token"`
	DiscoveryHash        string `json:"discoveryHash"`
	ContainerRuntime     string `json:"containerRuntime"`
}

type Drain struct {
	Hostname  string   `json:"hostname"`
	ExtraArgs []string `json:"extraArgs"`
}

func (stepper *GenNode) Validate() error {
	if stepper == nil {
		return fmt.Errorf("GenNode object is empty")
	}

	if stepper.Nodes == nil {
		return fmt.Errorf("GenNode Nodes object is empty")
	}

	return nil
}

func (stepper *GenNode) InitStepper(metadata *component.ExtraMetadata, cluster *v1.Cluster, role string) *GenNode {
	switch role {
	case NodeRoleMaster:
		stepper.Nodes = append(stepper.Nodes, metadata.Masters...)
	case NodeRoleWorker:
		stepper.Nodes = append(stepper.Nodes, metadata.Workers...)
	}
	stepper.Cluster = cluster
	stepper.role = role
	return stepper
}

func (stepper *GenNode) MakeInstallSteps(metadata *component.ExtraMetadata, patchNodes []v1.StepNode, role string) error {
	err := stepper.Validate()
	if err != nil {
		return err
	}
	avaMasters, err := metadata.Masters.AvailableKubeMasters()
	if err != nil {
		return err
	}
	// Join commands must be generated on an existing control-plane member;
	// for master-scale-up the joining node is already part of metadata.Masters
	// but has no usable kubeadm config until the join completes.
	masters := unwrapAvailableMasters(avaMasters, patchNodes)

	// add node to cluster
	if len(stepper.installSteps) == 0 {
		isControlPlane := stepper.role == NodeRoleMaster
		// We should use kubeadm to create join token on the first control plane node.
		steps, err := EnvSetupSteps(patchNodes)
		if err != nil {
			return err
		}
		stepper.installSteps = append(stepper.installSteps, steps...)

		if metadata.Offline {
			cf, err := cni.Load(stepper.Cluster.CNI.Type)
			if err != nil {
				return err
			}
			cniStepper := cf.Create().InitStep(metadata, &stepper.Cluster.CNI, &stepper.Cluster.Networking)
			steps, err = cniStepper.LoadImage(patchNodes)
			if err != nil {
				return err
			}
			stepper.installSteps = append(stepper.installSteps, steps...)

			for _, addon := range metadata.Addons {
				ad, ok := component.Load(fmt.Sprintf(component.RegisterFormat, addon.Name, addon.Version))
				if !ok {
					continue
				}
				compMeta := ad.NewInstance()
				if err := json.Unmarshal(addon.Config.Raw, compMeta); err != nil {
					return err
				}
				newComp, _ := compMeta.(component.Interface)
				if err := newComp.InitSteps(component.WithExtraMetadata(context.TODO(), *metadata)); err != nil {
					return err
				}
				stepList := newComp.GetInstallSteps()
				for _, st := range stepList {
					// TODO: Temporarily use hardcode to adjust, and subsequently optimize the step code for image download and load
					if strings.Contains(st.Name, "imageLoad") {
						st.Nodes = patchNodes
						stepper.installSteps = append(stepper.installSteps, st)
						break
					}
				}
			}
		}

		joinCmd := JoinCmd{ControlPlane: isControlPlane}
		steps, err = joinCmd.InitStepper(stepper.Cluster.ContainerRuntime.Type).InstallSteps([]v1.StepNode{masters[0]})
		if err != nil {
			return err
		}
		stepper.installSteps = append(stepper.installSteps, steps...)

		kubeadmConf := KubeadmConfig{}
		steps, err = kubeadmConf.InitStepper(stepper.Cluster, metadata).JoinSteps(isControlPlane, patchNodes)
		if err != nil {
			return err
		}
		stepper.installSteps = append(stepper.installSteps, steps...)

		join := ClusterNode{}
		steps, err = join.InitStepper(stepper.Cluster, metadata).InstallSteps(role, patchNodes)
		if err != nil {
			return err
		}
		stepper.installSteps = append(stepper.installSteps, steps...)

		// A successful kubeadm join only proves that the command exited cleanly.
		// Confirm that Kubernetes observes the nodes added by this operation as Ready.
		targetNames := make([]string, 0, len(patchNodes))
		for _, node := range patchNodes {
			name := node.Hostname
			if stepper.Cluster.Kubelet.IPAsName {
				name = node.NodeIPv4
			}
			if name == "" {
				return fmt.Errorf("added node %q has no Kubernetes node name", node.ID)
			}
			targetNames = append(targetNames, name)
		}
		ready := &AddedNodesReady{NodeNames: targetNames}
		steps, err = ready.InstallSteps([]v1.StepNode{masters[0]})
		if err != nil {
			return err
		}
		stepper.installSteps = append(stepper.installSteps, steps...)

		// refresh the API server virtual service on workers with the new
		// master list after the control-plane node joined
		if isControlPlane && stepper.Cluster.Networking.WorkerNodeVip != "" && len(metadata.Workers) > 0 {
			steps, err = LvsCareRefreshSteps(stepper.Cluster, metadata, utils.UnwrapNodeList(metadata.Workers), nil)
			if err != nil {
				return err
			}
			stepper.installSteps = append(stepper.installSteps, steps...)
		}
	}

	return nil
}

// unwrapAvailableMasters unwraps the available kube masters and drops the
// nodes being patched (joined/removed) so join/drain/etcd step targets always
// run on an existing control-plane member.
func unwrapAvailableMasters(avaMasters component.NodeList, patchNodes []v1.StepNode) []v1.StepNode {
	exclude := make(map[string]struct{}, len(patchNodes))
	for _, n := range patchNodes {
		exclude[n.ID] = struct{}{}
	}
	masters := make(component.NodeList, 0, len(avaMasters))
	for _, m := range avaMasters {
		if _, ok := exclude[m.ID]; ok {
			continue
		}
		masters = append(masters, m)
	}
	return utils.UnwrapNodeList(masters)
}

// MakeUninstallSteps make uninstall-steps
func (stepper *GenNode) MakeUninstallSteps(metadata *component.ExtraMetadata, patchNodes []v1.StepNode) error {
	err := stepper.Validate()
	if err != nil {
		return err
	}
	avaMasters, err := metadata.Masters.AvailableKubeMasters()
	if err != nil {
		return err
	}
	masters := unwrapAvailableMasters(avaMasters, patchNodes)
	if len(masters) == 0 {
		return fmt.Errorf("no existing control-plane node available to serve the drain and etcd steps")
	}
	if len(stepper.uninstallSteps) == 0 {
		if stepper.role == NodeRoleMaster {
			// Remove the leaving nodes from the etcd membership while they are
			// still alive; kubeadm reset alone never shrinks the membership.
			steps, err := EtcdMemberRemoveSteps(masters[0], patchNodes)
			if err != nil {
				return err
			}
			stepper.uninstallSteps = append(stepper.uninstallSteps, steps...)
		}
		args := []string{"--ignore-daemonsets", "--delete-emptydir-data", "--force", "--timeout=5m"}
		for _, node := range patchNodes {
			d := &Drain{}
			steps, err := d.InitStepper(node.Hostname, args).UninstallSteps([]v1.StepNode{masters[0]})
			if err != nil {
				return err
			}
			stepper.uninstallSteps = append(stepper.uninstallSteps, steps...)
		}

		steps, err := KubeadmReset(patchNodes)
		if err != nil {
			return err
		}
		stepper.uninstallSteps = append(stepper.uninstallSteps, steps...)
		// worker nodes don't need to remove etcd data dir
		if stepper.role == NodeRoleMaster {
			stepper.uninstallSteps = append(stepper.uninstallSteps,
				doCommandRemoveStep("removeEtcdDataDir", patchNodes,
					strutil.StringDefaultIfEmpty(EtcdDefaultDataDir, stepper.Cluster.Etcd.DataDir)))
		}
		stepper.uninstallSteps = append(stepper.uninstallSteps,
			doCommandRemoveStep("removeKubeletDataDir", patchNodes, KubeletDefaultDataDir),
			doCommandRemoveStep("removeDockershimDataDir", patchNodes, DockershimDefaultDataDir),
		)

		heal := &Health{}
		if err := heal.InitStepper(metadata.KubeVersion, DefaultKubeConfigPath); err != nil {
			return err
		}
		steps, err = heal.UninstallSteps(&stepper.Cluster.Networking, patchNodes...)
		if err != nil {
			return err
		}
		stepper.uninstallSteps = append(stepper.uninstallSteps, steps...)

		// clean CNI config
		steps, err = CleanCNI(metadata, &stepper.Cluster.CNI, &stepper.Cluster.Networking, patchNodes)
		if err != nil {
			return err
		}

		stepper.uninstallSteps = append(stepper.uninstallSteps, steps...)
		// clean Kubernetes config
		stepper.uninstallSteps = append(stepper.uninstallSteps,
			doCommandRemoveStep("removeKubernetesConfig", patchNodes, K8SDefaultConfigDir))
		// clear worker /etc/hosts vip domain
		// sed -i '/apiserver.cluster.local/d' /etc/hosts
		apiServerDomain := APIServerDomainPrefix + strutil.StringDefaultIfEmpty("cluster.local",
			stepper.Cluster.Networking.DNSDomain)
		stepper.uninstallSteps = append(stepper.uninstallSteps, v1.Step{
			ID:         strutil.GetUUID(),
			Name:       "clearVIPDomain",
			Timeout:    metav1.Duration{Duration: 5 * time.Second},
			ErrIgnore:  true,
			RetryTimes: 1,
			Nodes:      patchNodes,
			Action:     v1.ActionUninstall,
			Commands: []v1.Command{
				{
					Type:         v1.CommandShell,
					ShellCommand: []string{"bash", "-c", fmt.Sprintf("sed -i '/%s/d' /etc/hosts", apiServerDomain)},
				},
			},
		})

		// refresh the API server virtual service on the remaining workers with
		// the reduced master list after the control-plane node left
		if stepper.role == NodeRoleMaster && stepper.Cluster.Networking.WorkerNodeVip != "" && len(metadata.Workers) > 0 {
			steps, err = LvsCareRefreshSteps(stepper.Cluster, metadata, utils.UnwrapNodeList(metadata.Workers), patchNodes)
			if err != nil {
				return err
			}
			stepper.uninstallSteps = append(stepper.uninstallSteps, steps...)
		}

	}

	return nil
}

func (stepper *GenNode) GetSteps(action v1.StepAction) []v1.Step {
	switch action {
	case v1.ActionInstall:
		return stepper.installSteps
	case v1.ActionUninstall:
		return stepper.uninstallSteps
	}
	return nil
}

func (stepper *JoinCmd) InitStepper(criType string) *JoinCmd {
	stepper.ContainerRuntime = criType
	return stepper
}

func (stepper *JoinCmd) InstallSteps(nodes []v1.StepNode) ([]v1.Step, error) {
	bytes, err := json.Marshal(stepper)
	if err != nil {
		return nil, err
	}

	return []v1.Step{
		{
			ID:         strutil.GetUUID(),
			Name:       "getJoinCommand",
			Timeout:    metav1.Duration{Duration: 10 * time.Second},
			ErrIgnore:  false,
			RetryTimes: 1,
			Nodes:      nodes,
			Action:     v1.ActionInstall,
			Commands: []v1.Command{
				{
					Type:          v1.CommandCustom,
					Identity:      fmt.Sprintf(component.RegisterTemplateKeyFormat, joinNodeCmd, version, component.TypeStep),
					CustomCommand: bytes,
				},
			},
		},
	}, nil
}

func (stepper *JoinCmd) UninstallSteps() ([]v1.Step, error) {
	return nil, fmt.Errorf("JoinCmd does not support uninstall steps")
}

func (stepper *Drain) InitStepper(hostname string, extraArgs []string) *Drain {
	stepper.Hostname = hostname
	stepper.ExtraArgs = extraArgs
	return stepper
}

func (stepper *Drain) InstallSteps(nodes []v1.StepNode) ([]v1.Step, error) {
	return nil, fmt.Errorf("Drain does not support install steps")
}

func (stepper *Drain) UninstallSteps(nodes []v1.StepNode) ([]v1.Step, error) {
	bytes, err := json.Marshal(stepper)
	if err != nil {
		return nil, err
	}
	return []v1.Step{
		{
			ID:         strutil.GetUUID(),
			Name:       "drainNode",
			Timeout:    metav1.Duration{Duration: 30 * time.Minute},
			ErrIgnore:  false,
			RetryTimes: 1,
			Nodes:      nodes,
			Action:     v1.ActionUninstall,
			Commands: []v1.Command{
				{
					Type:          v1.CommandCustom,
					Identity:      fmt.Sprintf(component.RegisterStepKeyFormat, drain, version, component.TypeStep),
					CustomCommand: bytes,
				},
			},
		},
	}, nil
}

func (stepper *KubeadmJoinUtil) InitStepper(line, cri string) *KubeadmJoinUtil {
	line = strings.TrimLeft(line, " ")
	line = strings.TrimRight(line, " ")
	for _, str := range []string{"  ", "   ", "    ", "     "} {
		line = strings.ReplaceAll(line, str, " ")
	}
	strs := strings.Split(line, " ")
	if len(strs) < 6 {
		stepper = &KubeadmJoinUtil{}
	}
	stepper.ContainerRuntime = cri
	stepper.ControlPlaneEndpoint = strs[2]
	stepper.Token = strs[4]
	stepper.DiscoveryHash = strs[6]
	return stepper
}

func (stepper JoinCmd) NewInstance() component.ObjectMeta {
	return &JoinCmd{}
}

func (stepper JoinCmd) Install(ctx context.Context, opts component.Options) ([]byte, error) {
	if opts.DryRun {
		return nil, nil
	}

	// kubeadm token create --print-join-command
	ec, err := cmdutil.RunCmdWithContext(ctx, opts.DryRun, "kubeadm", "token", "create", "--print-join-command")
	if err != nil {
		logger.Error("run kubeadm token create error", zap.Error(err))
		return nil, err
	}
	cmd := KubeadmJoinUtil{}
	cmd.InitStepper(ec.StdOut(), stepper.ContainerRuntime)
	workerCmd := strings.Join(cmd.GetCmd(), " ")
	if !stepper.ControlPlane {
		// format: ${master node join command};${worker node join command}
		// Work around to split out the worker node join command.
		return []byte("," + workerCmd), nil
	}
	// Control-plane join needs a certificate key that decrypts the certs
	// uploaded to the kubeadm-certs secret; refresh the secret and pick up
	// the fresh key from an existing control-plane member.
	ec, err = cmdutil.RunCmdWithContext(ctx, opts.DryRun, "kubeadm", "init", "phase", "upload-certs", "--upload-certs")
	if err != nil {
		logger.Error("run kubeadm init phase upload-certs error", zap.Error(err), zap.String("stderr", ec.StdErr()))
		return nil, err
	}
	certKey, err := extractCertificateKey(ec.StdOut())
	if err != nil {
		logger.Error("extract certificate key error", zap.Error(err), zap.String("output", ec.StdOut()))
		return nil, err
	}
	// KubeadmConfig.Install splits the master join command on spaces and reads
	// the certificate key at index 9, so the command must not carry extra
	// flags such as --cri-socket (the join config file pins the socket).
	masterCmd := fmt.Sprintf("kubeadm join %s --token %s --discovery-token-ca-cert-hash %s --control-plane --certificate-key %s",
		cmd.ControlPlaneEndpoint, cmd.Token, cmd.DiscoveryHash, certKey)
	return []byte(masterCmd + "," + workerCmd), nil
}

var certificateKeyRegex = regexp.MustCompile(`(?i)certificate key:\s*([0-9a-fA-F]{16,})`)

// extractCertificateKey parses the certificate key printed by
// `kubeadm init phase upload-certs --upload-certs`; the key is printed either
// on the same line as or the line following the "Using certificate key:" hint.
func extractCertificateKey(output string) (string, error) {
	m := certificateKeyRegex.FindStringSubmatch(output)
	if len(m) < 2 {
		return "", fmt.Errorf("certificate key not found in upload-certs output")
	}
	return m[1], nil
}

func (stepper JoinCmd) Uninstall(ctx context.Context, opts component.Options) ([]byte, error) {
	return nil, fmt.Errorf("JoinNodeCmd does not support Uninstall")
}

func (stepper *Drain) NewInstance() component.ObjectMeta {
	return &Drain{}
}

// nodeReady reports the status of the node's Ready condition.
func (stepper *Drain) nodeReady(ctx context.Context, opts component.Options) (bool, error) {
	ec, err := cmdutil.RunCmdWithContext(ctx, opts.DryRun, "kubectl", "get", "node", stepper.Hostname,
		"-o", `jsonpath={.status.conditions[?(@.type=="Ready")].status}`)
	if err != nil {
		return false, err
	}
	return strings.TrimSpace(ec.StdOut()) == "True", nil
}

func (stepper *Drain) Install(ctx context.Context, opts component.Options) (bytes []byte, err error) {
	return
}

func (stepper *Drain) Uninstall(ctx context.Context, opts component.Options) (bytes []byte, err error) {
	var ec *cmdutil.ExecCmd
	var logErrMsg string
	errMsg := fmt.Sprintf("nodes \"%s\" not found", stepper.Hostname)
	defer func() {
		if err != nil {
			if strings.Contains(ec.StdErr(), errMsg) {
				err = nil
				return
			}
			err = errors.New(ec.StdErr())
			logger.Error(logErrMsg, zap.Error(err))
		}
	}()

	ec, err = cmdutil.RunCmdWithContext(ctx, opts.DryRun, "kubectl", "get", "node", stepper.Hostname)
	if err != nil {
		logErrMsg = "kubectl get nodes error"
		return
	}

	// A node whose runtime is wedged (Ready != True) can never drain cleanly:
	// its workloads are already gone or stuck, and disruption budgets have no
	// live pods left to protect. Skip the drain for such nodes — blocking
	// here would hang the removal until the operation deadline — and delete
	// the node object, which is what actually completes the removal. For a
	// Ready node the drain stays mandatory and its failure fails the step.
	if ready, readyErr := stepper.nodeReady(ctx, opts); readyErr == nil && !ready {
		logger.Warnf("node %s is not ready, skipping drain and deleting the node object", stepper.Hostname)
		if ec, err = cmdutil.RunCmdWithContext(ctx, opts.DryRun, "kubectl", "delete", "node", stepper.Hostname); err != nil {
			logErrMsg = "kubectl delete node error"
			return
		}
		return nil, nil
	}

	ec, err = cmdutil.RunCmdWithContext(
		ctx,
		opts.DryRun,
		"kubectl",
		"taint",
		"nodes",
		stepper.Hostname,
		"NoExec=true:NoExecute",
		"--overwrite",
	)
	if err != nil {
		logErrMsg = "kubectl taint node error"
		return
	}

	cmds := strings.Split(fmt.Sprintf("kubectl drain %s", stepper.Hostname), " ")
	cmds = append(cmds, stepper.ExtraArgs...)
	// kubectl drain ${node_name} --ignore-daemonsets --delete-emptydir-data --force
	ec, err = cmdutil.RunCmdWithContext(ctx, opts.DryRun, cmds[0], cmds[1:]...)
	if err != nil {
		logErrMsg = "kubectl drain node error"
		return
	}

	// kubectl delete node ${node_name}
	ec, err = cmdutil.RunCmdWithContext(ctx, opts.DryRun, "kubectl", "delete", "node", stepper.Hostname)
	if err != nil {
		logErrMsg = "kubectl delete node error"
		return
	}

	return
}

func (stepper *KubeadmJoinUtil) GetCmd() []string {
	cmd := fmt.Sprintf("kubeadm join %s --token %s --discovery-token-ca-cert-hash %s",
		stepper.ControlPlaneEndpoint, stepper.Token, stepper.DiscoveryHash)
	if stepper.ContainerRuntime == "containerd" {
		cmd += " --cri-socket /run/containerd/containerd.sock"
	}
	return strings.Split(cmd, " ")
}
