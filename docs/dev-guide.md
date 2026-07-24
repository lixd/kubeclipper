# KubeClipper Development Guide

## 1. Development Environment

### 1.1 Architecture

```txt
Arch
             +------------------+               +-------------------+
             | Ubuntu 24.04 2C4G|               |    Ubuntu 22.04   |
             |   Development    |               | KC-Agent(Optional)|
             +--------+---------+               +----------+--------+
                      |                                    |
                      |                                    |
                      |                                    |
+---------------------+------------------------------------+--------------------+
```

Development Environment：

1. Docker & containerd are both in `/usr/bin/containerd`
2. If deploy cluster, new containerd will be installed in `/usr/local/bin/containerd` by
   kubeclipper, which could co-exist with old containerd

Config SSH (permit root login with id_rsa) & `apt update -y && apt upgrade -y`

```console
# cat /etc/os-release 
PRETTY_NAME="Ubuntu 24.04.2 LTS"
NAME="Ubuntu"
VERSION_ID="24.04"
VERSION="24.04.2 LTS (Noble Numbat)"
VERSION_CODENAME=noble
ID=ubuntu
ID_LIKE=debian
HOME_URL="https://www.ubuntu.com/"
SUPPORT_URL="https://help.ubuntu.com/"
BUG_REPORT_URL="https://bugs.launchpad.net/ubuntu/"
PRIVACY_POLICY_URL="https://www.ubuntu.com/legal/terms-and-policies/privacy-policy"
UBUNTU_CODENAME=noble
LOGO=ubuntu-logo

# uname -a
Linux VM-4-12-ubuntu 6.8.0-51-generic #52-Ubuntu SMP PREEMPT_DYNAMIC Thu Dec  5 13:09:44 UTC 2024 x86_64 x86_64 x86_64 GNU/Linux
```

### 1.2 Install Golang in Development Environment

```bash
wget https://golang.google.cn/dl/go1.24.0.linux-amd64.tar.gz
# wget https://golang.google.cn/dl/go1.24.3.linux-arm64.tar.gz

tar zxvf go1.24.0.linux-amd64.tar.gz
g

mkdir -p /opt/go

vi /etc/profile
```

```bash
export GOROOT=/usr/local/go
export GOPATH=/opt/go
export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
export GOPROXY=https://mirrors.aliyun.com/goproxy/,direct
```

### 1.3 Install Docker for Development Environment

```bash
apt install docker.io docker-compose -y
systemctl enable docker --now
```

Confirm Version

```console
root@VM-4-12-ubuntu:~# which docker
/usr/bin/docker

root@VM-4-12-ubuntu:~# which containerd
/usr/bin/containerd

root@VM-4-12-ubuntu:~# containerd -version
containerd github.com/containerd/containerd 1.7.24

root@VM-4-12-ubuntu:~# docker version
Client:
 Version:           26.1.3
 API version:       1.45
 Go version:        go1.22.2
 Git commit:        26.1.3-0ubuntu1~24.04.1
 Built:             Mon Oct 14 14:29:26 2024
 OS/Arch:           linux/amd64
 Context:           default

Server:
 Engine:
  Version:          26.1.3
  API version:      1.45 (minimum version 1.24)
  Go version:       go1.22.2
  Git commit:       26.1.3-0ubuntu1~24.04.1
  Built:            Mon Oct 14 14:29:26 2024
  OS/Arch:          linux/amd64
  Experimental:     false
 containerd:
  Version:          1.7.24
  GitCommit:        
 runc:
  Version:          1.1.12-0ubuntu3.1
  GitCommit:        
 docker-init:
  Version:          0.19.0
  GitCommit:
```

Config Docker Mirror in China Mainland

```json
vi /etc/docker/daemon.json

{
  "registry-mirrors": [
    "https://docker.m.daocloud.io",
    "https://mirror.baidubce.com",
    "http://hub-mirror.c.163.com"
  ]
}
```

Restart Docker service

```bash
systemctl daemon-reload
systemctl restart docker
```

Test Docker

```bash
docker run hello-world
```

### 1.4 Install Prerequisite Packages

```bash
apt install build-essential git curl wget net-tools -y
apt install ntp -y
```

### 1.5 Clone & compile

```bash
git clone git@github.com:duicikeyihangaolou/kubeclipper.git
cd kubeclipper
git checkout release-1.4

# go clean -modcache
# go mod tidy
# go mod download

make build
```

Check assembly results:

```console
# ls dist/
kcctl  kubeclipper-agent  kubeclipper-server
```

## 2. Installing & Debugging

### 2.1 Deploy KC in Deployment Environment

Deploy `kcctl` as the only bootstrap binary. The old release tarball flow is no
longer the main path; server, agent, package artifacts, and runtime images are
resolved from registries.

```console
# install -m 0755 kcctl-linux-amd64 /usr/local/bin/kcctl
# kcctl version
```

Deploy KC with AIO mode

```bash
# Make sure you can ssh localhost without password for deploy/join operations.
#
# ssh-keygen -t rsa
# cat /root/.ssh/id_rsa.pub >> authorized_keys

# KC_VERSION=release-1.4 kcctl deploy
KC_VERSION=release-1.4 kcctl deploy --server 10.0.4.12 --agent 10.0.4.12 --pk-file ~/.ssh/id_rsa --package-registry registry.local:5000
# kcctl deploy --help
# kcctl deploy --user root --passwd {local-host-user-pwd} --package-registry registry.local:5000
# kcctl deploy --server $IPADDR_SERVER --agent $IPADDR_AGENT --passwd xxx --package-registry registry.local:5000 # or --pk-file
```

`kcctl resource` now talks directly to the OCI package registry. It does not require deploy SSH
credentials, but it does require `--registry`. `kcctl deploy` and `kcctl join` require
`packageRegistry` so both KubeClipper bootstrap assets and Kubernetes offline packages are
resolved from the same OCI source.

```console
# KC_VERSION=release-1.4 kcctl deploy --server 10.0.4.12 --agent 10.0.4.12 --pk-file ~/.ssh/id_rsa --package-registry registry.local:5000
2025-05-30T10:08:59+08:00	INFO	Using auto detected IPv4 address on interface eth0: 10.0.4.12/22
[2025-05-30T10:08:59+08:00][INFO] node-ip-detect inherits from ip-detect: first-found
[2025-05-30T10:08:59+08:00][INFO] run in aio mode.
[2025-05-30T10:08:59+08:00][INFO] ============>kc-etcd PRECHECK ...
[2025-05-30T10:08:59+08:00][INFO] ============>kc-etcd PRECHECK OK!
[2025-05-30T10:08:59+08:00][INFO] ============>kc-server PRECHECK ...
[2025-05-30T10:08:59+08:00][INFO] ============>kc-server PRECHECK OK!
[2025-05-30T10:08:59+08:00][INFO] ============>kc-agent PRECHECK ...
[2025-05-30T10:08:59+08:00][INFO] ============>kc-agent PRECHECK OK!
[2025-05-30T10:08:59+08:00][INFO] ============>TIME-LAG PRECHECK ...
[2025-05-30T10:08:59+08:00][INFO] BaseLine Time: 2025-05-30T10:08:59+08:00
[2025-05-30T10:08:59+08:00][INFO] [10.0.4.12] -0.949546294 seconds
[2025-05-30T10:08:59+08:00][INFO] all nodes time lag less then 5 seconds
[2025-05-30T10:08:59+08:00][INFO] ============>TIME-LAG PRECHECK OK!
[2025-05-30T10:08:59+08:00][INFO] ============>NTP PRECHECK ...
[2025-05-30T10:08:59+08:00][INFO] ============>NTP PRECHECK OK!
[2025-05-30T10:08:59+08:00][INFO] ============>sudo PRECHECK ...
[2025-05-30T10:08:59+08:00][INFO] ============>sudo PRECHECK OK!
[2025-05-30T10:08:59+08:00][INFO] ============>ipDetect PRECHECK ...
[2025-05-30T10:08:59+08:00][INFO] ============>ipDetect PRECHECK OK!
[2025-05-30T10:09:02+08:00][INFO] ------ Send packages ------
[2025-05-30T10:09:02+08:00][INFO] refresh bootstrap assets from OCI registry registry.local:5000
10.0.4.12: done!   
[2025-05-30T10:11:44+08:00][INFO] ------ Install kc-etcd ------
[2025-05-30T10:11:50+08:00][INFO] ------ Install kc-server ------
[2025-05-30T10:12:03+08:00][INFO] ------ Install kc-agent ------
[2025-05-30T10:12:03+08:00][INFO] ------ Install kc-console ------
[2025-05-30T10:12:04+08:00][INFO] ------ Delete intermediate files ------
[2025-05-30T10:12:04+08:00][INFO] ------ Dump configs ------
[2025-05-30T10:12:04+08:00][INFO] ------ Upload configs ------
10.0.4.12: done!   

 _   __      _          _____ _ _
| | / /     | |        /  __ \ (_)
| |/ / _   _| |__   ___| /  \/ |_ _ __  _ __   ___ _ __
|    \| | | | '_ \ / _ \ |   | | | '_ \| '_ \ / _ \ '__|
| |\  \ |_| | |_) |  __/ \__/\ | | |_) | |_) |  __/ |
\_| \_/\__,_|_.__/ \___|\____/_|_| .__/| .__/ \___|_|
                                 | |   | |
                                 |_|   |_|
        repository: github.com/kubeclipper
```

### 2.2 Check Services

```console
# systemctl list-unit-files | grep kc
kc-agent.service                             enabled         enabled
kc-console.service                           enabled         enabled
kc-etcd.service                              enabled         enabled
kc-server.service                            enabled         enabled
```

### 2.3 Clean

```bash
kcctl clean --help

# Uninstall the entire kubeclipper platform.
kcctl clean
kcctl clean -A
```

### 2.4 Debug kcctl using gdb

```bash
apt install gdb -y

export GOLDFLAGS=""
cd ~/kubeclipper
make build
# or
# make build-cli

# Start GDB debugger and load the kcctl binary for debugging
gdb dist/kcctl
# Set a breakpoint at line 278 of resource.go to pause execution
b resource.go:278
# Run kcctl with login command to authenticate against local server
r login --host http://127.0.0.1 --username admin --password Thinkbig1
# Run kcctl to list cluster resources (triggers breakpoint if hit)
r resource list
# Quit debug session
q
```

OR, debug with VSCode, see `.vscode/launch.json`.

## 3. Build OCI Offline Resources

The open packaging flow builds every resource from public upstream sources. It does not
consume the old static server directory or company-internal download services.

```bash
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --output /data/kubeclipper-resources \
  --registry registry.local:5000 \
  --image-registry registry.local:5000 \
  --arch amd64 \
  --include-bootstrap \
  --push
```

The output directory contains build-side metadata such as `images.lock`,
`charts.lock`, `build-report.json`, and `release-manifest.yaml`. Package payloads are
published as standard OCI images, charts are published as Helm OCI, and runtime images
remain standard container images. KubeClipper server does not consume the lock files.

The current offline-package workflow is:

1. Build and publish standard package OCI images, Helm OCI charts, and runtime images with `scripts/open-packaging/build-offline-resources.sh`.
2. Use the generated `release-manifest.yaml` to mirror the release to a local Registry or Harbor.
3. Optionally verify exact release references with `scripts/open-packaging/verify-release-manifest.sh`.
4. Maintain the supported component matrix with `kcctl delivery-policy`.
5. Set `packageRegistry` for `kcctl deploy`/`kcctl join`.
6. Use `kcctl resource list|inspect|refresh --registry <registry>` to inspect Registry-derived package/chart inventory.
7. Let install/join resolve packages from policy + inventory and fetch them by digest.

For an offline Kubernetes cluster, `--image-registry` selects a KubeClipper Registry resource.
KubeClipper assumes that Registry already contains the container images required by kubeadm
and the selected CNI. The package Registry remains independently configured by
`deployConfig.packageRegistry`; the two registries may use the same host but are resolved and
configured separately.

Runtime image lists are release-side metadata only. The build scripts generate `images.lock`
and aggregate it into `release-manifest.yaml` for publishing, mirroring, offline bundle
creation, and optional verification. KubeClipper server does not store this list and does not
perform Registry `HEAD` checks before cluster creation. Kubeadm, containerd, and kubelet
validate runtime images by actually pulling them on cluster nodes.

Command status after the OCI switch:

| Command or field | Status | Meaning |
| --- | --- | --- |
| `scripts/open-packaging/build-offline-resources.sh` | Release entry point | Build from public sources and publish package images, Helm OCI charts, and runtime images. |
| `scripts/open-packaging/generate-release-manifest.sh` | Release helper | Generate the synchronization and offline delivery manifest. |
| `scripts/open-packaging/verify-release-manifest.sh` | Optional verification | Verify exact package, chart, and runtime image references in a target Registry. |
| `scripts/open-packaging/export-offline-registry-bundle.sh` | Air-gap export | Export one architecture as checksummed, digest-preserving Registry seed data. |
| `scripts/open-packaging/import-offline-registry-bundle.sh` | Air-gap import | Import a bundle into any OCI Registry or Harbor without requiring Docker/containerd. |
| `tools/oci-publish` | Package image publisher | Assemble and push standard OCI package images. |
| `kcctl registry deploy --package-registry/--offline-bundle` | OCI bootstrap | Pull the Registry package image online, or bootstrap directly from the checksummed offline Registry bundle. |
| `kcctl resource list/inspect/refresh --registry` | Existing command, OCI-only semantics | Inspect Registry-derived inventory; no static-server SSH or `--transport` mode. |
| `kcctl delivery-policy` | OCI delivery capability | Maintain the supported component/version matrix; it does not upload packages. |
| `packageRegistry` | Deploy/join config | Registry source for offline package resolution and fetch. |
| `staticServer` / `staticServerPath` | Removed from the main flow | Do not use it for OCI package delivery. |

The complete release publishes four bootstrap package images, but they have
separate consumers. `kcctl registry deploy` fetches only `bootstrap/registry`;
`kcctl deploy` fetches `bootstrap/kubeclipper`, `bootstrap/etcd`, and
`bootstrap/console`. KubeClipper server/agent selection is pinned to the
caller's source commit recorded in `sourceRevision`, while join pins the agent
to the running server's commit. Registry tag ordering is never used as a
compatibility decision.

### Package Registry authentication and TLS

The package Registry is independent from the Registry resources selected by
`--image-registry` and `--cri-registry`. The former supplies KubeClipper OCI
packages and charts; the latter two configure containerd sources for Kubernetes
and workload images. They may point to the same Harbor instance, but their
credentials and repository paths are configured separately.

Package Registry access uses HTTPS with system trust by default. For a private
Harbor project, use a read-only robot account and pass the token through a file:

```bash
chmod 0600 harbor-robot-token
kcctl deploy \
  --server <server-ip> \
  --agent <agent-ip> \
  --pk-file ~/.ssh/id_rsa \
  --package-registry harbor.example.com/kubeclipper \
  --package-registry-username 'robot$kubeclipper-reader' \
  --package-registry-password-file harbor-robot-token \
  --package-registry-ca-file harbor-ca.pem
```

`kcctl` writes a mode `0600` Registry client configuration to each server and
agent before starting services. Passwords are not stored in the deploy ConfigMap
or passed as plaintext command-line arguments. `kcctl join` inherits this file
from an existing server through the independently configured server SSH
transport. Supplying any package Registry TLS/auth option, or changing
`--package-registry`, performs an explicit transactional rotation across all
existing servers and agents before committing the new deploy configuration.

Use `--package-registry-scheme http` only for an explicitly unauthenticated test
Registry. `--package-registry-skip-tls-verify` is available for qualification,
but a trusted CA is required for production. `kcctl clean --all` removes the
server and agent Registry credential directories. Registry-side robot accounts,
projects, tags, and garbage collection remain operator responsibilities. Harbor
with Catalog support is required for dynamic inventory of a project prefix;
GHCR does not provide the `/v2/_catalog` behavior required by that operation.

Pure OCI quick run:

```bash
export REGISTRY=registry.local:5000
export RESOURCE_DIR=/data/kubeclipper-resources
export KC_SERVER=https://127.0.0.1:8080
export K8S_VERSION=v1.36.1
export CRI_VERSION=2.2.4
export CNI_VERSION=v3.31.5
export KUBECLIPPER_VERSION=v2.0.0

# 1. Deploy a local Registry with KubeClipper's built-in registry command.
kcctl registry deploy \
  --node <registry-node-ip> \
  --pk-file ~/.ssh/id_rsa \
  --package-registry ghcr.io/kubeclipper/kubeclipper \
  --version 3.1.1 \
  --registry-port 5000

# If the Registry is already running, this health check should return an empty JSON
# object or HTTP 200-compatible /v2/ response.
curl -f "http://${REGISTRY}/v2/"

# Register the Kubernetes image Registry with KubeClipper. The cluster command
# references this resource by name, not by its raw host.
kcctl create registry \
  --name kubernetes-images \
  --host "${REGISTRY}" \
  --scheme http \
  --skip-tls-verify

# For a published release, mirror its complete OCI BOM without requiring
# Docker, containerd, crane, or skopeo. The default manifest matches kcctl.
kcctl registry sync \
  --registry harbor.example.com/kubeclipper \
  --registry-username 'robot$kubeclipper-writer' \
  --registry-password-file harbor-token \
  --registry-ca-file harbor-ca.pem

# 2. Build from public sources and publish package images, Helm charts, and
# standard runtime images directly to the local Registry.
scripts/open-packaging/build-offline-resources.sh \
  --manifest packaging/resources.yaml \
  --output "${RESOURCE_DIR}" \
  --registry "${REGISTRY}" \
  --image-registry "${REGISTRY}" \
  --arch amd64 \
  --include-bootstrap \
  --push

# If GitHub Release assets cannot be downloaded directly, use a local file:
# kcctl registry sync --manifest release-manifest-v2.0.0.yaml ...

# 3. The release manifest is for mirroring/offline delivery validation only.
# It is not uploaded to the KubeClipper control plane.
scripts/open-packaging/generate-release-manifest.sh \
  --build-manifest packaging/resources.yaml \
  --resource-dir "${RESOURCE_DIR}" \
  --output "${RESOURCE_DIR}/release-manifest.yaml" \
  --include-bootstrap \
  --resolve-digests \
  --source-revision "$(git rev-parse HEAD)"

scripts/open-packaging/verify-release-manifest.sh \
  --manifest "${RESOURCE_DIR}/release-manifest.yaml" \
  --registry "${REGISTRY}" \
  --arch amd64 \
  --insecure

# For a physically disconnected environment, run this while the source
# Registry is reachable, carry the resulting tar.gz across the air gap, then
# import it after kcctl registry deploy or into an existing Harbor instance.
scripts/open-packaging/export-offline-registry-bundle.sh \
  --manifest "${RESOURCE_DIR}/release-manifest.yaml" \
  --arch amd64 \
  --output kubeclipper-offline-registry-bundle-v2.0.0-amd64.tar.gz

# On an air-gapped host with no existing Registry, bootstrap from this same bundle.
kcctl registry deploy \
  --node 10.0.0.10 \
  --offline-bundle kubeclipper-offline-registry-bundle-v2.0.0-amd64.tar.gz

scripts/open-packaging/import-offline-registry-bundle.sh \
  --bundle kubeclipper-offline-registry-bundle-v2.0.0-amd64.tar.gz \
  --registry "${REGISTRY}" \
  --insecure-destination

# 4. Confirm Registry-derived package/chart inventory.
kcctl resource list --registry ${REGISTRY} --refresh
kcctl resource inspect --registry ${REGISTRY} --name k8s --version ${K8S_VERSION} --arch amd64 -o yaml

# 5. Deploy KubeClipper with the package Registry. This creates the default
# delivery policy in the control plane on first deployment.
kcctl deploy \
  --server <server-ip> \
  --agent <agent-ip> \
  --pk-file ~/.ssh/id_rsa \
  --package-registry ${REGISTRY}

# 6. Optional: inspect or replace the default support matrix after deployment.
kcctl login --host ${KC_SERVER} --username admin --password '<password>'
kcctl delivery-policy get -o yaml
kcctl delivery-policy template -o yaml > delivery-policy.yaml
kcctl delivery-policy validate -f delivery-policy.yaml
kcctl delivery-policy apply -f delivery-policy.yaml

# 7. Optional: join more agent nodes with the same package Registry.
kcctl join \
  --agent <new-agent-ip> \
  --pk-file ~/.ssh/id_rsa \
  --package-registry ${REGISTRY}

# If the existing server uses a different SSH endpoint, keep the transports
# separate instead of reusing the agent port and credentials for certificate
# retrieval:
kcctl join \
  --agent <new-agent-ip> \
  --pk-file ~/.ssh/agent_id_rsa \
  --server-ssh-user root \
  --server-ssh-port 2202 \
  --server-ssh-pk-file ~/.ssh/server_id_rsa \
  --package-registry ${REGISTRY}

# 8. Create an offline Kubernetes cluster from policy + inventory.
kcctl create cluster \
  --name demo \
  --master <master-node-id-or-ip> \
  --offline=true \
  --cri containerd \
  --cri-version ${CRI_VERSION} \
  --k8s-version ${K8S_VERSION} \
  --cni calico \
  --cni-version ${CNI_VERSION} \
  --image-registry kubernetes-images

# 9. Add or remove Kubernetes worker nodes after the cluster is running.
# The worker value may be a KubeClipper node ID or its primary IPv4 address.
kcctl cluster add-node \
  --cluster-name demo \
  --worker <worker-node-id-or-ip>

kcctl cluster remove-node \
  --cluster-name demo \
  --worker <worker-node-id-or-ip>

# For a single-node control-plane-only test cluster, remove control-plane taints
# if CoreDNS remains Pending because there is no worker node.
kubectl taint node <node-name> node-role.kubernetes.io/control-plane:NoSchedule- || true
kubectl taint node <node-name> node-role.kubernetes.io/master:NoSchedule- || true
```

Each package workflow writes its `github.sha` to the package manifest and the
OCI `org.opencontainers.image.revision` label. Release assembly passes the
kcctl release commit with `--source-revision`. Every package platform must have
provenance and all platforms under one tag must agree; specifically,
`bootstrap/kubeclipper` must match the kcctl commit. Third-party components may
retain their own earlier source revision. The command also pins every artifact
by digest before an offline Registry bundle is exported.

Worker-node additions use the same OCI resolver as cluster creation. Before an
`AddNodes` operation is queued, KubeClipper resolves containerd, k8s-extension,
and Kubernetes package images for the target architecture and stores the
digest-pinned plan with the pending operation. `RemoveNodes` uninstalls the
components already present on the node and does not fetch package content from
the Registry. `kcctl join` only installs and registers a KubeClipper agent; use
`kcctl cluster add-node` when that agent should become a Kubernetes worker.

`delivery-policy` is part of the OCI delivery flow. Registry inventory proves that package
bytes exist; delivery policy proves that a Kubernetes/component version combination is
supported. Both must match before an offline install can resolve a digest-pinned plan.
Unlike the old static resource index, policy does not store package URLs, digests, or
availability state.

`delivery-policy` contains only component compatibility rules. It deliberately does not
contain runtime image names. Adding a Kubernetes or Calico version requires publishing its
package/chart and runtime images, regenerating the release manifest, and updating the policy
when the new combination should be supported; it does not require editing server code.

Shell completion for `kcctl create cluster --k8s-version`, `--cri-version`, and
`--cni-version` is generated from the ComponentMeta projection. This projection
is the intersection of Registry-derived package inventory and `delivery-policy`,
so completion only offers versions that are both published and supported. It no
longer switches between separate online/offline metadata sources. CRI and CNI
completion is scoped to the selected Kubernetes version's policy rule. When the
version flags are omitted, `kcctl` selects the newest published and supported
Kubernetes version and then uses that rule's default CRI and CNI versions.

Registry inventory reads only OCI descriptors and
`/opt/kubeclipper/resource/kc-package-manifest.json`; it does not download package
payloads while listing versions. Package payload digests are checked by the
publisher/verifier and checked again by the fetcher when an installation actually
extracts the package.

Kubernetes and containerd binaries are always resolved as OCI packages. Setting
`--offline=false` only allows runtime images to use their upstream registries;
it does not restore HTTP/static-server package downloads. The open OCI release
currently supports containerd as the cluster runtime.

### 3.3 Deploy k8s using kcctl, add pause tag

**TODO** A bug, will check later.

Modify `/etc/containerd/config.toml`

```bash
vi /etc/containerd/config.toml
# from
# sandbox_image = "registry.k8s.io/pause:"
# to
# sandbox_image = "registry.k8s.io/pause:3.10"
```

Then, `systemctl restart containerd`

Pause installation from webpage, then resume from the breakpoint.

You also can backup /tmp/.k8s and reboot host server, however pause & resume from webpage is much
eaiser.

```bash
# Just log the steps here FYI.
mkdir ~/bak -p
cp /tmp/.k8s ~/bak -a
# after reboot
cp ~/bak/.k8s /tmp -a
# continue deploy k8s with web GUI
```

Check version:

```console
# kubectl get po -A
... # all running

# kubectl version
Client Version: v1.32.5
Kustomize Version: v5.5.0
Server Version: v1.32.5
```

## 4. Demo with kubeedge

### 4.1 kind deploy k8s

```bash
# go 1.16+ and docker, podman or nerdctl installed 

# cat portmapping.yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
  extraPortMappings:
  - containerPort: 10000
    hostPort: 10000
  - containerPort: 10001
    hostPort: 10001
  - containerPort: 10002
    hostPort: 10002
  - containerPort: 10003
    hostPort: 10003
  - containerPort: 10004
    hostPort: 10004
    # optional: set the bind address on the host
    # 0.0.0.0 is the current default
    # listenAddress: "127.0.0.1"
    # optional: set the protocol to one of TCP, UDP, SCTP.
    # TCP is the default
    protocol: TCP

kind create cluster --config portmapping.yaml

# kind delete cluster

# cp to host computer
docker exec  kind-control-plane tar -c -f -  /usr/bin/kubectl > kubectl.tar
tar xf kubectl.tar
mv usr/bin/kubectl /usr/bin/
```

### 4.2 deploy kubeedge with keadm

```bash
# cloudnode
wget https://github.com/kubeedge/kubeedge/releases/download/v1.20.0/keadm-v1.20.0-linux-amd64.tar.gz
# scp keadm-v1.20.0-linux-amd64.tar.gz edgenode
tar -zxvf keadm-v1.20.0-linux-amd64.tar.gz
cp keadm-v1.20.0-linux-amd64/keadm/keadm /usr/local/bin/keadm
keadm version

wget https://github.com/kubeedge/kubeedge/releases/download/v1.20.0/kubeedge-v1.20.0-linux-amd64.tar.gz
wget https://github.com/kubeedge/kubeedge/releases/download/v1.20.0/edgesite-v1.20.0-linux-amd64.tar.gz

mkdir /etc/kubeedge
cp kubeedge-v1.20.0-linux-amd64.tar.gz  edgesite-v1.20.0-linux-amd64.tar.gz /etc/kubeedge

docker pull kubeedge/installation-package:v1.20.0
docker pull kubeedge/iptables-manager:v1.20.0
docker pull kubeedge/cloudcore:v1.20.0
kind load docker-image kubeedge/installation-package:v1.20.0 --name kind
kind load docker-image kubeedge/iptables-manager:v1.20.0 --name kind
kind load docker-image kubeedge/cloudcore:v1.20.0  --name kind

# keadm init --advertise-address="THE-EXPOSED-IP" --kubeedge-version=v1.20.0
keadm init  --advertise-address=192.168.1.208 -v 7

docker exec  kind-control-plane ss -tuln

kubectl get all -n kubeedge 
kubectl get nodes
# kubectl  delete nodes k8s-node1

keadm gettoken


# edgenode

vi /etc/containerd/config.toml

runtime_type = "io.containerd.runc.v2"
# disabled_plugins = ["cri"]
disabled_plugins = []
SystemdCgroup = true


# edge node install cni for cri
wget https://github.com/containernetworking/plugins/releases/download/v1.6.2/cni-plugins-linux-amd64-v1.6.2.tgz
mkdir -p /opt/cni/bin
tar Cxzvf /opt/cni/bin cni-plugins-linux-amd64-v1.6.2.tgz

mkdir -p /etc/cni/net.d/
$ cat >/etc/cni/net.d/10-containerd-net.conflist <<EOF
{
  "cniVersion": "1.0.0",
  "name": "containerd-net",
  "plugins": [
    {
      "type": "bridge",
      "bridge": "cni0",
      "isGateway": true,
      "ipMasq": true,
      "promiscMode": true,
      "ipam": {
        "type": "host-local",
        "ranges": [
          [{
            "subnet": "10.88.0.0/16"
          }],
          [{
            "subnet": "2001:db8:4860::/64"
          }]
        ],
        "routes": [
          { "dst": "0.0.0.0/0" },
          { "dst": "::/0" }
        ]
      }
    },
    {
      "type": "portmap",
      "capabilities": {"portMappings": true}
    }
  ]
}
EOF

systemctl restart containerd.service

ctr -n k8s.io image ls

docker pull kubeedge/installation-package:v1.20.0
docker save kubeedge/installation-package:v1.20.0 -o keip.tar
ctr -n k8s.io image import keip.tar

docker pull pause:3.8
docker save pause:3.8 -o pause.3.8.tar
ctr -n k8s.io image import pause.3.8.tar


ctr -n k8s.io image pull docker.io/library/eclipse-mosquitto:1.6.15
# or
docker pull eclipse-mosquitto:1.6.15
docker save eclipse-mosquitto:1.6.15 -o tt.tar
ctr -n k8s.io image import tt.tar

docker pull docker.io/kindest/kindnetd:v20250214-acbabc1a
docker save docker.io/kindest/kindnetd:v20250214-acbabc1a -o kek.tar
ctr -n k8s.io image import kek.tar

docker pull registry.k8s.io/kube-proxy:v1.32.2
docker save registry.k8s.io/kube-proxy:v1.32.2 -o kep.tar
ctr -n k8s.io image import kep.tar

ctr -n k8s.io image  ls

ctr -n  kube-system c ls
ctr -n  kube-system t ls
ctr -n  kube-system i ls


keadm join  --cloudcore-ipport=172.18.0.2:10000 \
--token=keadm-gettoken-cloudnode --kubeedge-version=v1.20.0 \
-p=unix:///var/run/docker.sock

systemctl status  edgecore.service
# systemctl restart  edgecore.service
journalctl -u edgecore.service -xe
```

### 4.3 Config kube api in kubeedge node

#### 4.3.1 CloudCore side

```bash
kubectl edit cm cloudcore -n kubeedge

      dynamicController:
        enable: true

docker exec -it kind-control-plane bash
crictl pods
# restart cloudcore pod
crictl stopp cloudcore-5d9ccb9dc8-lv2qb
```

#### 4.3.2 EdgeCore Side

```bash
vi /etc/kubeedge/config/edgecore.yaml
modules:
  ...
  edgeMesh:
    enable: false
  ...
  metaManager:
    metaServer:
      enable: true

vi /etc/kubeedge/config/edgecore.yaml
modules:
  ...
  edged:
    ...
    tailoredKubeletConfig:
      ...
      clusterDNS:
      - 169.254.96.16
      clusterDomain: cluster.local

systemctl restart  edgecore.service

# metaServer port 10550
netstat -natp|grep 10550
curl 127.0.0.1:10550/api/v1/services
curl http://127.0.0.1:10550/api/v1/namespaces/kube-system/pods|jq '.items.[].metadata.name'

"coredns-668d6bf9bc-j8cq4"
"coredns-668d6bf9bc-kf4cl"
"etcd-kind-control-plane"
"kindnet-5bgh7"
"kindnet-qsllc"
"kube-apiserver-kind-control-plane"
"kube-controller-manager-kind-control-plane"
"kube-proxy-92w6x"
"kube-proxy-rcjfq"
"kube-scheduler-kind-control-plane"


# cloud
kubectl get pods -n kube-system -o wide

# edge memroy
ps -p <PID> -o rss
```

### 4.4 Deploy hostnetwork application

#### 4.4.1 edge node

```bash
docker pull nginx
docker save nginx:latest -o nginx.tar
ctr -n k8s.io image import nginx.tar
ctr image import nginx.tar
ctr -n k8s.io image list
ctr image list
netstat -natp|grep nginx
```

#### 4.4.2 cloud node

```bash
kubectl apply -f hostnginx.yaml
kubectl get pods -o wide
kubectl describe pod  nginx-deployment-799c65967c-q8nvt
```

#### 4.4.3 Application yaml file

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-deployment
  labels:
    app: nginx
spec:
  replicas: 1
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      hostNetwork: true
      containers:
      - name: nginx
        image: docker.io/library/nginx:latest
        imagePullPolicy: Never
        ports:
        - containerPort: 80
          hostPort: 80
```
