# 在线与离线交付场景测试步骤

本文件供 Agent 按步骤执行，与[核心功能清单](core-feature-checklist.md)中的
`1.1-01`、`1.1-03`、`1.1-11`、`1.1-07`、`1.1-08`、`3-18`、`3-29` 对应。
每轮建立独立结果记录，填写实际候选提交、制品来源、主机、命令退出码和证据路径。
每条场景依次完成 kcctl 准备、Registry 部署、KC 部署、Agent 纳管、创建集群及健康验收。

## 执行准备

1. 确认目标主机及清理授权，记录服务、监听端口、磁盘空间、现有集群和 Registry。
   保存平台配置、原二进制和 etcd 快照，恢复资料放在权限为 0700 的私有目录。
   清理配置中的 `serverIPs` 和 `agents` 必须全部属于本轮目标主机。
   `serverIPs` 只填写实际部署 Server 的主机，`agents` 包含后续 `join` 纳管的节点。
2. 确认本轮 kcctl、bootstrap package 和清单对应同一版本，记录实际构建提交。
   fork qualification 测试使用该候选的 Registry 前缀，正式发行测试使用发行默认值。
3. 每轮使用独立缓存目录，CLI、Server、Agent 分别配置 `KUBECLIPPER_CACHE_DIR`。
   通过 systemd drop-in 设置服务环境变量，保存原配置并执行 `daemon-reload`。
   清点 containerd 的现有镜像与数据，冷启动轮次使用空的测试运行时存储。
   从 containerd 配置的 `root` 获取实际存储路径，并核对原生删除集群后该路径已清理。
   大归档导出、全文件校验与集群健康验收顺序执行，记录主机磁盘 I/O 压力。
4. 两台主机均准备 SSH、可用的时间同步和支持的 Linux 系统。记录 CPU 架构。
   多网卡环境确认节点之间可达的接口。示例使用 Ubuntu 24.04、amd64 和 `ens3`。
   现场验收使用 Python 3 标准库，将本目录 `scripts/verify-offline-import.py` 和
   `scripts/verify-registry-architectures.py` 一并复制到测试主机的本轮 `scripts` 目录。
5. 准备以下变量，按现场地址填写。管理员测试口令使用 8–16 位，包含大小写字母和数字。

```bash
set -euo pipefail
export KC_VERSION=v2.0.11
export KC_TEST_SERVER=172.16.131.146
export KC_TEST_WORKER=172.16.131.230
export KC_TEST_SSH_KEY=/root/test-private/id_ed25519
export KC_TEST_IFACE=ens3
export KC_TEST_DIR=/root/kc-delivery-test
export KC_TEST_REGISTRY_PORT=5627
export KC_TEST_LOCAL_REGISTRY="$KC_TEST_SERVER:$KC_TEST_REGISTRY_PORT/kubeclipper-release"
export KC_TEST_ADMIN_PASSWORD="Aa1$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
umask 077
mkdir -p "$KC_TEST_DIR/scripts"
chmod 0700 "$KC_TEST_DIR"
```

后续命令使用本轮 Bash 环境。开启新会话时重新设置上述变量和严格退出选项，
每步保存退出码，失败后停止依赖该结果的步骤。

先检查二进制和命令入口，记录输出。

```bash
kcctl version
kcctl registry export --help
kcctl registry import --help
```

`registry export` 的默认目录为当前工作目录下的 `offline`，生成完整发行归档和独立
Registry 小包。默认读取 kcctl 内置清单。`--manifest` 用于显式覆盖清单。

## 共同的部署配置

在当前场景的证据目录保存私有 `deploy.yaml`，先部署 Server 主机上的一个 Agent，
再通过 `join` 纳管 Worker，以覆盖两个取包入口。每轮从下列模板生成新文件。
`deploy` 会把解析后的仓库和节点信息保存回配置文件，下一场景应重新生成配置。

```bash
cat > "$KC_TEST_DIR/deploy.yaml" <<EOF
ssh:
  user: root
  port: 22
  pkFile: $KC_TEST_SSH_KEY
serverIPs:
  - $KC_TEST_SERVER
agents:
  $KC_TEST_SERVER:
    region: default
ipDetect: interface=$KC_TEST_IFACE
nodeIPDetect: interface=$KC_TEST_IFACE
tls: true
serverPort: 8080
consolePort: 80
tempDir: $KC_TEST_DIR/temp
authentication:
  initialPassword: $KC_TEST_ADMIN_PASSWORD
EOF
chmod 0600 "$KC_TEST_DIR/deploy.yaml"
```

部署配置中的路径、地址、接口必须与本轮变量一致。设置文件权限为 0600。
在线场景通过所在区域的默认 Package Registry 部署，离线场景显式传入本地仓库。

## 1.1-01 国外在线安装

### 网络和下载

1. 配置测试环境需要的 VPN 或 HTTP 代理。CLI、KC Server、两个 Agent 和 containerd
   都应具备公网访问能力。`NO_PROXY` 包含两台主机、loopback、Pod/Service 网段、
   `.svc` 和集群 DNS 后缀。大小写代理变量设置一致，检查既有 systemd drop-in 的优先级，
   通过 `systemctl show <服务> --property=Environment` 核对最终生效值。
   逐台验证 GHCR 访问并记录代理配置位置。
2. 使用发行入口安装指定版本，保存安装日志和下载 URL。

```bash
export KCZONE=global
curl -sfLk https://oss.kubeclipper.io/get-kubeclipper.sh -o get-kubeclipper.sh
KC_VERSION="$KC_VERSION" bash get-kubeclipper.sh
kcctl version
```

预期 kcctl 和 `kcctl-checksums.txt` 从 GitHub Release 下载，校验通过，运行版本正确。
对 fork qualification 候选，记录实际二进制来源和下载覆盖参数。发行附件尚未发布时，
下载子步骤记录为待发布验证，并继续使用该提交构建的二进制执行部署子步骤。

### Registry、平台与集群

1. 在空的本轮 Registry 数据目录执行在线自举。

```bash
kcctl registry deploy \
  --node "$KC_TEST_SERVER" --pk-file "$KC_TEST_SSH_KEY" \
  --registry-port "$KC_TEST_REGISTRY_PORT" \
  --data-root "$KC_TEST_DIR/registry"
curl -fsS "http://$KC_TEST_SERVER:$KC_TEST_REGISTRY_PORT/v2/"
```

预期通过 GHCR 中的 `package-bootstrap-registry` 获取目标架构的 Registry 二进制，
服务 active，`/v2/` 返回 200。保存取包地址、实际二进制版本及服务配置。

2. 从空缓存部署平台，保存完整退出码和日志。

```bash
kcctl deploy --deploy-config "$KC_TEST_DIR/deploy.yaml" --assumeyes
kcctl login --host "https://$KC_TEST_SERVER:8080" --username admin \
  --password "$KC_TEST_ADMIN_PASSWORD"
kcctl join --agent "$KC_TEST_WORKER" --pk-file "$KC_TEST_SSH_KEY" \
  --ip-detect "interface=$KC_TEST_IFACE" --node-ip-detect "interface=$KC_TEST_IFACE"
kcctl get node -o json
kcctl get registry -o json
kcctl doctor
```

预期一个 Server、两个 Agent 健康，Console 可访问。核对运行中 Server、Agent 的
提交版本，默认镜像 Registry 的地址为本次 GHCR 前缀。保存 doctor 的完整状态。

3. 使用平台自动初始化的镜像仓库创建集群。

```bash
kcctl create cluster --name delivery-global \
  --master "$KC_TEST_SERVER" --worker "$KC_TEST_WORKER" \
  --k8s-version v1.37.0 --cri containerd --cri-version 2.2.4 \
  --cni calico --cni-version v3.31.5 \
  --calico.ipv4-auto-detection "interface=$KC_TEST_IFACE" \
  --calico.net-mode Overlay-Vxlan-All --assumeyes
```

等待 Cluster 为 Running、CreateCluster Operation 为 Succeeded，然后执行下文的共同健康验收。
记录 packagePlan 和实际运行镜像的 Registry 前缀，均应指向本轮 GHCR 仓库。

## 1.1-11 国内在线安装

### 网络和下载

1. 正常删除前一场景集群并清理测试平台。移除本轮代理 drop-in、关闭代理隧道，
   清除 CLI 与服务的大小写代理环境变量，建立新的空缓存目录。
2. 用直连请求验证阿里云 Registry 和 OSS，保存请求结果、服务环境及网络配置。
   Registry `/v2/` 的 401 作为认证挑战记录，公开仓库的真实制品拉取应能匿名完成。
3. 通过区域变量安装指定版本。

```bash
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy
export KCZONE=cn
curl -sfLk https://oss.kubeclipper.io/get-kubeclipper.sh -o get-kubeclipper.sh
KC_VERSION="$KC_VERSION" bash get-kubeclipper.sh
kcctl version
```

预期二进制和校验文件下载 URL 位于 OSS 的同版本目录，Linux 架构文件名与 GitHub
Release 一致。新版本尚未上传 OSS 时，记录下载子步骤的实际状态与候选二进制来源。

### Registry、平台与集群

1. 执行与国外场景相同的 `registry deploy`、`deploy`、`login`、`join` 命令，
   保持 `KCZONE=cn`。使用本轮的新 Registry 数据目录和平台配置。
2. 核对日志及 Server/Agent 的交付配置，Package Registry 应自动解析为
   `registry.cn-hangzhou.aliyuncs.com/kubeclipper-release`。Registry 自举也应来自该仓库。
3. 检查自动初始化的 `kc-package-registry` 指向上述阿里云命名空间，创建
   `delivery-cn` 集群，参数与国外场景相同，执行共同健康验收。
4. 保存 packagePlan、实际 Pod 镜像和取包日志。核对全过程的代理环境为空。
5. 显式仓库覆盖检查与下文离线部署合并执行。在 `KCZONE=cn` 下传入本地仓库前缀，
   核对 CLI、Server、Agent 实际使用显式值。

国内场景的通过条件包括直连阿里云获取 bootstrap package、安装 package、chart 与运行
镜像，完成两 Agent 纳管和真实双节点建群。保存每个子步骤的独立结果。

## 1.1-03 完整离线安装

### 联网机器导出

1. 在联网机器安装与候选版本一致的 kcctl。选择已验证的在线源，建立空的工作目录。
   使用默认内置清单导出完整发行物料。

```bash
mkdir -p "$KC_TEST_DIR/export"
cd "$KC_TEST_DIR/export"
kcctl registry export
ls -lh offline
```

预期 `offline` 中出现以下两个文件，记录实际文件名与大小。

```text
offline/
  registry-3.1.1.tar.gz
  kubeclipper-offline-v2.0.11.tar.gz
```

2. 核对导出日志覆盖所有发行清单中的仓库/tag，并保留源索引的所有架构。
   使用 `KCZONE=cn` 验证从阿里云导出，使用 `--package-registry` 验证自定义源覆盖。
   外部清单覆盖作为独立子步骤执行并单独记录。
3. 将同版本 kcctl、两个归档和本轮文件校验记录传输到离线部署机器。逐文件核对
   传输前后 SHA256，保存源、目标机器及传输退出码。

### 隔离公网与 Registry 自举

1. 正常删除在线场景集群并清理平台，检查目标 Registry 使用空的数据目录。
   使用新的 CLI、Server、Agent 缓存和测试运行时存储。
2. 关闭代理隧道，移除代理配置，清除环境变量。对 CLI、Server、Agent 和 Kubernetes
   两台主机都实施公网隔离，保留 SSH、两台主机互通、loopback、Pod/Service 网段。
   IPv4、IPv6 的出口规则均需检查。先准备恢复命令并验证管理连接。
3. 验证公网 GHCR、阿里云 Registry、OSS 请求均失败，同时 SSH 和本地网络仍可达。
   保存规则、计数器及实际请求结果，后续部署阶段持续保留隔离。
   隔离前记录这些域名的公网地址，隔离后增加 `curl --resolve` 直连检查。
   Ubuntu 测试主机可使用下列独立 nftables 表，同时覆盖主机进程和 Pod 转发流量。
   在两台主机分别检查语法并加载，预先保存删除该表的恢复命令。

```bash
cat > "$KC_TEST_DIR/offline-egress.nft" <<EOF
table inet kc_delivery_test {
  chain allowed_egress {
    oifname "lo" accept
    ct direction reply accept
    ip daddr { $KC_TEST_SERVER, $KC_TEST_WORKER, 172.25.0.0/16, 10.96.0.0/12, 169.254.169.100 } accept
    counter reject with icmpx type admin-prohibited
  }
  chain host_output {
    type filter hook output priority -10; policy accept;
    jump allowed_egress
  }
  chain pod_forward {
    type filter hook forward priority -10; policy accept;
    jump allowed_egress
  }
}
EOF
# 在每台目标机执行，文件先传输到该主机的本轮目录。
nft --check --file "$KC_TEST_DIR/offline-egress.nft"
nft --file "$KC_TEST_DIR/offline-egress.nft"
nft list table inet kc_delivery_test
# 本轮结束时的恢复命令。
# nft delete table inet kc_delivery_test
```

该表保留入站管理连接的回复、两台测试主机互通以及所列集群网段，阻断其余 IPv4/IPv6
出站连接。每轮检查表名是否冲突，按实际 Pod/Service 网段调整白名单。

4. 仅使用已传输的小包自举 Registry。

```bash
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY http_proxy https_proxy all_proxy KCZONE
kcctl registry deploy \
  --node "$KC_TEST_SERVER" --pk-file "$KC_TEST_SSH_KEY" \
  --bundle "$KC_TEST_DIR/materials/registry-3.1.1.tar.gz" \
  --registry-port "$KC_TEST_REGISTRY_PORT" \
  --data-root "$KC_TEST_DIR/registry-offline"
curl -fsS "http://$KC_TEST_SERVER:$KC_TEST_REGISTRY_PORT/v2/"
```

预期本地小包提供正确架构的 Registry 二进制，空 Registry 服务启动且健康检查为 200。
保存服务、二进制、监听地址和空仓库检查结果。

```bash
python3 "$KC_TEST_DIR/scripts/verify-registry-architectures.py" \
  --bundle "$KC_TEST_DIR/materials/registry-3.1.1.tar.gz" \
  --output "$KC_TEST_DIR/registry-architectures.json"
```

预期同时找到 linux/amd64 和 linux/arm64，ELF machine 分别为 62、183，
package manifest 中的二进制摘要与归档内容一致。

### 导入完整包并部署

1. 导入大包，完整保留 amd64、arm64 及原索引内的其他平台。

```bash
kcctl registry import \
  --bundle "$KC_TEST_DIR/materials/kubeclipper-offline-$KC_VERSION.tar.gz" \
  --target "$KC_TEST_LOCAL_REGISTRY" --target-scheme http --arch all
```

预期首次导入所有清单 tag。逐个核对根索引、子 manifest 和引用的 blob 均存在且内容完整。
保存清单条目数、去重 tag 数、平台列表和缺失/损坏数量。

```bash
tar -xOzf "$KC_TEST_DIR/materials/kubeclipper-offline-$KC_VERSION.tar.gz" index.json \
  > "$KC_TEST_DIR/export-index.json"
python3 "$KC_TEST_DIR/scripts/verify-offline-import.py" \
  --index "$KC_TEST_DIR/export-index.json" \
  --target "http://$KC_TEST_LOCAL_REGISTRY" \
  --output "$KC_TEST_DIR/import-proof.json"
```

该步骤完整读取并核对目标端每个 manifest、仓库 blob 链接和所有去重 blob 的大小与
SHA256，断言每个 tag 包含 linux/amd64 与 linux/arm64。全量读取期间保持公网隔离。

2. 原样重复上述导入命令，覆盖 `1.1-08` 的幂等检查。预期 copied 为 0，全部 tag 跳过，
   所有内容一致。在线 `registry import` 的同源重复导入对应 `1.1-07`。
   随后在独立测试命名空间执行下表负例。每项同时核对非零退出码与错误关键字，
   再检查正式测试仓库的内容摘要。

| 负例 | 测试输入 | 预期错误关键字 |
|---|---|---|
| 归档不存在 | `--bundle` 指向本轮目录中不存在的文件 | `read manifest` |
| 架构不支持 | 有效大包配合 `--arch riscv64` | `--arch must be amd64, arm64 or all` |
| gzip 损坏 | 在本轮目录创建非 gzip 文件并传给 `--bundle` | `gzip: invalid header` |
| 小包用于完整导入 | 把 `registry-3.1.1.tar.gz` 传给 `registry import` | `a release bundle is required for import` |
| 目标 tag 冲突 | 在独立测试命名空间写入不同内容的同名 tag，再导入对应有效制品 | `target tag conflict` |

冲突测试使用的有效 ReleaseManifest 必须包含 `bootstrap/kubeclipper`。检查原目标摘要，
完成后删除本轮占位 tag，并验证正式命名空间中的对应 tag 摘要。

部署前再次检查根盘和 imagefs 的可用字节、可用比例及 inode。
本轮大包约 7.1 GiB，展开 OCI 及 Registry 数据各约 7.4 GiB。
按实际 kubelet 配置留出运行镜像解压空间，本轮 `imagefs.available` 阈值为 15%。
在准备机保留原始归档及校验记录，目标端完成完整校验和重复导入后，可清理本轮重复
大包副本并记录路径，以释放安装空间。

3. 在持续隔离公网的条件下通过本地仓库部署 KC、纳管第二台 Agent。

```bash
KCZONE=cn kcctl deploy --deploy-config "$KC_TEST_DIR/deploy.yaml" --assumeyes \
  --package-registry "$KC_TEST_LOCAL_REGISTRY" --package-registry-scheme http
kcctl login --host "https://$KC_TEST_SERVER:8080" --username admin \
  --password "$KC_TEST_ADMIN_PASSWORD"
kcctl join --agent "$KC_TEST_WORKER" --pk-file "$KC_TEST_SSH_KEY" \
  --ip-detect "interface=$KC_TEST_IFACE" --node-ip-detect "interface=$KC_TEST_IFACE"
kcctl get node -o json
kcctl get registry -o json
kcctl doctor
```

预期 join 继承平台的本地 Package Registry，默认运行镜像仓库自动指向同一前缀。
核对 Server、Agent、Registry 运行版本及实际配置。

4. 创建 `delivery-offline` 集群，参数与在线场景相同，执行共同健康验收。
   packagePlan、实际 Pod 镜像和 Registry 访问记录均应来自本地仓库。
   集群健康后再次验证公网请求失败，记录隔离规则计数器。

## 共同健康验收

每个场景都执行以下检查，并保存实际结果。

1. KC API、Console、etcd 读写、Server 和两个 Agent 正常。doctor 的失败或 Unknown
   保留原始输出并排查。独立 etcd 健康检查、真实 KV 写入/读取/删除也要保存结果。
2. Kubernetes 两节点 Ready，版本、角色、运行时和 Calico 版本符合本轮参数。
   CreateCluster Operation 为 Succeeded，系统 Pod Ready，近期事件中没有持续拉取失败。

```bash
kubectl --kubeconfig /etc/kubernetes/admin.conf get nodes -o wide
kubectl --kubeconfig /etc/kubernetes/admin.conf get pods -A -o wide
kubectl --kubeconfig /etc/kubernetes/admin.conf get events -A --sort-by=.lastTimestamp
```

3. 在两个不同节点各运行一个测试 Pod，使用本轮源中已准备的镜像。验证双向 Pod 网络、
   跨节点 HTTP Service、集群 DNS 解析和通过 Service 访问 Kubernetes API。
   保存 Pod 节点、IP、实际请求结果及退出码。

先从节点清单确认 hostname，并填写本场景镜像前缀。以下命令在控制面主机的 Bash 中执行。

```bash
set -euo pipefail
export KUBECONFIG=/etc/kubernetes/admin.conf
export KC_TEST_NODE_0=lixd-dev-3
export KC_TEST_NODE_1=lixd-dev-4
# 按场景填写 GHCR、阿里云或本地 Registry 前缀。
export KC_TEST_IMAGE_REGISTRY=registry.cn-hangzhou.aliyuncs.com/kubeclipper-release
kubectl create namespace delivery-check --dry-run=client -o yaml | kubectl apply -f -
for index in 0 1; do
  node_var="KC_TEST_NODE_$index"
  kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: http-$index
  namespace: delivery-check
  labels:
    app: delivery-http
spec:
  nodeName: ${!node_var}
  tolerations:
    - operator: Exists
  containers:
    - name: http
      image: $KC_TEST_IMAGE_REGISTRY/netshoot:v0.13
      command:
        - sh
        - -c
        - mkdir -p /tmp/www; hostname > /tmp/www/index.html; exec python3 -m http.server 8080 --bind 0.0.0.0 --directory /tmp/www
      readinessProbe:
        httpGet:
          path: /
          port: 8080
        periodSeconds: 2
EOF
done
kubectl -n delivery-check expose pod http-0 --name delivery-http \
  --selector app=delivery-http --port 8080 --target-port 8080 \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n delivery-check wait --for=condition=Ready pod --all --timeout=360s
kubectl -n delivery-check get pod,service -o wide

for index in 0 1; do
  peer=$((1 - index))
  peer_ip=$(kubectl -n delivery-check get pod "http-$peer" -o jsonpath='{.status.podIP}')
  kubectl -n delivery-check exec "http-$index" -- ping -c 3 -W 3 "$peer_ip"
  result=$(kubectl -n delivery-check exec "http-$index" -- \
    curl -fsS --max-time 15 "http://$peer_ip:8080/")
  test "$result" = "http-$peer"
  kubectl -n delivery-check exec "http-$index" -- \
    curl -fsS --max-time 15 http://delivery-http.delivery-check.svc.cluster.local.:8080/
  kubectl -n delivery-check exec "http-$index" -- \
    dig +short kubernetes.default.svc.cluster.local
  kubectl -n delivery-check exec "http-$index" -- sh -c \
    'curl -fsS --max-time 15 --cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt -H "Authorization: Bearer $(cat /var/run/secrets/kubernetes.io/serviceaccount/token)" https://kubernetes.default.svc/healthz'
done
```

双向 ping 均应零丢包，直连 HTTP 返回另一台 Pod 的名称，Service 返回其中一个 Pod 名称，
DNS 返回 Kubernetes Service IP，API 返回 `ok`。保存结果后删除 `delivery-check` namespace。

Service HTTP 使用末尾带 `.` 的绝对域名。现场 Pod 继承了 `openstacklocal` 搜索域，
省略末尾点的长名称在 `ndots:5` 下可能触发该外部搜索域，离线上游 DNS 被阻断时解析失败。
本轮短服务名和绝对域名均已实测成功。Kubernetes 的 DNS 文档说明了 Pod 搜索域和
名称扩展规则。[Kubernetes DNS 文档](https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/)

4. 核对全部运行镜像的 Registry 前缀和运行中二进制，保存经过脱敏的配置投影。
   多架构交付另外检查 amd64/arm64 索引、包内二进制 ELF 架构及完整内容。
   原生 arm64 主机的运行结果独立登记。

如果测试 Pod 因 DiskPressure 被驱逐，先保存 Pod 状态、节点 condition 和事件。
释放本轮冗余物料后，等待压力 condition 恢复，再删除已保存证据的失败 Pod，重新执行
网络验收段。本轮 kubelet 的压力恢复周期为 5 分钟。

```bash
kubectl --kubeconfig /etc/kubernetes/admin.conf wait \
  --for=condition=DiskPressure=false "node/$KC_TEST_NODE_0" --timeout=420s
kubectl --kubeconfig /etc/kubernetes/admin.conf -n delivery-check delete pod http-0 --wait=true
```

发生 API/etcd 超时或组件重启时，保存首次失败、事件、组件日志和 `/proc/pressure/io`。
排查完成后，检查 Pod 重启计数保持稳定，并至少连续完成五轮 API 真实写入、读取和删除。

```bash
set -euo pipefail
export KUBECONFIG=/etc/kubernetes/admin.conf
kubectl get pods -A -o wide
for iteration in 0 1 2 3 4; do
  date -u
  kubectl --request-timeout=20s get --raw=/readyz
  kubectl create configmap delivery-writecheck -n default \
    --from-literal="iteration=$iteration" --dry-run=client -o yaml | kubectl apply -f -
  result=$(kubectl get configmap delivery-writecheck -n default -o jsonpath='{.data.iteration}')
  test "$result" = "$iteration"
  kubectl delete configmap delivery-writecheck -n default --wait=true
  sleep 20
done
kubectl get pods -A -o wide
```

## 结束与失败记录

每个子步骤记录 passed、failed、blocked 或待执行，以及对应证据。重试保留首次失败日志。
修复后填写新提交和重跑范围。场景汇总分别列出下载、Registry、KC、join、建群、健康
和离线隔离检查，使用已有本地二进制时注明来源。

进入下一场景前通过 `kcctl delete cluster` 正常删除集群并等待节点释放，再清理平台。
使用已核对目标的部署配置执行 `kcctl clean --all --force --deploy-config ... --assumeyes`。
Registry 清理前确认当前 CLI Registry 配置指向本轮实例。仅清理本轮创建的服务、数据、
防火墙规则、代理和缓存。核对共享 Registry、NFS 数据和既有 SSH 可用，记录最终保留环境。
