#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

resource_dir=""
registry=""
arch="amd64"
output_dir=""
push_images=false
push_charts=false
registry_node=""
registry_port="5000"
chart_repository_prefix="kubeclipper/charts"
keep_workdir=false
dry_run=false
include_extensions=false
kcctl_bin="${KCCTL_BIN:-}"
helm_oci_publish_bin="${HELM_OCI_PUBLISH_BIN:-}"

usage() {
  cat <<'EOF'
Usage:
  publish-resource-artifacts.sh --resource-dir <dir> --registry <host:port> [flags]

Flags:
  --resource-dir <dir>       Local static resource directory to migrate.
  --registry <host:port>     Target OCI Registry for package images.
  --arch <arch|all>          Architecture to publish. Default: amd64.
  --output-dir <dir>         Directory for generated package tarballs.
  --push-images              Also push images.tar.gz files as runtime images.
  --push-charts              Also push charts.tgz files as Helm OCI charts.
  --registry-node <node>     Registry node for kcctl registry push.
  --registry-port <port>     Registry port for kcctl registry push. Default: 5000.
  --chart-repository-prefix <path>
                              Helm chart repository prefix. Default: kubeclipper/charts.
  --include-extensions       Also publish legacy extension resources.
  --dry-run                  Validate inputs and create package tarballs without pushing.
  --keep-workdir             Keep temporary work directory for debugging.
  -h, --help                 Show this help.

Examples:
  scripts/open-packaging/publish-resource-artifacts.sh \
    --resource-dir /opt/kubeclipper-server/resource \
    --registry 10.0.0.10:5000 \
    --arch amd64

  scripts/open-packaging/publish-resource-artifacts.sh \
    --resource-dir /opt/kubeclipper-server/resource \
    --registry 10.0.0.10:5000 \
    --arch all \
    --push-images \
    --registry-node 10.0.0.10 \
    --registry-port 5000
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

need_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "$1 requires a value"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
  --resource-dir)
    need_value "$@"
    resource_dir="$2"
    shift 2
    ;;
  --registry)
    need_value "$@"
    registry="$2"
    shift 2
    ;;
  --arch)
    need_value "$@"
    arch="$2"
    shift 2
    ;;
  --output-dir)
    need_value "$@"
    output_dir="$2"
    shift 2
    ;;
  --push-images)
    push_images=true
    shift
    ;;
  --push-charts)
    push_charts=true
    shift
    ;;
  --dry-run)
    dry_run=true
    shift
    ;;
  --registry-node)
    need_value "$@"
    registry_node="$2"
    shift 2
    ;;
  --registry-port)
    need_value "$@"
    registry_port="$2"
    shift 2
    ;;
  --chart-repository-prefix)
    need_value "$@"
    chart_repository_prefix="${2#/}"
    chart_repository_prefix="${chart_repository_prefix%/}"
    shift 2
    ;;
  --include-extensions)
    include_extensions=true
    shift
    ;;
  --keep-workdir)
    keep_workdir=true
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    die "unknown argument: $1"
    ;;
  esac
done

[[ -n "$resource_dir" ]] || die "--resource-dir is required"
[[ -d "$resource_dir" ]] || die "resource dir not found: $resource_dir"
[[ -n "$registry" ]] || die "--registry is required"
[[ "$arch" == "amd64" || "$arch" == "arm64" || "$arch" == "all" ]] || die "--arch must be amd64, arm64, or all"

workdir="$(mktemp -d -t kc-resource-oci.XXXXXX)"
if [[ "$keep_workdir" != true ]]; then
  trap 'rm -rf "$workdir"' EXIT
else
  echo "keeping workdir: $workdir"
fi

artifact_dir="${output_dir:-$workdir/artifacts}"
mkdir -p "$artifact_dir"

package_identity() {
  local source_name=$1
  case "$source_name" in
  k8s)
    echo "k8s k8s"
    ;;
  containerd)
    echo "cri $source_name"
    ;;
  calico)
    echo "cni calico"
    ;;
  csi-driver-nfs)
    echo "csi $source_name"
    ;;
  nvidia-dra-driver-gpu | nvidia-gpu-operator)
    echo "app $source_name"
    ;;
  k8s-extension | kc-extension | kubectl-terminal)
    echo "extension $source_name"
    ;;
  *)
    echo "$source_name $source_name"
    ;;
  esac
}

content_profile() {
  local kind=$1
  local name=$2
  case "$kind/$name" in
  k8s/k8s)
    echo "k8s"
    ;;
  cri/*)
    echo "runtime"
    ;;
  binary/*)
    echo "binary"
    ;;
  cni/* | csi/* | app/*)
    echo "addon"
    ;;
  extension/*)
    echo "extension"
    ;;
  *)
    echo ""
    ;;
  esac
}

make_resource_package() {
  local source_name=$1
  local version=$2
  local target_arch=$3
  local source_path=$4
  local pkg="$artifact_dir/${source_name}-${version}-${target_arch}.tar.gz"
  local root="$workdir/pkg-$source_name-$version-$target_arch"

  rm -rf "$root"
  mkdir -p "$root/$source_name/$version/$target_arch"
  find "$source_path" -maxdepth 1 -type f ! -name images.tar.gz ! -name images.txt ! -name charts.tgz ! -name manifest.json -exec cp -f {} "$root/$source_name/$version/$target_arch/" \;
  tar -C "$root" -zcf "$pkg" "$source_name"
  echo "$pkg"
}

has_package_payload_files() {
  local source_path=$1
  find "$source_path" -maxdepth 1 -type f ! -name images.tar.gz ! -name images.txt ! -name charts.tgz ! -name manifest.json | grep -q .
}

publish_resource_package() {
  local source_name=$1
  local version=$2
  local target_arch=$3
  local source_path=$4
  shift 4
  local identity kind name profile pkg

  identity="$(package_identity "$source_name")"
  read -r kind name <<<"$identity"
  profile="$(content_profile "$kind" "$name")"
  pkg=""
  if has_package_payload_files "$source_path"; then
    pkg="$(make_resource_package "$source_name" "$version" "$target_arch" "$source_path")"
  fi

  echo "publishing resource package image: source=$source_name kind=$kind name=$name version=$version arch=$target_arch"
  args=(
    --kind "$kind"
    --name "$name"
    --version "$version"
    --arch "$target_arch"
    --registry "$registry"
  )
  if [[ -n "$profile" ]]; then
    args+=(--profile "$profile")
  fi
  if [[ -n "$pkg" ]]; then
    args=(--package "$pkg" "${args[@]}")
  fi
  if [[ $# -gt 0 ]]; then
    args+=("$@")
  fi
  if [[ "$dry_run" == true ]]; then
    echo "dry-run: would publish ${pkg:-manifest-only package image} ${args[*]}"
    return 0
  fi
  "$ROOT/scripts/publish-oci-package.sh" "${args[@]}"
}

helm_chart_name() {
  local source_name=$1
  case "$source_name" in
  calico)
    echo "tigera-operator"
    ;;
  *)
    echo "$source_name"
    ;;
  esac
}

push_chart() {
  local source_name=$1
  local version=$2
  local chart_archive=$3
  local chart_name repository ref digest descriptor bin

  chart_name="$(helm_chart_name "$source_name")"
  repository="$registry/$chart_repository_prefix/$chart_name"
  ref="oci://$registry/$chart_repository_prefix"
  echo "pushing helm chart: $chart_archive -> $ref" >&2
  if [[ "$dry_run" != true ]]; then
    bin="$helm_oci_publish_bin"
    if [[ -z "$bin" && -x "$ROOT/bin/helm-oci-publish" ]]; then
      bin="$ROOT/bin/helm-oci-publish"
    fi
    if [[ -z "$bin" ]] && command -v helm-oci-publish >/dev/null 2>&1; then
      bin="$(command -v helm-oci-publish)"
    fi
    if [[ -n "$bin" ]]; then
      "$bin" --chart "$chart_archive" --registry "$registry" --repository-prefix "$chart_repository_prefix" --name "$chart_name" >&2
    else
      need_cmd helm
      if ! helm push "$chart_archive" "$ref" >&2; then
        if ! helm push "$chart_archive" "$ref" --plain-http >&2; then
          die "push helm chart failed: $chart_archive"
        fi
      fi
    fi
  else
    echo "dry-run: would publish helm chart $chart_archive to $ref with helm-oci-publish or helm push" >&2
  fi
  digest="$(chart_digest "$chart_archive")"
  descriptor="name=charts,file=$(basename "$chart_archive"),mediaType=application/vnd.cncf.helm.chart.content.v1.tar+gzip,transport=helm-oci,ref=$repository,digest=$digest"
  echo "$descriptor"
}

chart_digest() {
  local chart_archive=$1
  if command -v sha256sum >/dev/null 2>&1; then
    echo "sha256:$(sha256sum "$chart_archive" | awk '{print $1}')"
  else
    echo "sha256:$(shasum -a 256 "$chart_archive" | awk '{print $1}')"
  fi
}

push_runtime_images() {
  local archive=$1
  local args=(registry push --image-archive "$archive" --registry-port "$registry_port")
  local bin="$kcctl_bin"
  if [[ -z "$bin" ]]; then
    if [[ -x "$ROOT/kcctl" ]]; then
      bin="$ROOT/kcctl"
    elif command -v kcctl >/dev/null 2>&1; then
      bin="$(command -v kcctl)"
    else
      die "kcctl is required for --push-images; set KCCTL_BIN or put kcctl in PATH"
    fi
  fi
  if [[ -n "$registry_node" ]]; then
    args+=(--node "$registry_node")
  fi
  echo "pushing runtime image archive: $archive"
  if [[ "$dry_run" == true ]]; then
    echo "dry-run: would run $bin ${args[*]}"
    return 0
  fi
  "$bin" "${args[@]}"
}

matches_arch() {
  local target_arch=$1
  [[ "$arch" == "all" || "$arch" == "$target_arch" ]]
}

published=0
while IFS= read -r -d '' leaf; do
  rel="${leaf#$resource_dir/}"
  IFS="/" read -r source_name version target_arch extra <<<"$rel"
  has_payload=false
  if [[ -z "${source_name:-}" || -z "${version:-}" || -z "${target_arch:-}" || -n "${extra:-}" ]]; then
    continue
  fi
  if ! matches_arch "$target_arch"; then
    continue
  fi
  if [[ "$include_extensions" != true ]]; then
    case "$source_name" in
    k8s-extension|kc-extension|kubectl-terminal)
      echo "skipping legacy extension resource: $source_name/$version/$target_arch"
      continue
      ;;
    esac
  fi
  case "$source_name" in
  conntrack)
    echo "skipping conntrack helper resource: it is packaged inside k8s configs.tar.gz"
    continue
    ;;
  docker|ceph|cinder|csi-driver-wekafs|kc-csi)
    echo "skipping non-open resource: $source_name/$version/$target_arch"
    continue
    ;;
  esac
  if has_package_payload_files "$leaf"; then
    has_payload=true
  fi
  if [[ ! -f "$leaf/configs.tar.gz" && ! -f "$leaf/charts.tgz" && ! -f "$leaf/images.tar.gz" && "$has_payload" != true ]]; then
    continue
  fi

  chart_descriptor=""
  if [[ "$push_charts" == true && -f "$leaf/charts.tgz" ]]; then
    chart_descriptor="$(push_chart "$source_name" "$version" "$leaf/charts.tgz")"
  fi

  if [[ -f "$leaf/configs.tar.gz" || -n "$chart_descriptor" || "$has_payload" == true ]]; then
    if [[ -n "$chart_descriptor" ]]; then
      extra_args=(--external-content "$chart_descriptor")
    else
      extra_args=()
    fi
    publish_resource_package "$source_name" "$version" "$target_arch" "$leaf" "${extra_args[@]}"
  fi
  published=$((published + 1))

  if [[ "$push_images" == true && -f "$leaf/images.tar.gz" ]]; then
    push_runtime_images "$leaf/images.tar.gz"
  fi
done < <(find "$resource_dir" -mindepth 3 -maxdepth 3 -type d -print0)

if [[ "$published" -eq 0 ]]; then
  die "no resource packages found under $resource_dir for arch=$arch"
fi

if [[ "$dry_run" == true ]]; then
  echo "dry-run complete; generated package tarballs are in $artifact_dir"
else
  echo "published $published resource package image(s)"
fi
