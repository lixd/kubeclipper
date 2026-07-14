#!/usr/bin/env bash

set -euo pipefail

die() {
  echo "error: $*" >&2
  exit 1
}

log() {
  echo "==> $*"
}

need_value() {
  [[ $# -ge 2 && -n "$2" ]] || die "$1 requires a value"
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

validate_arch() {
  case "$1" in
  amd64 | arm64 | all) ;;
  *) die "arch must be amd64, arm64, or all" ;;
  esac
}

arch_list() {
  case "$1" in
  all)
    echo "amd64"
    echo "arm64"
    ;;
  *)
    echo "$1"
    ;;
  esac
}

host_arch() {
  case "$(uname -m)" in
  x86_64 | amd64) echo "amd64" ;;
  aarch64 | arm64) echo "arm64" ;;
  *) die "unsupported host architecture: $(uname -m)" ;;
  esac
}

download() {
  local url=$1
  local dst=$2
  local retries="${KC_DOWNLOAD_RETRIES:-3}"
  local connect_timeout="${KC_DOWNLOAD_CONNECT_TIMEOUT:-20}"
  local max_time="${KC_DOWNLOAD_MAX_TIME:-900}"
  local partial="${dst}.part"

  mkdir -p "$(dirname "$dst")"
  if command -v curl >/dev/null 2>&1; then
    local curl_retry_all_errors=()
    if curl --help all 2>/dev/null | grep -q -- '--retry-all-errors'; then
      curl_retry_all_errors=(--retry-all-errors)
    fi
    curl --http1.1 -fL \
      --continue-at - \
      --retry "$retries" \
      --retry-delay 2 \
      --connect-timeout "$connect_timeout" \
      --max-time "$max_time" \
      "${curl_retry_all_errors[@]}" \
      "$url" -o "$partial"
    mv -f "$partial" "$dst"
    return
  fi
  if command -v wget >/dev/null 2>&1; then
    wget --continue --tries "$retries" \
      --connect-timeout "$connect_timeout" \
      --timeout "$max_time" \
      -O "$partial" "$url"
    mv -f "$partial" "$dst"
    return
  fi
  die "curl or wget is required"
}

copy_or_download() {
  local file=$1
  local url=$2
  local dst=$3

  mkdir -p "$(dirname "$dst")"
  if [[ -n "$file" ]]; then
    [[ -f "$file" ]] || die "file not found: $file"
    cp -f "$file" "$dst"
    return
  fi
  [[ -n "$url" ]] || die "missing input for $dst; pass a local file or URL"
  download "$url" "$dst"
}

run_with_optional_timeout() {
  local seconds=$1
  shift
  if command -v timeout >/dev/null 2>&1; then
    timeout "$seconds" "$@"
    return
  fi
  "$@"
}

run_with_retries() {
  local attempts=$1
  local delay=$2
  local attempt
  shift 2

  for ((attempt = 1; attempt <= attempts; attempt++)); do
    if "$@"; then
      return 0
    fi
    if [[ "$attempt" -eq "$attempts" ]]; then
      return 1
    fi
    sleep "$((delay * attempt))"
  done
}

md5_digest() {
  local file=$1
  if command -v md5sum >/dev/null 2>&1; then
    md5sum "$file" | awk '{print $1}'
    return
  fi
  if command -v md5 >/dev/null 2>&1; then
    md5 -q "$file"
    return
  fi
  die "md5sum or md5 is required"
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

generate_manifest() {
  local root=$1
  local output=$2
  local first=true

  mkdir -p "$(dirname "$output")"
  printf '[' > "$output"
  while IFS= read -r file; do
    local rel dir name digest
    rel="${file#"$root"/}"
    dir="$(dirname "$rel")"
    name="$(basename "$rel")"
    digest="$(md5_digest "$file")"
    [[ "$dir" == "." ]] && dir="/"
    if [[ "$first" == true ]]; then
      first=false
    else
      printf ',' >> "$output"
    fi
    printf '{"name":"%s","digest":"%s","path":"%s"}' \
      "$(json_escape "$name")" \
      "$(json_escape "$digest")" \
      "$(json_escape "$dir")" >> "$output"
  done < <(find "$root" -type f ! -name manifest.json -print | sort)
  printf ']' >> "$output"
}

pack_configs() {
  local build_dir=$1
  local out_dir=$2

  mkdir -p "$out_dir"
  tar -C "$build_dir" -zcf "$out_dir/configs.tar.gz" .
}

generate_package_manifest() {
  local package_dir=$1
  generate_manifest "$package_dir" "$package_dir/manifest.json"
}

write_file() {
  local dst=$1
  shift
  mkdir -p "$(dirname "$dst")"
  printf '%s\n' "$@" > "$dst"
}
