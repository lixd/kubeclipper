#!/usr/bin/env bash
set -euo pipefail

# Usage: curl ... | bash -s -- --mirror cn
# --mirror cn selects the domestic download site for this invocation only.
# KC_BIN_DIR selects the install directory; ARCH overrides architecture detection.
# The release workflow stamps DEFAULT_VERSION before publishing this script.
DEFAULT_VERSION="v2.0.9"
KC_VERSION="${KC_VERSION:-$DEFAULT_VERSION}"
BIN_DIR="${KC_BIN_DIR:-/usr/local/bin}"

info() { echo "[INFO] $*"; }
fatal() { echo "[ERROR] $*" >&2; exit 1; }

case "$(uname -s)" in
  Linux) OS_TYPE=linux ;;
  Darwin) OS_TYPE=darwin ;;
  *) fatal 'Only Linux and macOS are supported' ;;
esac
case "${ARCH:-$(uname -m)}" in
  amd64|x86_64) ARCH=amd64 ;;
  arm64|aarch64) ARCH=arm64 ;;
  *) fatal 'Only amd64 and arm64 are supported' ;;
esac

DOWNLOAD_URL="https://github.com/kubeclipper/kubeclipper/releases/download"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mirror)
      [[ $# -ge 2 && "$2" == cn ]] || fatal '--mirror requires cn'
      DOWNLOAD_URL="https://release.kubeclipper.io/kc"
      shift 2
      ;;
    -h|--help)
      echo 'Usage: bash get-kubeclipper.sh [--mirror cn]'
      exit 0
      ;;
    *) fatal "Unknown argument: $1" ;;
  esac
done

if command -v curl >/dev/null 2>&1; then
  DOWNLOADER=curl
elif command -v wget >/dev/null 2>&1; then
  DOWNLOADER=wget
else
  fatal 'Please install curl or wget first'
fi
if command -v sha256sum >/dev/null 2>&1; then
  CHECKSUM=sha256sum
elif command -v shasum >/dev/null 2>&1; then
  CHECKSUM=shasum
else
  fatal 'Please install sha256sum or shasum first'
fi

TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/kcctl.XXXXXXXX")
trap 'rm -rf "$TMP_DIR"' EXIT

download() {
  info "Downloading $2"
  case "$DOWNLOADER" in
    curl) curl --fail --location --show-error --connect-timeout 15 -o "$1" "$2" ;;
    wget) wget --timeout=30 -O "$1" "$2" ;;
  esac || fatal "Download failed: $2"
}

ASSET="kcctl-${OS_TYPE}-${ARCH}"
BASE_URL="${DOWNLOAD_URL%/}/${KC_VERSION}"
download "$TMP_DIR/$ASSET" "$BASE_URL/$ASSET"
download "$TMP_DIR/kcctl-checksums.txt" "$BASE_URL/kcctl-checksums.txt"
EXPECTED=$(awk -v asset="$ASSET" '$2 == asset {print $1}' "$TMP_DIR/kcctl-checksums.txt")
[[ "$EXPECTED" =~ ^[a-fA-F0-9]{64}$ ]] || fatal "Missing or invalid SHA256 for $ASSET"
if [[ "$CHECKSUM" == sha256sum ]]; then
  ACTUAL=$(sha256sum "$TMP_DIR/$ASSET")
else
  ACTUAL=$(shasum -a 256 "$TMP_DIR/$ASSET")
fi
[[ "${ACTUAL%% *}" == "$EXPECTED" ]] || fatal "SHA256 mismatch for $ASSET"

info "Installing kcctl $KC_VERSION to $BIN_DIR/kcctl"
if [[ "$(id -u)" == 0 || ( -d "$BIN_DIR" && -w "$BIN_DIR" ) ]]; then
  install -m 0755 "$TMP_DIR/$ASSET" "$BIN_DIR/kcctl"
else
  sudo install -m 0755 "$TMP_DIR/$ASSET" "$BIN_DIR/kcctl"
fi
info "Installed $BIN_DIR/kcctl; run '$BIN_DIR/kcctl version' to check the version"
