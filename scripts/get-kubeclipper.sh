#!/usr/bin/env bash
set -euo pipefail

# Usage: export KCZONE=cn; curl ... | bash
# KCZONE selects the download zone; KC_DOWNLOAD_URL overrides its release base URL.
# KC_BIN_DIR selects the install directory; ARCH overrides architecture detection.
# The release workflow stamps DEFAULT_VERSION before publishing this script.
DEFAULT_VERSION="v2.0.9"
DEFAULT_DOWNLOAD_URL="https://github.com/kubeclipper/kubeclipper/releases/download"
DEFAULT_CN_DOWNLOAD_URL="https://kubeclipper.oss-ap-southeast-1.aliyuncs.com/kc"
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

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      echo 'Usage: [KCZONE=cn] [KC_DOWNLOAD_URL=<release-base-url>] bash get-kubeclipper.sh'
      exit 0
      ;;
    *) fatal "Unknown argument: $1" ;;
  esac
done

case "${KCZONE:-global}" in
  global) DOWNLOAD_URL="$DEFAULT_DOWNLOAD_URL" ;;
  cn) DOWNLOAD_URL="$DEFAULT_CN_DOWNLOAD_URL" ;;
  *) fatal 'KCZONE must be global or cn' ;;
esac
DOWNLOAD_URL="${KC_DOWNLOAD_URL:-$DOWNLOAD_URL}"
[[ -n "$DOWNLOAD_URL" ]] || fatal 'Download source is not configured; set KC_DOWNLOAD_URL'
case "$DOWNLOAD_URL" in
  https://*|http://*) ;;
  *) fatal 'KC_DOWNLOAD_URL must be an HTTP or HTTPS release base URL' ;;
esac

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
