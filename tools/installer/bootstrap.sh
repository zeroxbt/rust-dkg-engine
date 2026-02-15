#!/usr/bin/env bash
set -euo pipefail

# Minimal bootstrap:
# - downloads the version-matched installer bundle from GitHub Releases
# - verifies SHA256
# - executes installer, forwarding args
#
# Intended usage:
#   curl -fsSL https://github.com/zeroxbt/rust-dkg-engine/releases/latest/download/bootstrap.sh | sudo bash -s -- --version latest

REPO_OWNER="zeroxbt"
REPO_NAME="rust-dkg-engine"

log() { printf '%s\n' "$*"; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

require_root() {
  [[ "${EUID}" -eq 0 ]] || die "This installer must be run as root (use sudo)."
}

extract_version_arg() {
  local v="latest"
  local args=("$@")
  local i=0
  while (( i < ${#args[@]} )); do
    if [[ "${args[$i]}" == "--version" ]]; then
      (( i + 1 < ${#args[@]} )) || die "--version requires an argument"
      v="${args[$((i + 1))]}"
      break
    fi
    i=$((i + 1))
  done
  printf '%s' "$v"
}

main() {
  require_root

  command -v curl >/dev/null 2>&1 || die "curl not found"
  command -v tar >/dev/null 2>&1 || die "tar not found"
  command -v sha256sum >/dev/null 2>&1 || die "sha256sum not found"

  local version base_url tmp
  version="$(extract_version_arg "$@")"

  if [[ "$version" == "latest" ]]; then
    base_url="https://github.com/${REPO_OWNER}/${REPO_NAME}/releases/latest/download"
  else
    base_url="https://github.com/${REPO_OWNER}/${REPO_NAME}/releases/download/${version}"
  fi

  tmp="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap "rm -rf '$tmp'" EXIT

  log "Downloading installer bundle (${version})..."
  curl -fsSL -o "$tmp/installer.tar.gz" "${base_url}/installer.tar.gz"
  curl -fsSL -o "$tmp/SHA256SUMS" "${base_url}/SHA256SUMS"

  grep " installer.tar.gz\$" "$tmp/SHA256SUMS" >"$tmp/SHA256SUMS.one" || die "SHA256SUMS missing installer.tar.gz entry"
  (cd "$tmp" && sha256sum -c "SHA256SUMS.one") >/dev/null

  tar -xzf "$tmp/installer.tar.gz" -C "$tmp"

  [[ -f "$tmp/installer/install.sh" ]] || die "installer/install.sh missing from installer.tar.gz"
  exec bash "$tmp/installer/install.sh" "$@"
}

main "$@"

