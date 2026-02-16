#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
LOCK_FILE="${ROOT_DIR}/dkg-evm-module.lock"
LOCAL_ABI_DIR="${ROOT_DIR}/crates/dkg-blockchain/abi"
TOOLS_EVM_PKG_JSON="${ROOT_DIR}/tools/evm/package.json"

usage() {
  cat <<'EOF'
Usage:
  tools/evm/scripts/sync_abis.sh [--check] [--ref <git-ref>] [--write-lock]

Behavior:
  - Default ref is read from dkg-evm-module.lock (ref=...).
  - Required ABI files are inferred from code references in crates/dkg-blockchain/src/ and tools/local_network/.
  - In sync mode (default), syncs inferred ABI files from upstream into crates/dkg-blockchain/abi/.
  - In --check mode, verifies inferred ABI files match upstream and exits non-zero on mismatch.
  - With --write-lock, updates dkg-evm-module.lock ref=<resolved-sha> and also updates
    tools/evm/package.json to pin dkg-evm-module to that SHA.

Examples:
  tools/evm/scripts/sync_abis.sh --check
  tools/evm/scripts/sync_abis.sh
  tools/evm/scripts/sync_abis.sh --ref main --write-lock
EOF
}

CHECK=0
WRITE_LOCK=0
REF=""
REQUIRED_ABIS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --check) CHECK=1; shift ;;
    --write-lock) WRITE_LOCK=1; shift ;;
    --ref) REF="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ ! -f "${LOCK_FILE}" ]]; then
  echo "Missing ${LOCK_FILE}" >&2
  exit 2
fi

infer_required_abis() {
  local matches
  if command -v rg >/dev/null 2>&1; then
    matches="$(rg -o --no-filename 'abi/[A-Za-z0-9_]+\.json' \
      "${ROOT_DIR}/crates/dkg-blockchain/src" "${ROOT_DIR}/tools/local_network" || true)"
  else
    matches="$(grep -RhoE 'abi/[A-Za-z0-9_]+\.json' \
      "${ROOT_DIR}/crates/dkg-blockchain/src" "${ROOT_DIR}/tools/local_network" || true)"
  fi

  mapfile -t REQUIRED_ABIS < <(
    printf '%s\n' "${matches}" \
      | sed -E 's#^abi/##' \
      | sed '/^$/d' \
      | sort -u
  )

  if [[ "${#REQUIRED_ABIS[@]}" -eq 0 ]]; then
    echo "Could not infer required ABI files from crates/dkg-blockchain/src/ and tools/local_network/" >&2
    exit 2
  fi
}

infer_required_abis

REPO_URL="$(awk -F= '/^repo=/{print $2}' "${LOCK_FILE}" | tail -n 1 | tr -d '\r' || true)"
LOCK_REF="$(awk -F= '/^ref=/{print $2}' "${LOCK_FILE}" | tail -n 1 | tr -d '\r' || true)"

if [[ -z "${REPO_URL}" || -z "${LOCK_REF}" ]]; then
  echo "Invalid ${LOCK_FILE}; expected repo=... and ref=..." >&2
  exit 2
fi

if [[ -z "${REF}" ]]; then
  REF="${LOCK_REF}"
fi

TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "${TMP_DIR}"; }
trap cleanup EXIT

REPO_DIR="${TMP_DIR}/dkg-evm-module"
git init -q "${REPO_DIR}"
git -C "${REPO_DIR}" remote add origin "${REPO_URL}"
git -C "${REPO_DIR}" fetch -q --depth 1 origin "${REF}"
git -C "${REPO_DIR}" checkout -q --detach FETCH_HEAD

RESOLVED_SHA="$(git -C "${REPO_DIR}" rev-parse HEAD)"
UPSTREAM_ABI_DIR="${REPO_DIR}/abi"

if [[ ! -d "${UPSTREAM_ABI_DIR}" ]]; then
  echo "Upstream ref ${REF} (${RESOLVED_SHA}) has no abi/ directory" >&2
  exit 2
fi

if [[ "${CHECK}" -eq 1 ]]; then
  mismatch=0
  for abi_file in "${REQUIRED_ABIS[@]}"; do
    upstream_file="${UPSTREAM_ABI_DIR}/${abi_file}"
    local_file="${LOCAL_ABI_DIR}/${abi_file}"

    if [[ ! -f "${upstream_file}" ]]; then
      echo "Missing upstream ABI: ${abi_file} (${REPO_URL}@${RESOLVED_SHA})" >&2
      mismatch=1
      continue
    fi
    if [[ ! -f "${local_file}" ]]; then
      echo "Missing local ABI: ${abi_file} (${LOCAL_ABI_DIR})" >&2
      mismatch=1
      continue
    fi
    if ! diff -q "${upstream_file}" "${local_file}" >/dev/null; then
      echo "ABI mismatch: ${abi_file}" >&2
      mismatch=1
    fi
  done

  if [[ "${mismatch}" -ne 0 ]]; then
    echo "Run: tools/evm/scripts/sync_abis.sh" >&2
    exit 1
  fi
  exit 0
fi

mkdir -p "${LOCAL_ABI_DIR}"
for abi_file in "${REQUIRED_ABIS[@]}"; do
  upstream_file="${UPSTREAM_ABI_DIR}/${abi_file}"
  if [[ ! -f "${upstream_file}" ]]; then
    echo "Missing upstream ABI: ${abi_file} (${REPO_URL}@${RESOLVED_SHA})" >&2
    exit 2
  fi
  cp "${upstream_file}" "${LOCAL_ABI_DIR}/${abi_file}"
done

if [[ "${WRITE_LOCK}" -eq 1 ]]; then
  # Update lock file to the resolved commit SHA.
  awk -v sha="${RESOLVED_SHA}" '
    BEGIN { updated=0 }
    /^ref=/ { print "ref=" sha; updated=1; next }
    { print }
    END { if (updated==0) print "ref=" sha }
  ' "${LOCK_FILE}" >"${LOCK_FILE}.tmp"
  mv "${LOCK_FILE}.tmp" "${LOCK_FILE}"

  # Keep tools/evm/package.json pinned to the same SHA for local dev.
  python3 - "${TOOLS_EVM_PKG_JSON}" "${RESOLVED_SHA}" <<'PY'
import json, sys
path = sys.argv[1]
sha = sys.argv[2]
with open(path, "r", encoding="utf-8") as f:
    data = json.load(f)
deps = data.get("dependencies") or {}
deps["dkg-evm-module"] = f"git+https://github.com/OriginTrail/dkg-evm-module.git#{sha}"
data["dependencies"] = deps
with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)
    f.write("\n")
PY
fi

echo "Synced crates/dkg-blockchain/abi/ from ${REPO_URL}@${RESOLVED_SHA} (ref=${REF})"
