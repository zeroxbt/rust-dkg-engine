#!/usr/bin/env bash
set -euo pipefail

REPO_OWNER="zeroxbt"
REPO_NAME="rust-dkg-engine"

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

INSTALL_ROOT="/opt/rust-dkg-engine"
RELEASES_DIR="$INSTALL_ROOT/releases"
CURRENT_LINK="$INSTALL_ROOT/current"
BIN_DIR="$INSTALL_ROOT/bin"
ETC_DIR="/etc/rust-dkg-engine"
CONFIG_PATH="$ETC_DIR/config.toml"

SERVICE_NAME="rust-dkg-engine"

DEFAULT_VERSION="latest"

OVERWRITE_CONFIG="0"
REQUESTED_VERSION="$DEFAULT_VERSION"
USE_EXISTING_CONFIG="0"

log() { printf '%s\n' "$*"; }
warn() { printf '%s\n' "$*" >&2; }
die() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }

toml_escape_basic_string() {
  # Escape for TOML basic strings (double-quoted).
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\r'/\\r}"
  s="${s//$'\t'/\\t}"
  printf '%s' "$s"
}

require_root() {
  [[ "${EUID}" -eq 0 ]] || die "This installer must be run as root (use sudo)."
}

require_systemd() {
  command -v systemctl >/dev/null 2>&1 || die "systemctl not found. This installer requires systemd."
  systemctl --version >/dev/null 2>&1 || die "systemctl is not functional."
}

detect_pkg_manager() {
  if command -v apt-get >/dev/null 2>&1; then
    echo "apt"
  elif command -v dnf >/dev/null 2>&1; then
    echo "dnf"
  elif command -v yum >/dev/null 2>&1; then
    echo "yum"
  elif command -v pacman >/dev/null 2>&1; then
    echo "pacman"
  else
    echo ""
  fi
}

pm_update() {
  local pm="$1"
  case "$pm" in
    apt) DEBIAN_FRONTEND=noninteractive apt-get update -y ;;
    dnf) dnf -y makecache ;;
    yum) yum -y makecache ;;
    pacman) pacman -Sy --noconfirm ;;
    *) die "Unsupported package manager: $pm" ;;
  esac
}

pm_install() {
  local pm="$1"; shift
  case "$pm" in
    apt) DEBIAN_FRONTEND=noninteractive apt-get install -y "$@" ;;
    dnf) dnf -y install "$@" ;;
    yum) yum -y install "$@" ;;
    pacman) pacman -S --noconfirm --needed "$@" ;;
    *) die "Unsupported package manager: $pm" ;;
  esac
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --version)
        [[ $# -ge 2 ]] || die "--version requires an argument"
        REQUESTED_VERSION="$2"
        shift 2
        ;;
      --overwrite-config)
        OVERWRITE_CONFIG="1"
        shift
        ;;
      -h|--help)
        cat <<EOF
Usage: sudo bash tools/installer/install.sh [options]

Options:
  --version latest|<tag>      Release to install (default: latest)
  --overwrite-config          Overwrite /etc/rust-dkg-engine/config.toml if it exists
EOF
        exit 0
        ;;
      *)
        die "Unknown argument: $1"
        ;;
    esac
  done
}

maybe_handle_existing_config() {
  # If a config already exists and the user didn't explicitly pass --overwrite-config,
  # ask what to do to avoid the annoying "rerun with --overwrite-config" loop.
  if [[ -f "$CONFIG_PATH" && "$OVERWRITE_CONFIG" != "1" ]]; then
    warn "Config already exists at ${CONFIG_PATH}"
    local choice
    choice="$(prompt "Overwrite existing config? (y/N)" "n")"
    case "${choice}" in
      y|Y|yes|YES)
        OVERWRITE_CONFIG="1"
        ;;
      *)
        USE_EXISTING_CONFIG="1"
        ;;
    esac
  fi
}

prompt() {
  local msg="$1"
  local def="${2:-}"
  local out=""
  if [[ -n "$def" ]]; then
    read -r -p "$msg [$def]: " out
    if [[ -z "$out" ]]; then out="$def"; fi
  else
    read -r -p "$msg: " out
  fi
  printf '%s' "$out"
}

prompt_secret() {
  local msg="$1"
  local out=""
  read -r -s -p "$msg: " out
  echo >&2
  printf '%s' "$out"
}

prompt_secret_confirm_minlen() {
  local label="$1"
  local min_len="$2"
  local p1="" p2=""
  while true; do
    p1="$(prompt_secret "$label")"
    [[ "${#p1}" -ge "$min_len" ]] || { warn "Invalid input: '${label}' must be at least ${min_len} characters."; continue; }
    p2="$(prompt_secret "Confirm $label")"
    [[ "$p1" == "$p2" ]] || { warn "Invalid input: '${label}' values do not match. Try again."; continue; }
    printf '%s' "$p1"
    return 0
  done
}

prompt_secret_confirm_nonempty() {
  local label="$1"
  local p1="" p2=""
  while true; do
    p1="$(prompt_secret "$label")"
    [[ -n "$p1" ]] || { warn "Invalid input: '${label}' cannot be empty."; continue; }
    p2="$(prompt_secret "Confirm $label")"
    [[ "$p1" == "$p2" ]] || { warn "Invalid input: '${label}' values do not match. Try again."; continue; }
    printf '%s' "$p1"
    return 0
  done
}

detect_arch_tag() {
  local m
  m="$(uname -m)"
  case "$m" in
    x86_64|amd64) echo "x86_64" ;;
    aarch64|arm64) echo "aarch64" ;;
    *) die "Unsupported architecture: $m" ;;
  esac
}

trim() {
  local s="$1"
  # Trim leading/trailing whitespace.
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "$s"
}

strip_quotes() {
  local s
  s="$(trim "$1")"
  if [[ "$s" == \"*\" && "$s" == *\" ]]; then
    s="${s#\"}"
    s="${s%\"}"
  fi
  printf '%s' "$s"
}

toml_get_top_value() {
  # Read a key from the top-level (before the first [section]).
  local file="$1"
  local key="$2"
  awk -v k="$key" '
    /^\s*\[/ { exit }
    $0 ~ "^[[:space:]]*" k "[[:space:]]*=" {
      sub("^[[:space:]]*" k "[[:space:]]*=[[:space:]]*", "", $0)
      print $0
      exit
    }
  ' "$file"
}

toml_get_section_value() {
  # Read a key from a specific section like "managers.repository" (matches [managers.repository]).
  local file="$1"
  local section="$2"
  local key="$3"
  awk -v s="[$section]" -v k="$key" '
    /^\s*\[/ { in_section = ($0 == s) }
    in_section && $0 ~ "^[[:space:]]*" k "[[:space:]]*=" {
      sub("^[[:space:]]*" k "[[:space:]]*=[[:space:]]*", "", $0)
      print $0
      exit
    }
  ' "$file"
}

ensure_user() {
  local user="$1"
  local group="$2"
  if ! getent group "$group" >/dev/null 2>&1; then
    groupadd --system "$group"
  fi
  if ! id -u "$user" >/dev/null 2>&1; then
    useradd --system --gid "$group" --home-dir /nonexistent --shell /usr/sbin/nologin "$user"
  fi
}

ensure_dirs() {
  mkdir -p "$RELEASES_DIR" "$BIN_DIR" "$ETC_DIR"
  chmod 0755 "$INSTALL_ROOT" "$RELEASES_DIR" "$BIN_DIR" "$ETC_DIR"
}

fix_config_permissions_for_service() {
  # Systemd runs the node as rustdkg; allow group-read access to the config.
  if [[ -f "$CONFIG_PATH" ]]; then
    chown root:rustdkg "$ETC_DIR" 2>/dev/null || true
    chmod 0750 "$ETC_DIR" 2>/dev/null || true
    chown root:rustdkg "$CONFIG_PATH" 2>/dev/null || true
    chmod 0640 "$CONFIG_PATH" 2>/dev/null || true
  fi
}

install_base_deps() {
  local pm="$1"
  pm_update "$pm"

  case "$pm" in
    apt) pm_install "$pm" curl jq tar ca-certificates coreutils util-linux ;;
    dnf|yum) pm_install "$pm" curl jq tar ca-certificates coreutils util-linux ;;
    pacman) pm_install "$pm" curl jq tar ca-certificates coreutils util-linux ;;
    *) die "Unsupported package manager: $pm" ;;
  esac
}

detect_db_service_unit() {
  if systemctl list-unit-files --type=service 2>/dev/null | awk '{print $1}' | grep -qx 'mariadb.service'; then
    echo "mariadb"
  elif systemctl list-unit-files --type=service 2>/dev/null | awk '{print $1}' | grep -qx 'mysql.service'; then
    echo "mysql"
  else
    echo ""
  fi
}

install_and_start_mariadb_if_missing() {
  local pm="$1"
  local unit
  unit="$(detect_db_service_unit)"
  if [[ -z "$unit" ]]; then
    log "Installing MariaDB..."
    case "$pm" in
      apt) pm_install "$pm" mariadb-server ;;
      dnf|yum) pm_install "$pm" mariadb-server ;;
      pacman) pm_install "$pm" mariadb ;;
      *) die "Unsupported package manager: $pm" ;;
    esac
    unit="$(detect_db_service_unit)"
  fi

  [[ -n "$unit" ]] || die "Could not detect MariaDB/MySQL systemd unit after install."

  # Arch often requires explicit DB initialization after package install.
  if [[ "$pm" == "pacman" ]] && command -v mariadb-install-db >/dev/null 2>&1; then
    if [[ ! -d /var/lib/mysql/mysql ]]; then
      mariadb-install-db --user=mysql --basedir=/usr --datadir=/var/lib/mysql >/dev/null 2>&1 || true
    fi
  fi

  systemctl enable --now "$unit"
  systemctl is-active --quiet "$unit" || die "Database service '$unit' is not active."
  echo "$unit"
}

mysql_exec_root_socket() {
  local sql="$1"
  mysql -u root -e "$sql"
}

mysql_exec_admin() {
  local admin_user="$1"
  local admin_pass="$2"
  local sql="$3"
  MYSQL_PWD="$admin_pass" mysql -u "$admin_user" -e "$sql"
}

wait_for_mysql_root_socket() {
  local tries=30
  while (( tries > 0 )); do
    if mysql -u root -e "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    tries=$((tries - 1))
    sleep 1
  done
  return 1
}

is_simple_identifier() {
  local v="$1"
  [[ "$v" =~ ^[A-Za-z0-9_]+$ ]]
}

sql_escape_password() {
  # Escape for single-quoted MySQL string literal.
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\'/\'\'}"
  printf '%s' "$s"
}

provision_database() {
  local db_name="$1"
  local app_user="$2"
  local app_pass="$3"

  is_simple_identifier "$db_name" || die "Database name must match [A-Za-z0-9_]+ (got: $db_name)"
  is_simple_identifier "$app_user" || die "Database username must match [A-Za-z0-9_]+ (got: $app_user)"

  local esc_pass
  esc_pass="$(sql_escape_password "$app_pass")"

  # No backticks needed if we restrict db_name to a safe identifier.
  local sql
  sql=$(
    cat <<EOF
CREATE DATABASE IF NOT EXISTS $db_name;
CREATE USER IF NOT EXISTS '$app_user'@'localhost' IDENTIFIED BY '$esc_pass';
ALTER USER '$app_user'@'localhost' IDENTIFIED BY '$esc_pass';
GRANT ALL PRIVILEGES ON $db_name.* TO '$app_user'@'localhost';
FLUSH PRIVILEGES;
EOF
  )

  # Give the service a moment to come up after installation/start.
  wait_for_mysql_root_socket || true

  if mysql_exec_root_socket "$sql" >/dev/null 2>&1; then
    return 0
  fi

  log "Root socket provisioning failed; please provide DB admin credentials to provision DB/user."
  local admin_user admin_pass
  admin_user="$(prompt "DB admin username" "root")"
  admin_pass="$(prompt_secret "DB admin password")"
  mysql_exec_admin "$admin_user" "$admin_pass" "$sql" >/dev/null 2>&1 || die "Failed to provision database with provided admin credentials."
}

install_blazegraph() {
  local pm="$1"
  log "Installing Blazegraph..."

  ensure_user "blazegraph" "blazegraph"

  case "$pm" in
    apt) pm_install "$pm" openjdk-17-jre-headless ;;
    dnf|yum) pm_install "$pm" java-17-openjdk-headless ;;
    pacman) pm_install "$pm" jre-openjdk-headless ;;
    *) die "Unsupported package manager: $pm" ;;
  esac

  mkdir -p "$INSTALL_ROOT/blazegraph"
  curl -fsSL -o "$INSTALL_ROOT/blazegraph/blazegraph.jar" \
    "https://github.com/blazegraph/database/releases/latest/download/blazegraph.jar"
  chown -R root:root "$INSTALL_ROOT/blazegraph"
  chmod 0755 "$INSTALL_ROOT/blazegraph"
  chmod 0644 "$INSTALL_ROOT/blazegraph/blazegraph.jar"

  install -m 0644 "$SCRIPT_DIR/templates/blazegraph.service" /etc/systemd/system/blazegraph.service
  systemctl daemon-reload
  systemctl enable --now blazegraph
  systemctl is-active --quiet blazegraph || die "blazegraph.service is not active"
}

github_api_get_release_json() {
  local version="$1"
  local api="https://api.github.com/repos/${REPO_OWNER}/${REPO_NAME}"
  if [[ "$version" == "latest" ]]; then
    curl -fsSL -H "Accept: application/vnd.github+json" "${api}/releases/latest"
  else
    curl -fsSL -H "Accept: application/vnd.github+json" "${api}/releases/tags/${version}"
  fi
}

download_and_install_binary() {
  local version="$1"
  local arch_tag="$2"

  log "Fetching release metadata from GitHub (${version})..."
  local json tag checksum_url asset_url asset_name
  json="$(github_api_get_release_json "$version")"
  tag="$(jq -r '.tag_name' <<<"$json")"
  [[ -n "$tag" && "$tag" != "null" ]] || die "Failed to resolve release tag_name."

  checksum_url="$(jq -r '.assets[] | select(.name=="SHA256SUMS") | .browser_download_url' <<<"$json" | head -n 1 || true)"
  [[ -n "$checksum_url" ]] || die "Release is missing SHA256SUMS asset."

  asset_url="$(jq -r --arg arch "$arch_tag" '
    .assets[]
    | select(
        (.name | ascii_downcase | contains("linux"))
        and
        (.name | ascii_downcase | contains($arch))
      )
    | select(
        (.name | endswith(".tar.gz"))
        or (.name | endswith(".tgz"))
        or (.name | endswith(".tar.xz"))
        or (.name == "rust-dkg-engine")
      )
    | .browser_download_url
  ' <<<"$json" | head -n 1 || true)"

  asset_name="$(jq -r --arg arch "$arch_tag" '
    .assets[]
    | select(
        (.name | ascii_downcase | contains("linux"))
        and
        (.name | ascii_downcase | contains($arch))
      )
    | select(
        (.name | endswith(".tar.gz"))
        or (.name | endswith(".tgz"))
        or (.name | endswith(".tar.xz"))
        or (.name == "rust-dkg-engine")
      )
    | .name
  ' <<<"$json" | head -n 1 || true)"

  if [[ -z "$asset_url" || -z "$asset_name" ]]; then
    log "Available assets:"
    jq -r '.assets[].name' <<<"$json" >&2
    die "Could not find a Linux ${arch_tag} release asset."
  fi

  local tmp
  tmp="$(mktemp -d)"
  # Avoid `set -u` issues: embed the path into the trap command at definition time.
  # shellcheck disable=SC2064
  trap "rm -rf '$tmp'" EXIT

  log "Downloading ${asset_name}..."
  curl -fsSL -o "$tmp/$asset_name" "$asset_url"
  log "Downloading SHA256SUMS..."
  curl -fsSL -o "$tmp/SHA256SUMS" "$checksum_url"

  # Verify checksum for the selected asset only.
  grep " ${asset_name}\$" "$tmp/SHA256SUMS" >"$tmp/SHA256SUMS.one" || die "SHA256SUMS does not contain an entry for ${asset_name}"
  (cd "$tmp" && sha256sum -c "SHA256SUMS.one") >/dev/null

  local release_dir="$RELEASES_DIR/$tag"
  mkdir -p "$release_dir"

  local extracted="$tmp/extracted"
  mkdir -p "$extracted"

  if [[ "$asset_name" == "rust-dkg-engine" ]]; then
    install -m 0755 "$tmp/$asset_name" "$release_dir/rust-dkg-engine"
  else
    case "$asset_name" in
      *.tar.gz|*.tgz) tar -xzf "$tmp/$asset_name" -C "$extracted" ;;
      *.tar.xz) tar -xJf "$tmp/$asset_name" -C "$extracted" ;;
      *) die "Unsupported asset format: $asset_name" ;;
    esac
    local bin_path
    bin_path="$(find "$extracted" -type f -name rust-dkg-engine | head -n 1 || true)"
    [[ -n "$bin_path" ]] || die "Extracted archive did not contain rust-dkg-engine"
    install -m 0755 "$bin_path" "$release_dir/rust-dkg-engine"
  fi

  ln -sfn "$release_dir" "$CURRENT_LINK"
  log "Installed ${tag} to ${release_dir} and updated ${CURRENT_LINK}"
}

write_config_toml() {
  local environment="$1"
  local p2p_port="$2"
  local external_ip="$3"
  local triple_backend="$4"
  local triple_url="$5"
  local db_host="$6"
  local db_port="$7"
  local db_name="$8"
  local db_user="$9"
  local db_pass="${10}"
  local node_name="${11}"
  local operator_fee="${12}"
  local chain_blocks="${13}"
  local bootstrap_lines="${14}"
  local telemetry_metrics_enabled="${15}"
  local telemetry_metrics_bind_address="${16}"

  umask 077
  mkdir -p "$ETC_DIR"
  # Allow the service user (Group=rustdkg) to read the config without making it world-readable.
  chown root:rustdkg "$ETC_DIR"
  chmod 0750 "$ETC_DIR"

  if [[ -f "$CONFIG_PATH" && "$OVERWRITE_CONFIG" != "1" ]]; then
    die "Config already exists at ${CONFIG_PATH}. Re-run with --overwrite-config to replace it."
  fi

  local tmp
  tmp="$(mktemp)"
  local environment_esc db_host_esc db_name_esc db_user_esc db_pass_esc external_ip_esc triple_url_esc telemetry_metrics_bind_address_esc
  environment_esc="$(toml_escape_basic_string "$environment")"
  db_host_esc="$(toml_escape_basic_string "$db_host")"
  db_name_esc="$(toml_escape_basic_string "$db_name")"
  db_user_esc="$(toml_escape_basic_string "$db_user")"
  db_pass_esc="$(toml_escape_basic_string "$db_pass")"
  external_ip_esc="$(toml_escape_basic_string "$external_ip")"
  triple_url_esc="$(toml_escape_basic_string "$triple_url")"
  telemetry_metrics_bind_address_esc="$(toml_escape_basic_string "$telemetry_metrics_bind_address")"

  cat >"$tmp" <<EOF
environment = "${environment_esc}"
app_data_path = "/var/lib/rust-dkg-engine"

[managers.repository]
host = "${db_host_esc}"
port = ${db_port}
database = "${db_name_esc}"
user = "${db_user_esc}"
password = "${db_pass_esc}"
max_connections = 120
min_connections = 1

[managers.network]
port = ${p2p_port}
idle_connection_timeout_secs = 300
EOF

  if [[ -n "$external_ip" ]]; then
    printf 'external_ip = "%s"\n' "$external_ip_esc" >>"$tmp"
  fi

  if [[ -n "$bootstrap_lines" ]]; then
    cat >>"$tmp" <<EOF
bootstrap = [
$bootstrap_lines
]
EOF
  fi

  cat >>"$tmp" <<EOF

[managers.triple_store]
backend = "${triple_backend}"
url = "${triple_url_esc}"
connect_max_retries = 10
connect_retry_frequency_ms = 10000
max_concurrent_operations = 25

[managers.triple_store.timeouts]
query_ms = 60000
insert_ms = 300000
ask_ms = 10000

EOF

  printf '%s\n' "$chain_blocks" >>"$tmp"

  cat >>"$tmp" <<EOF

[http_api]
enabled = true
port = 8900

[http_api.rate_limiter]
enabled = false
time_window_seconds = 60
max_requests = 10

[http_api.auth]
enabled = true
ip_whitelist = ["127.0.0.1", "::1"]

[telemetry]
# Metrics are counters/gauges/histograms for dashboards and alerts.

[telemetry.metrics]
enabled = ${telemetry_metrics_enabled}
bind_address = "${telemetry_metrics_bind_address_esc}"
EOF

  install -m 0640 "$tmp" "$CONFIG_PATH"
  chown root:rustdkg "$CONFIG_PATH"
  rm -f "$tmp"
  log "Wrote ${CONFIG_PATH}"
}

prompt_environment() {
  local choice
  choice="$(prompt "Environment (mainnet/testnet)" "mainnet")"
  case "$choice" in
    mainnet|testnet) echo "$choice" ;;
    *) die "Invalid environment: $choice" ;;
  esac
}

prompt_triple_store() {
  warn ""
  warn "Triple store selection:"
  warn "  [1] Oxigraph (embedded, default)"
  warn "  [2] Blazegraph (local service)"
  local choice
  choice="$(prompt "Select triple store (1/2 or oxigraph/blazegraph)" "1")"
  case "$choice" in
    1|oxigraph|Oxigraph|OXIGRAPH) echo "oxigraph" ;;
    2|blazegraph|Blazegraph|BLAZEGRAPH) echo "blazegraph" ;;
    *) die "Invalid triple store choice: $choice (use 1=oxigraph or 2=blazegraph)" ;;
  esac
}

prompt_external_ip() {
  local detected=""
  detected="$(curl -fsSL https://api.ipify.org 2>/dev/null || true)"
  if [[ -n "$detected" ]]; then
    prompt "External IP to announce (blank to skip)" "$detected"
  else
    prompt "External IP to announce (blank to skip)" ""
  fi
}

prompt_bootstrap_list() {
  local environment="${1:-}"
  local bootstrap_csv
  bootstrap_csv="$(prompt "Bootstrap multiaddrs (comma-separated, blank to use defaults)" "")"
  if [[ -z "$bootstrap_csv" ]]; then
    # Defaults taken from src/config/defaults.rs (mainnet). Testnet defaults are empty.
    if [[ "$environment" == "mainnet" ]]; then
      printf '%s\n' \
        '    "/ip4/157.230.96.194/tcp/9000/p2p/QmZFcns6eGUosD96beHyevKu1jGJ1bA56Reg2f1J4q59Jt",' \
        '    "/ip4/18.132.135.102/tcp/9000/p2p/QmemqyXyvrTAm7PwrcTcFiEEFx69efdR92GSZ1oQprbdja",'
      return 0
    fi
    echo ""
    return 0
  fi
  local out=""
  IFS=',' read -r -a arr <<<"$bootstrap_csv"
  for v in "${arr[@]}"; do
    v="$(echo "$v" | xargs)"
    [[ -z "$v" ]] && continue
    v="$(toml_escape_basic_string "$v")"
    out+="    \"${v}\",\n"
  done
  # strip last newline
  printf "%b" "$out"
}

prompt_nonempty() {
  local label="$1"
  local def="${2:-}"
  local value=""

  while true; do
    value="$(prompt "$label" "$def")"
    value="$(echo "$value" | xargs)"
    if [[ -n "$value" ]]; then
      printf '%s' "$value"
      return 0
    fi
    warn "Invalid input: value cannot be empty."
  done
}

prompt_telemetry_config() {
  local metrics_choice telemetry_metrics_enabled telemetry_metrics_bind_address

  metrics_choice="$(prompt "Enable telemetry metrics (/metrics endpoint for dashboards/alerts)? (y/N)" "n")"
  case "$metrics_choice" in
    y|Y|yes|YES)
      telemetry_metrics_enabled="true"
      telemetry_metrics_bind_address="$(prompt_nonempty "Metrics bind address" "127.0.0.1:9464")"
      ;;
    *)
      telemetry_metrics_enabled="false"
      telemetry_metrics_bind_address="127.0.0.1:9464"
      ;;
  esac

  printf '%s|%s\n' \
    "$telemetry_metrics_enabled" \
    "$telemetry_metrics_bind_address"
}

prompt_rpc_endpoints_csv() {
  local label="$1"
  local def_csv="$2"
  local v
  v="$(prompt "$label (comma-separated)" "$def_csv")"
  v="$(echo "$v" | xargs)"
  [[ -n "$v" ]] || die "RPC endpoints cannot be empty."
  echo "$v"
}

prompt_u16() {
  local label="$1"
  local def="$2"
  local v=""
  while true; do
    v="$(prompt "$label" "$def")"
    [[ "$v" =~ ^[0-9]+$ ]] || { warn "Invalid input: enter a number."; continue; }
    (( v >= 1 && v <= 65535 )) || { warn "Invalid input: enter a value in range 1-65535."; continue; }
    echo "$v"
    return 0
  done
}

prompt_operator_fee() {
  local v=""
  while true; do
    v="$(prompt "Operator fee (0-100)" "0")"
    [[ "$v" =~ ^[0-9]+$ ]] || { warn "Invalid input: enter a number."; continue; }
    (( v >= 0 && v <= 100 )) || { warn "Invalid input: enter a value in range 0-100."; continue; }
    echo "$v"
    return 0
  done
}

toml_array_lines_from_csv() {
  local csv="$1"
  local out=""
  IFS=',' read -r -a arr <<<"$csv"
  for v in "${arr[@]}"; do
    v="$(echo "$v" | xargs)"
    [[ -z "$v" ]] && continue
    v="$(toml_escape_basic_string "$v")"
    out+="    \"${v}\",\n"
  done
  printf "%b" "$out"
}

build_chain_blocks() {
  local environment="$1"
  local node_name="$2"
  local operator_fee="$3"

  local enable_neuroweb enable_gnosis enable_base
  enable_neuroweb="$(prompt "Enable NeuroWeb chain? (y/n)" "y")"
  enable_gnosis="$(prompt "Enable Gnosis chain? (y/n)" "n")"
  enable_base="$(prompt "Enable Base chain? (y/n)" "n")"

  local blocks=""
  local enabled_count=0

  if [[ "$enable_neuroweb" == "y" || "$enable_neuroweb" == "Y" ]]; then
    enabled_count=$((enabled_count + 1))
    local bc_id hub_addr default_rpc
    if [[ "$environment" == "mainnet" ]]; then
      bc_id="otp:2043"
      hub_addr="0x0957e25BD33034948abc28204ddA54b6E1142D6F"
      default_rpc="https://astrosat-parachain-rpc.origin-trail.network,https://astrosat.origintrail.network/,https://astrosat-2.origintrail.network/"
    else
      bc_id="otp:20430"
      hub_addr="0xe233b5b78853a62b1e11ebe88bf083e25b0a57a6"
      default_rpc="https://lofar-testnet.origin-trail.network,https://lofar-testnet.origintrail.network"
    fi
    local rpc_csv op_priv mgmt_addr
    rpc_csv="$(prompt_rpc_endpoints_csv "NeuroWeb RPC endpoints" "$default_rpc")"
    op_priv="$(prompt_secret_confirm_minlen "NeuroWeb operational wallet private key" 8)"
    mgmt_addr="$(prompt "NeuroWeb management wallet address (0x...)" "")"
    [[ -n "$mgmt_addr" ]] || die "Management wallet address is required."
    local op_priv_esc mgmt_addr_esc node_name_esc
    op_priv_esc="$(toml_escape_basic_string "$op_priv")"
    mgmt_addr_esc="$(toml_escape_basic_string "$mgmt_addr")"
    node_name_esc="$(toml_escape_basic_string "$node_name")"
    local rpc_lines
    rpc_lines="$(toml_array_lines_from_csv "$rpc_csv")"
    blocks+=$(
      cat <<EOF
[[managers.blockchain]]
[managers.blockchain.NeuroWeb]
enabled = true
blockchain_id = "${bc_id}"
hub_contract_address = "${hub_addr}"
rpc_endpoints = [
$rpc_lines
]
operator_fee = ${operator_fee}
evm_operational_wallet_private_key = "${op_priv_esc}"
evm_management_wallet_address = "${mgmt_addr_esc}"
node_name = "${node_name_esc}"
tx_confirmations = 1
tx_receipt_timeout_ms = 300000

EOF
    )
    # Command substitutions strip trailing newlines; ensure block separation explicitly.
    blocks+=$'\n'
  fi

  if [[ "$enable_gnosis" == "y" || "$enable_gnosis" == "Y" ]]; then
    enabled_count=$((enabled_count + 1))
    local bc_id hub_addr default_rpc
    if [[ "$environment" == "mainnet" ]]; then
      bc_id="gnosis:100"
      hub_addr="0x882D0BF07F956b1b94BBfe9E77F47c6fc7D4EC8f"
      default_rpc=""
    else
      bc_id="gnosis:10200"
      hub_addr="0x2c08AC4B630c009F709521e56Ac385A6af70650f"
      default_rpc="https://rpc.chiadochain.net"
    fi
    local rpc_csv op_priv mgmt_addr
    rpc_csv="$(prompt_rpc_endpoints_csv "Gnosis RPC endpoints" "$default_rpc")"
    op_priv="$(prompt_secret_confirm_minlen "Gnosis operational wallet private key" 8)"
    mgmt_addr="$(prompt "Gnosis management wallet address (0x...)" "")"
    [[ -n "$mgmt_addr" ]] || die "Management wallet address is required."
    local op_priv_esc mgmt_addr_esc node_name_esc
    op_priv_esc="$(toml_escape_basic_string "$op_priv")"
    mgmt_addr_esc="$(toml_escape_basic_string "$mgmt_addr")"
    node_name_esc="$(toml_escape_basic_string "$node_name")"
    local rpc_lines
    rpc_lines="$(toml_array_lines_from_csv "$rpc_csv")"
    blocks+=$(
      cat <<EOF
[[managers.blockchain]]
[managers.blockchain.Gnosis]
enabled = true
blockchain_id = "${bc_id}"
hub_contract_address = "${hub_addr}"
rpc_endpoints = [
$rpc_lines
]
operator_fee = ${operator_fee}
evm_operational_wallet_private_key = "${op_priv_esc}"
evm_management_wallet_address = "${mgmt_addr_esc}"
node_name = "${node_name_esc}"
tx_confirmations = 1
tx_receipt_timeout_ms = 300000

EOF
    )
    blocks+=$'\n'
  fi

  if [[ "$enable_base" == "y" || "$enable_base" == "Y" ]]; then
    enabled_count=$((enabled_count + 1))
    local bc_id hub_addr default_rpc
    if [[ "$environment" == "mainnet" ]]; then
      bc_id="base:8453"
      hub_addr="0x99Aa571fD5e681c2D27ee08A7b7989DB02541d13"
      default_rpc=""
    else
      bc_id="base:84532"
      hub_addr="0xf21CE8f8b01548D97DCFb36869f1ccB0814a4e05"
      default_rpc="https://sepolia.base.org"
    fi
    local rpc_csv op_priv mgmt_addr
    rpc_csv="$(prompt_rpc_endpoints_csv "Base RPC endpoints" "$default_rpc")"
    op_priv="$(prompt_secret_confirm_minlen "Base operational wallet private key" 8)"
    mgmt_addr="$(prompt "Base management wallet address (0x...)" "")"
    [[ -n "$mgmt_addr" ]] || die "Management wallet address is required."
    local op_priv_esc mgmt_addr_esc node_name_esc
    op_priv_esc="$(toml_escape_basic_string "$op_priv")"
    mgmt_addr_esc="$(toml_escape_basic_string "$mgmt_addr")"
    node_name_esc="$(toml_escape_basic_string "$node_name")"
    local rpc_lines
    rpc_lines="$(toml_array_lines_from_csv "$rpc_csv")"
    blocks+=$(
      cat <<EOF
[[managers.blockchain]]
[managers.blockchain.Base]
enabled = true
blockchain_id = "${bc_id}"
hub_contract_address = "${hub_addr}"
rpc_endpoints = [
$rpc_lines
]
operator_fee = ${operator_fee}
evm_operational_wallet_private_key = "${op_priv_esc}"
evm_management_wallet_address = "${mgmt_addr_esc}"
node_name = "${node_name_esc}"
tx_confirmations = 1
tx_receipt_timeout_ms = 300000

EOF
    )
    blocks+=$'\n'
  fi

  [[ "$enabled_count" -ge 1 ]] || die "You must enable at least one blockchain."
  printf '%s\n' "$blocks"
}

install_rust_service() {
  local wants_units="$1"
  local after_units="$2"

  local tmp
  tmp="$(mktemp)"
  sed \
    -e "s/@WANTS_UNITS@/${wants_units}/g" \
    -e "s/@AFTER_UNITS@/${after_units}/g" \
    "$SCRIPT_DIR/templates/rust-dkg-engine.service" >"$tmp"

  install -m 0644 "$tmp" "/etc/systemd/system/${SERVICE_NAME}.service"
  rm -f "$tmp"

  systemctl daemon-reload
  systemctl enable --now "${SERVICE_NAME}.service"
  systemctl is-active --quiet "${SERVICE_NAME}.service" || die "${SERVICE_NAME}.service is not active."
}

install_auto_updater() {
  # Script + units shipped inside installer bundle under templates/.
  install -m 0755 "$SCRIPT_DIR/templates/rust-dkg-engine-update" "${BIN_DIR}/rust-dkg-engine-update"
  chown root:root "${BIN_DIR}/rust-dkg-engine-update"

  install -m 0644 "$SCRIPT_DIR/templates/rust-dkg-engine-update.service" /etc/systemd/system/rust-dkg-engine-update.service
  install -m 0644 "$SCRIPT_DIR/templates/rust-dkg-engine-update.timer" /etc/systemd/system/rust-dkg-engine-update.timer

  systemctl daemon-reload
  systemctl enable --now rust-dkg-engine-update.timer
  systemctl is-enabled --quiet rust-dkg-engine-update.timer || die "Failed to enable rust-dkg-engine-update.timer"
}

maybe_enable_auto_updates_prompt() {
  local choice
  choice="$(prompt "Enable auto-updates via systemd timer? (y/N)" "n")"
  case "$choice" in
    y|Y|yes|YES)
      install_auto_updater
      log "Auto-updates enabled: systemctl status rust-dkg-engine-update.timer"
      ;;
    *)
      log "Auto-updates not enabled. You can enable later with:"
      log "  systemctl enable --now rust-dkg-engine-update.timer"
      ;;
  esac
}

main() {
  parse_args "$@"
  require_root
  require_systemd
  maybe_handle_existing_config

  local pm
  pm="$(detect_pkg_manager)"
  [[ -n "$pm" ]] || die "No supported package manager found (apt/dnf/yum/pacman)."

  install_base_deps "$pm"

  ensure_user "rustdkg" "rustdkg"
  ensure_dirs

  if [[ "$USE_EXISTING_CONFIG" == "1" ]]; then
    warn "Using existing config (no prompts)."
    fix_config_permissions_for_service

    local environment triple_backend db_host
    environment="$(strip_quotes "$(toml_get_top_value "$CONFIG_PATH" "environment" || true)")"
    triple_backend="$(strip_quotes "$(toml_get_section_value "$CONFIG_PATH" "managers.triple_store" "backend" || true)")"
    db_host="$(strip_quotes "$(toml_get_section_value "$CONFIG_PATH" "managers.repository" "host" || true)")"

    triple_backend="$(echo "${triple_backend:-}" | tr '[:upper:]' '[:lower:]')"
    db_host="$(echo "${db_host:-}" | tr '[:upper:]' '[:lower:]')"

    # Optional dependency installs based on config.
    local db_unit=""
    if [[ "$db_host" == "localhost" || "$db_host" == "127.0.0.1" || "$db_host" == "::1" || -z "$db_host" ]]; then
      db_unit="$(install_and_start_mariadb_if_missing "$pm")"
    fi

    if [[ "$triple_backend" == "blazegraph" ]]; then
      install_blazegraph "$pm"
    fi

    # Download/install binary + (re)install service
    local arch_tag
    arch_tag="$(detect_arch_tag)"
    download_and_install_binary "$REQUESTED_VERSION" "$arch_tag"

    local wants_units="" after_units=""
    if [[ -n "$db_unit" ]]; then
      wants_units="${db_unit}.service"
      after_units="${db_unit}.service"
    fi
    if [[ "$triple_backend" == "blazegraph" ]]; then
      wants_units="${wants_units} blazegraph.service"
      after_units="${after_units} blazegraph.service"
    fi
    wants_units="$(echo "$wants_units" | xargs || true)"
    after_units="$(echo "$after_units" | xargs || true)"

    install_rust_service "$wants_units" "$after_units"
    maybe_enable_auto_updates_prompt

    log ""
    log "Install complete."
    if [[ -n "$environment" ]]; then
      log "Environment: ${environment}"
    fi
    log "Service: systemctl status ${SERVICE_NAME}"
    log "Logs:    journalctl -u ${SERVICE_NAME} -f"
    if [[ "$triple_backend" == "blazegraph" ]]; then
      log "Blazegraph: systemctl status blazegraph"
    fi
    log "Config:   ${CONFIG_PATH}"
    log "Data dir: /var/lib/rust-dkg-engine"
    log "Binary:   ${CURRENT_LINK}/rust-dkg-engine"
    return 0
  fi

  # 1) Prompt environment + basic network
  local environment p2p_port external_ip bootstrap_lines
  environment="$(prompt_environment)"
  p2p_port="$(prompt_u16 "P2P port" "9000")"
  external_ip="$(prompt_external_ip)"
  bootstrap_lines="$(prompt_bootstrap_list "$environment")"

  # 2) Triple store selection
  local triple_backend triple_url
  triple_backend="$(prompt_triple_store)"
  if [[ "$triple_backend" == "blazegraph" ]]; then
    install_blazegraph "$pm"
    triple_url="http://127.0.0.1:9999"
  else
    triple_url=""
  fi

  # 3) DB setup (MariaDB)
  local db_unit db_name db_user db_pass db_host db_port
  db_unit="$(install_and_start_mariadb_if_missing "$pm")"
  db_host="localhost"
  db_port="3306"
  if [[ "$environment" == "mainnet" ]]; then
    db_name="$(prompt "Database name" "rust_dkg_engine_mainnet")"
  else
    db_name="$(prompt "Database name" "rust_dkg_engine_testnet")"
  fi
  db_user="$(prompt "Database username" "rustdkg_db")"
  db_pass="$(prompt_secret_confirm_nonempty "Database user password")"
  provision_database "$db_name" "$db_user" "$db_pass"

  # 4) Download/install binary
  local arch_tag
  arch_tag="$(detect_arch_tag)"
  download_and_install_binary "$REQUESTED_VERSION" "$arch_tag"

  # 5) Blockchain prompts + config generation
  local node_name operator_fee chain_blocks
  node_name="$(prompt "Node name" "")"
  [[ -n "$node_name" ]] || die "Node name is required."
  operator_fee="$(prompt_operator_fee)"
  chain_blocks="$(build_chain_blocks "$environment" "$node_name" "$operator_fee")"

  # 5b) Telemetry setup (node export only; does not install local observability services)
  local telemetry_metrics_enabled telemetry_metrics_bind_address telemetry_tuple
  telemetry_tuple="$(prompt_telemetry_config)"
  IFS='|' read -r telemetry_metrics_enabled telemetry_metrics_bind_address <<<"$telemetry_tuple"

  write_config_toml \
    "$environment" \
    "$p2p_port" \
    "$external_ip" \
    "$triple_backend" \
    "$triple_url" \
    "$db_host" \
    "$db_port" \
    "$db_name" \
    "$db_user" \
    "$db_pass" \
    "$node_name" \
    "$operator_fee" \
    "$chain_blocks" \
    "$bootstrap_lines" \
    "$telemetry_metrics_enabled" \
    "$telemetry_metrics_bind_address"

  # 6) Install service
  local wants_units after_units
  wants_units="${db_unit}.service"
  after_units="${db_unit}.service"
  if [[ "$triple_backend" == "blazegraph" ]]; then
    wants_units="${wants_units} blazegraph.service"
    after_units="${after_units} blazegraph.service"
  fi
  install_rust_service "$wants_units" "$after_units"
  maybe_enable_auto_updates_prompt

  log ""
  log "Install complete."
  log "Service: systemctl status ${SERVICE_NAME}"
  log "Start:   systemctl start ${SERVICE_NAME}"
  log "Stop:    systemctl stop ${SERVICE_NAME}"
  log "Restart: systemctl restart ${SERVICE_NAME}"
  log "Logs:    journalctl -u ${SERVICE_NAME} -f"
  if [[ "$triple_backend" == "blazegraph" ]]; then
    log "Blazegraph: systemctl status blazegraph"
  fi
  log "Config:   ${CONFIG_PATH}"
  log "Data dir: /var/lib/rust-dkg-engine"
  log "Binary:   ${CURRENT_LINK}/rust-dkg-engine"
}

main "$@"
