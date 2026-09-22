#!/usr/bin/env bash
set -euo pipefail

if ! command -v jq >/dev/null 2>&1;
then
  echo "jq is required for this script."
  exit 1
fi

APPD_BIN="/home/mikers/dev/snissn/celestia-app/build/celestia-appd"
if ! command -v /home/mikers/dev/snissn/celestia-app/build/celestia-appd >/dev/null 2>&1;
then
  echo "celestia-appd not found; run 'make install-standalone' or set CELESTIA_APPD_BIN."
  exit 1
fi

CHAIN_ID="celestia"
RPC1="https://celestia-mainnet-rpc.itrocket.net"
RPC2="https://rpc.celestia.mainnet.dteam.tech"
CURL_OPTS="--max-time 10 --connect-timeout 5 --retry 3 --retry-delay 2"
LOCAL_RPC="http://127.0.0.1:36657"
P2P_LADDR="tcp://0.0.0.0:36656"
RPC_LADDR="tcp://127.0.0.1:36657"
PPROF_LADDR="localhost:6062"
DB_BACKEND="${DB_BACKEND:-treedb}"
APP_DB_BACKEND="${APP_DB_BACKEND:-${DB_BACKEND}}"

if [ "${DB_BACKEND}" = "treedb" ] || [ "${APP_DB_BACKEND}" = "treedb" ]; then
  : "${TREEDB_BENCH_DISABLE_BG:=1}"
  : "${TREEDB_CLOSE_CHECKPOINT:=1}"
  : "${TREEDB_CLOSE_VACUUM_INDEX_ONLINE:=1}"
  : "${TREEDB_CLOSE_SCOPE_CONTAINS:=application.db}"
fi

TS="$(date +%Y%m%d%H%M%S)"
HOME_DIR="${HOME}/.celestia-app-mainnet-${DB_BACKEND}-${TS}"
LOG_DIR="${HOME_DIR}/sync"
NODE_LOG="${LOG_DIR}/node.log"
TIME_LOG="${LOG_DIR}/sync-time.log"

mkdir -p "${LOG_DIR}"

fallback_home=""
for dir in "${HOME}"/.celestia-app-mainnet-*; do
  if [ -f "${dir}/config/genesis.json" ]; then
    fallback_home="${dir}"
    break
  fi
done

fetch_or_copy() {
  local url="$1"
  local dest="$2"
  local fallback="$3"
  if ! curl -fsSL ${CURL_OPTS} "${url}" -o "${dest}"; then
    if [ -n "${fallback}" ] && [ -f "${fallback}" ]; then
      cp "${fallback}" "${dest}"
      return 0
    fi
    return 1
  fi
}

echo "Using home: ${HOME_DIR}"
echo "Logs: ${LOG_DIR}"

/home/mikers/dev/snissn/celestia-app/build/celestia-appd init treedb-mainnet --chain-id "${CHAIN_ID}" --home "${HOME_DIR}" >/dev/null

fetch_or_copy \
  https://raw.githubusercontent.com/celestiaorg/networks/master/celestia/genesis.json \
  "${HOME_DIR}/config/genesis.json" \
  "${fallback_home}/config/genesis.json"
fetch_or_copy \
  https://raw.githubusercontent.com/celestiaorg/networks/master/celestia/peers.txt \
  "${HOME_DIR}/config/peers.txt" \
  "${fallback_home}/config/peers.txt"
fetch_or_copy \
  https://raw.githubusercontent.com/celestiaorg/networks/master/celestia/seeds.txt \
  "${HOME_DIR}/config/seeds.txt" \
  "${fallback_home}/config/seeds.txt"

SEEDS="$(grep -Ev '^\s*$' "${HOME_DIR}/config/seeds.txt" | paste -sd, -)"
PEERS="$(grep -Ev '^\s*$' "${HOME_DIR}/config/peers.txt" | paste -sd, -)"

NET_INFO_JSON="$(curl -fsSL ${CURL_OPTS} "${RPC1}/net_info" 2>/dev/null || curl -fsSL ${CURL_OPTS} "${RPC2}/net_info" 2>/dev/null || true)"
if [ -n "${NET_INFO_JSON}" ]; then
  NET_INFO_PEERS="$(echo "${NET_INFO_JSON}" | jq -r '.result.peers[] | .node_info.id + "@" + .remote_ip + ":" + (.node_info.listen_addr | split(":") | last)' | head -n 20 | paste -sd, -)"
  if [ -n "${NET_INFO_PEERS}" ]; then
    PEERS="${NET_INFO_PEERS}"
  fi
fi

export HOME_DIR SEEDS PEERS P2P_LADDR RPC_LADDR PPROF_LADDR DB_BACKEND
python3 - <<'PY'
import os
import re
from pathlib import Path

cfg_path = Path(os.environ["HOME_DIR"]) / "config" / "config.toml"
data = cfg_path.read_text()
data, pprof_count = re.subn(
    r"(?m)^pprof_laddr\s*=.*$",
    f"pprof_laddr = \"{os.environ['PPROF_LADDR']}\"",
    data,
)
data, seeds_count = re.subn(
    r"(?m)^seeds\s*=.*$",
    f"seeds = \"{os.environ['SEEDS']}\"",
    data,
)
data, peers_count = re.subn(
    r"(?m)^persistent_peers\s*=.*$",
    f"persistent_peers = \"{os.environ['PEERS']}\"",
    data,
)
data, rpc_count = re.subn(
    r"(?m)^laddr\s*=\s*\"tcp://127.0.0.1:26657\"$",
    f"laddr = \"{os.environ['RPC_LADDR']}\"",
    data,
)
data, p2p_count = re.subn(
    r"(?m)^laddr\s*=\s*\"tcp://0.0.0.0:26656\"$",
    f"laddr = \"{os.environ['P2P_LADDR']}\"",
    data,
)
data, db_count = re.subn(
    r"(?m)^db_backend\s*=.*$",
    f"db_backend = \"{os.environ['DB_BACKEND']}\"",
    data,
)
if (
    pprof_count == 0
    or seeds_count == 0
    or peers_count == 0
    or rpc_count == 0
    or p2p_count == 0
    or db_count == 0
):
    raise SystemExit("Failed to update config.toml (ports/peers/seeds/pprof).")
cfg_path.write_text(data)
PY

export HOME_DIR APP_DB_BACKEND
python3 - <<'PY'
import os
import re
from pathlib import Path

app_path = Path(os.environ["HOME_DIR"]) / "config" / "app.toml"
data = app_path.read_text()
data, count = re.subn(
    r"(?m)^app-db-backend\s*=.*$",
    f"app-db-backend = \"{os.environ['APP_DB_BACKEND']}\"",
    data,
)
if count == 0:
    raise SystemExit("Failed to update app.toml (app-db-backend).")
app_path.write_text(data)
PY

LATEST="$(curl -fsSL ${CURL_OPTS} "${RPC1}/status" 2>/dev/null | jq -r .result.sync_info.latest_block_height || curl -fsSL ${CURL_OPTS} "${RPC2}/status" 2>/dev/null | jq -r .result.sync_info.latest_block_height)"
TRUST_HEIGHT=$((LATEST-2000))
TRUST_HASH="$(curl -fsSL ${CURL_OPTS} "${RPC1}/block?height=${TRUST_HEIGHT}" 2>/dev/null | jq -r .result.block_id.hash || curl -fsSL ${CURL_OPTS} "${RPC2}/block?height=${TRUST_HEIGHT}" 2>/dev/null | jq -r .result.block_id.hash)"

export HOME_DIR RPC1 RPC2 TRUST_HEIGHT TRUST_HASH
python3 - <<'PY'
import os
import re
from pathlib import Path

cfg_path = Path(os.environ["HOME_DIR"]) / "config" / "config.toml"
block = (
    "[statesync]\n"
    f"enable = true\n"
    f"rpc_servers = \"{os.environ['RPC1']},{os.environ['RPC2']}\"\n"
    f"trust_height = {os.environ['TRUST_HEIGHT']}\n"
    f"trust_hash = \"{os.environ['TRUST_HASH']}\"\n"
    "trust_period = \"168h\"\n\n"
)
data = cfg_path.read_text()
data, count = re.subn(r"[statesync][\s\S]*?(?=\n\[blocksync\])", block, data, count=1)
if count == 0:
    raise SystemExit("Failed to update statesync config block.")
cfg_path.write_text(data)
PY


sed -e 's/max_open_connections = 3$/max_open_connections = 900/g' -i ${HOME_DIR}/config/config.toml 
sed -i "s/max_num_inbound_peers = .*/max_num_inbound_peers = 100/g" ${HOME_DIR}/config/config.toml
sed -i "s/max_num_outbound_peers = .*/max_num_outbound_peers = 150/g" ${HOME_DIR}/config/config.toml
sed -i "s/upnp = .*/upnp = true/g" ${HOME_DIR}/config/config.toml
sed -i "s/external_address = .*/external_address = \"72.130.67.121:36656\"/g" ${HOME_DIR}/config/config.toml
sed -i "s/handshake_timeout = .*/handshake_timeout = \"5s\"/g" ${HOME_DIR}/config/config.toml
sed -i "s/dial_timeout = .*/dial_timeout = \"1s\"/g" ${HOME_DIR}/config/config.toml
sed -i "s/addr_book_strict = .*/addr_book_strict = true/g" ${HOME_DIR}/config/config.toml
sed -i "s/allow_duplicate_ip = .*/allow_duplicate_ip = false/g" ${HOME_DIR}/config/config.toml

START_EPOCH="$(date +%s)"
START_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

safe_du_bytes() {
  local target="$1"
  if [ -e "${target}" ]; then
    if du -sb "${target}" >/dev/null 2>&1; then
      du -sb "${target}" 2>/dev/null | awk '{print $1}'
    else
      du -sk "${target}" 2>/dev/null | awk '{print $1 * 1024}'
    fi
  else
    echo 0
  fi
}

START_HOME_BYTES="$(safe_du_bytes "${HOME_DIR}")"
START_DATA_BYTES="$(safe_du_bytes "${HOME_DIR}/data")"
START_APP_BYTES="$(safe_du_bytes "${HOME_DIR}/data/app")"
START_BLOCKSTORE_BYTES="$(safe_du_bytes "${HOME_DIR}/data/blockstore")"
MAX_RSS_KB=0
MAX_APP_BYTES=0
MAX_INDEX_BYTES=0
MAX_HWM_KB=0
{
  echo "start_utc=${START_TS}"
  echo "rpc1=${RPC1}"
  echo "rpc2=${RPC2}"
  echo "trust_height=${TRUST_HEIGHT}"
  echo "trust_hash=${TRUST_HASH}"
  echo "home=${HOME_DIR}"
  echo "db_backend=${DB_BACKEND}"
  echo "app_db_backend=${APP_DB_BACKEND}"
  echo "start_home_bytes=${START_HOME_BYTES}"
  echo "start_data_bytes=${START_DATA_BYTES}"
  echo "start_app_bytes=${START_APP_BYTES}"
  echo "start_blockstore_bytes=${START_BLOCKSTORE_BYTES}"
} >> "${TIME_LOG}"

echo "Starting node..."
/home/mikers/dev/snissn/celestia-app/build/celestia-appd start --home "${HOME_DIR}" --force-no-bbr >"${NODE_LOG}" 2>&1 &
NODE_PID=$!

echo "Waiting for local RPC..."
until curl -fsSL "${LOCAL_RPC}/status" >/dev/null 2>&1; do
  sleep 2
done

echo "Monitoring sync..."
while true; do
  LOCAL_STATUS="$(curl -fsSL "${LOCAL_RPC}/status" 2>/dev/null || true)"
  if [ -z "${LOCAL_STATUS}" ]; then
    echo "local RPC unavailable; stopping."
    break
  fi
  LOCAL_HEIGHT="$(echo "${LOCAL_STATUS}" | jq -r .result.sync_info.latest_block_height)"
  CATCHING_UP="$(echo "${LOCAL_STATUS}" | jq -r .result.sync_info.catching_up)"

  REMOTE_STATUS="$(curl -fsSL ${CURL_OPTS} "${RPC1}/status" 2>/dev/null || curl -fsSL ${CURL_OPTS} "${RPC2}/status" 2>/dev/null || true)"
  if [ -z "${REMOTE_STATUS}" ]; then
    echo "remote RPC unavailable; retrying."
    sleep 10
    continue
  fi
  REMOTE_HEIGHT="$(echo "${REMOTE_STATUS}" | jq -r .result.sync_info.latest_block_height)"
  REMOTE_TARGET=$((REMOTE_HEIGHT-2))
  if [ "${REMOTE_TARGET}" -lt 0 ]; then
    REMOTE_TARGET=0
  fi

  echo "local=${LOCAL_HEIGHT} catching_up=${CATCHING_UP} remote=${REMOTE_HEIGHT}"

  RSS_KB=""
  if command -v ps >/dev/null 2>&1; then
    RSS_KB="$(ps -o rss= -p "${NODE_PID}" 2>/dev/null | awk '{print $1}' || true)"
  fi
  if [ -z "${RSS_KB}" ] && [ -r "/proc/${NODE_PID}/status" ]; then
    RSS_KB="$(awk '/VmRSS:/ {print $2}' "/proc/${NODE_PID}/status" 2>/dev/null || true)"
  fi
  if [ -r "/proc/${NODE_PID}/status" ]; then
    HWM_KB="$(awk '/VmHWM:/ {print $2}' "/proc/${NODE_PID}/status" 2>/dev/null || true)"
  else
    HWM_KB=""
  fi
  if [ -n "${RSS_KB}" ] && [ "${RSS_KB}" -gt "${MAX_RSS_KB}" ]; then
    MAX_RSS_KB="${RSS_KB}"
  fi
  if [ -n "${HWM_KB}" ] && [ "${HWM_KB}" -gt "${MAX_HWM_KB}" ]; then
    MAX_HWM_KB="${HWM_KB}"
  fi

  if [ "${CATCHING_UP}" = "false" ] && [ "${LOCAL_HEIGHT}" -ge "${REMOTE_TARGET}" ]; then
    break
  fi
  sleep 10
done

END_EPOCH="$(date +%s)"
END_TS="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
DURATION=$((END_EPOCH-START_EPOCH))
END_HOME_BYTES="$(safe_du_bytes "${HOME_DIR}")"
END_DATA_BYTES="$(safe_du_bytes "${HOME_DIR}/data")"
END_APP_BYTES="$(safe_du_bytes "${HOME_DIR}/data/app")"
END_BLOCKSTORE_BYTES="$(safe_du_bytes "${HOME_DIR}/data/blockstore")"

{
  echo "end_utc=${END_TS}"
  echo "duration_seconds=${DURATION}"
  echo "final_local_height=${LOCAL_HEIGHT}"
  echo "final_remote_height=${REMOTE_HEIGHT}"
  echo "max_rss_kb=${MAX_RSS_KB}"
  echo "max_hwm_kb=${MAX_HWM_KB}"
  echo "end_home_bytes=${END_HOME_BYTES}"
  echo "end_data_bytes=${END_DATA_BYTES}"
  echo "end_app_bytes=${END_APP_BYTES}"
  echo "end_blockstore_bytes=${END_BLOCKSTORE_BYTES}"
  echo "---"
} >> "${TIME_LOG}"

echo "Caught up. Stopping node..."
SHUTDOWN_START_EPOCH="$(date +%s)"
kill -INT "${NODE_PID}" >/dev/null 2>&1 || true
wait "${NODE_PID}" >/dev/null 2>&1 || true
SHUTDOWN_END_EPOCH="$(date +%s)"
SHUTDOWN_DURATION=$((SHUTDOWN_END_EPOCH-SHUTDOWN_START_EPOCH))
{
  echo "shutdown_seconds=${SHUTDOWN_DURATION}"
} >> "${TIME_LOG}"


APP_DB="${HOME_DIR}/data/application.db"
BREAKDOWN_LOG="${LOG_DIR}/disk-breakdown.log"
if [ -d "${APP_DB}" ]; then
  {
    echo "app_db=${APP_DB}"
    echo "du_human:"
    du -sh "${APP_DB}" "${APP_DB}"/* 2>/dev/null || true
    echo "du_bytes:"
	    if du -sb "${APP_DB}" >/dev/null 2>&1; then
	      du -sb "${APP_DB}" "${APP_DB}"/* 2>/dev/null || true
	    else
	      du -sk "${APP_DB}" "${APP_DB}"/* 2>/dev/null | awk '{print $1 * 1024 " " $2}'
	    fi
	    echo "top_files_bytes:"
	    find "${APP_DB}" -type f -printf "%s %p\n" 2>/dev/null | sort -nr | head -n 20
	  } > "${BREAKDOWN_LOG}"
  echo "Disk breakdown log: ${BREAKDOWN_LOG}"
fi

echo "Sync complete. Time log: ${TIME_LOG}"
