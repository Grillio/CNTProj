#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

PEERINFO=""
COMMON=""
if [[ -f "PeerInfo.cfg" ]]; then PEERINFO="PeerInfo.cfg"; elif [[ -f "PeerInfo.txt" ]]; then PEERINFO="PeerInfo.txt"; fi
if [[ -f "Common.cfg" ]]; then COMMON="Common.cfg"; elif [[ -f "Common.txt" ]]; then COMMON="Common.txt"; fi

if [[ -z "$PEERINFO" ]]; then
  echo "[ERROR] Could not find PeerInfo.cfg or PeerInfo.txt in $(pwd)"
  exit 1
fi

if [[ -z "$COMMON" ]]; then
  echo "[ERROR] Could not find Common.cfg or Common.txt in $(pwd)"
  exit 1
fi

if [[ ! -f "./peerProcess.py" ]]; then
  echo "[ERROR] Could not find ./peerProcess.py in $(pwd)"
  exit 1
fi
LAUNCHER=(python3 "./peerProcess.py")

echo "Launching nodes from $PEERINFO (common: $COMMON)..."
echo

while read -r ID IP PORT HASFILE; do
  [[ -z "${ID:-}" ]] && continue
  [[ "${ID:0:1}" == "#" ]] && continue
  echo "[START] id=$ID ip=$IP port=$PORT hasFile=$HASFILE"
  "${LAUNCHER[@]}" "$ID" > "node_${ID}.out" 2>&1 &
  sleep 1
  echo
done < "$PEERINFO"

echo "All processes started."
echo "Stop all with: pkill -f \"peerProcess.py\""
