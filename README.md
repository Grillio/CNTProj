# P2P File Sharing — BitTorrent-like Protocol

## Group Members
- Carter Nosek
- Brandon Grill
- Eva Nastevska

## Requirements
- Python 3.10+
- No third-party packages

## Files
- `peerProcess.py` — main peer node implementation; reads both config files, manages all protocol logic
- `Neighbor.py` — per-connection class handling handshake, framing, and message send/receive
- `peerProcess` — shell script entry point; runs `python3 peerProcess.py <peerID>`

## Config Files
Both files must be in the working directory when launching peers.

**`Common.cfg` / `Common.txt`**
```
NumberOfPreferredNeighbors 3
UnchokingInterval 5
OptimisticUnchokingInterval 10
FileName thefile
FileSize 2167705
PieceSize 16384
```

**`PeerInfo.cfg` / `PeerInfo.txt`** — one peer per line: `[peerID] [host] [port] [hasFile]`
```
1001 127.0.0.1 6001 1
1002 127.0.0.1 6002 0
```
Peers with `hasFile=1` must have the file already present in their `peer_<peerID>/` subdirectory before starting.

## Running

Start peers **in the order they appear in PeerInfo**, one at a time. Each peer connects outbound to all peers listed before it.

**Single peer (Linux/Mac):**
```bash
python3 peerProcess.py 1001
```

**All peers on one machine (Linux/Mac):**
```bash
bash RunAllLocalNodes.sh
```

**All peers on one machine (Windows):**
```powershell
.\RunAllLocalNodes.bat
```

The `.bat` script requires a `venv/` in the project directory. The `.sh` script uses the system `python3`.

## Logs
Each peer writes a log to `log_peer_<peerID>.log` in the working directory. Log entries cover TCP connections, choking/unchoking, have/interested messages, piece downloads, and file completion.

## Peer Subdirectories
Each peer uses `peer_<peerID>/` as its working directory for file storage. Create these before running if they do not already exist.

## Termination
Peers shut down automatically once every peer listed in `PeerInfo` has received the complete file.
