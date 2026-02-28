# P2P Project README

## Requirements
- Python 3.14+
- No third-party packages (`requirements.txt` is intentionally empty of pip deps)

## Config Files
This project uses two config files in the project root:
- `PeerInfo.txt`: peer id, ip, port, hasFile (one peer per line)
- `Common.txt`: preferred neighbor counts, intervals, file name, file size, piece size

Current files:
- `PeerInfo.txt`
- `Common.txt`

## Run (Recommended: `Node.py`)
`Node.py` is the script that takes **both** PeerInfo and Common config.

Example for one peer:
```powershell
python .\Node.py --ip 127.0.0.1 --port 6001 --id 1001 --peerinfo .\PeerInfo.txt --commonconfig .\Common.txt
```

Start all peers from `PeerInfo.txt` (Windows):
```powershell
.\RunAllLocalNodes.bat
```

## Run (`ServerClient.py`)
`ServerClient.py` currently accepts `--peerinfo` (not `--commonconfig`).

Example:
```powershell
python .\ServerClient.py --ip 127.0.0.1 --port 6001 --id 1001 --peerinfo .\PeerInfo.txt
```

## Notes
- Start peers in the same order as `PeerInfo.txt` when launching manually.
- Peer with `hasFile=1` should already have `thefile` in the project folder.
