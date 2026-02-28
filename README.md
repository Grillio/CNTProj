# P2P Project README

## What To Run
Run `Node.py` for this project.

`Node.py` is the main program entrypoint and is what each peer process should execute.

## Script Roles
- `Node.py`: main peer node implementation (uses both config files).
- `Neighbor.py`: defines the `Neighbor` class used by `Node.py` to manage per-neighbor connection/state.
- `ServerClient.py`: separate standalone networking script (simpler/older); `Node.py` does **not** import or call it.

## Requirements
- Python 3.14+
- No third-party packages (`requirements.txt` is intentionally empty of pip deps)

## Config Files
This project uses two config files in the project root:
- `PeerInfo.txt`: peer id, ip, port, hasFile (one peer per line)
- `Common.txt`: preferred neighbor counts, intervals, file name, file size, piece size

## Run (Main Project Flow)
Example for one peer:
```powershell
python .\Node.py --ip 127.0.0.1 --port 6001 --id 1001 --peerinfo .\PeerInfo.txt --commonconfig .\Common.txt
```

Start all peers from `PeerInfo.txt` (Windows):
```powershell
.\RunAllLocalNodes.bat
```

## Notes
- Start peers in the same order as `PeerInfo.txt` when launching manually.
- Peers with `hasFile=1` should already have `thefile` in the project folder.
