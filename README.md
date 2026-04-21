# P2P Project README

## What To Run
Run `peerProcess.py` for this project.

`peerProcess.py` is the main program entrypoint and is what each peer process should execute.

## Script Roles
- `peerProcess.py`: main peer node implementation (uses both config files).
- `Neighbor.py`: defines the `Neighbor` class used by `peerProcess.py` to manage per-neighbor connection/state.
- `ServerClient.py`: separate standalone networking script (simpler/older); `peerProcess.py` does **not** import or call it.

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
python .\peerProcess.py 1001
```

Start all peers from `PeerInfo.txt` (Windows):
```powershell
.\RunAllLocalNodes.bat
```

## Notes
- Start peers in the same order as `PeerInfo.txt` when launching manually.
- Peers with `hasFile=1` should already have `thefile` in the project folder.
