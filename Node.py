from __future__ import annotations

import argparse
import random
import socket
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from Neighbor import Neighbor


# -----------------------------
# Config structs + parsing
# -----------------------------

@dataclass(frozen=True)
class PeerEntry:
    pid: int
    ip: str
    port: int
    has_file: bool  # 0/1 in PeerInfo.txt


@dataclass(frozen=True)
class CommonConfig:
    # peer-selection config
    number_of_preferred_neighbors: int
    unchoking_interval: int
    optimistic_unchoking_interval: int

    # file config
    file_name: str
    file_size: int
    piece_size: int


def load_peerinfo(path: str) -> List[PeerEntry]:
    """
    PeerInfo.txt format:
      <id> <ip> <port> <hasFile>
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"peerinfo file not found: {path}")

    entries: List[PeerEntry] = []
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) != 4:
            raise ValueError(f"Invalid peerinfo line (expected: id ip port hasFile): {raw}")
        pid = int(parts[0])
        ip = parts[1]
        port = int(parts[2])
        has_file_int = int(parts[3])
        if has_file_int not in (0, 1):
            raise ValueError(f"hasFile must be 0 or 1 (got {has_file_int}) in line: {raw}")
        entries.append(PeerEntry(pid=pid, ip=ip, port=port, has_file=(has_file_int == 1)))
    return entries


def load_common_config(path: str) -> CommonConfig:
    """
    Common.txt format example:
        NumberOfPreferredNeighbors 2
        UnchokingInterval 5
        OptimisticUnchokingInterval 15
        FileName TheFile.dat
        FileSize 10000232
        PieceSize 32768
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"common config file not found: {path}")

    kv: Dict[str, str] = {}
    for raw in p.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            continue
        k, v = parts[0].strip(), parts[1].strip()
        kv[k] = v

    required = [
        "NumberOfPreferredNeighbors",
        "UnchokingInterval",
        "OptimisticUnchokingInterval",
        "FileSize",
        "PieceSize",
    ]
    missing = [k for k in required if k not in kv]
    if missing:
        raise ValueError(f"Common config missing fields: {missing}")

    num_pref = int(kv["NumberOfPreferredNeighbors"])
    unchoke = int(kv["UnchokingInterval"])
    opt_unchoke = int(kv["OptimisticUnchokingInterval"])
    file_size = int(kv["FileSize"])
    piece_size = int(kv["PieceSize"])
    file_name = kv.get("FileName", "").strip()

    if num_pref < 0:
        raise ValueError("NumberOfPreferredNeighbors must be >= 0")
    if unchoke <= 0 or opt_unchoke <= 0:
        raise ValueError("UnchokingInterval and OptimisticUnchokingInterval must be > 0")
    if file_size < 0 or piece_size <= 0:
        raise ValueError("Invalid FileSize/PieceSize values")

    return CommonConfig(
        number_of_preferred_neighbors=num_pref,
        unchoking_interval=unchoke,
        optimistic_unchoking_interval=opt_unchoke,
        file_name=file_name,
        file_size=file_size,
        piece_size=piece_size,
    )


# -----------------------------
# Choke state (local tracking)
# -----------------------------

class ChokeState(str, Enum):
    CHOKED = "choked"
    UNCHOKED = "unchoked"


# -----------------------------
# Node
# -----------------------------

class Node:
    def __init__(
        self,
        my_id: int,
        listen_ip: str,
        listen_port: int,
        all_peers: List[PeerEntry],
        peerinfo_path: str,
        common: CommonConfig,
        connect_timeout_s: float = 5.0,
        retry_interval_s: float = 1.0,
    ):
        self.my_id = my_id
        self.listen_ip = listen_ip
        self.listen_port = listen_port
        self.all_peers = all_peers
        self.peerinfo_path = peerinfo_path

        # Common config
        self.common = common
        self.NumberOfPreferredNeighbors = common.number_of_preferred_neighbors
        self.UnchokingInterval = common.unchoking_interval
        self.OptimisticUnchokingInterval = common.optimistic_unchoking_interval
        self.FileSize = common.file_size
        self.PieceSize = common.piece_size
        self.FileName = common.file_name

        # Determine whether THIS node starts with the file (from PeerInfo.txt)
        me = next((p for p in all_peers if p.pid == my_id), None)
        if me is None:
            raise ValueError(f"my_id {my_id} not found in peer list")
        self.has_file_initially = me.has_file

        # REQUIRED: neighbors dict indexed by peer process id -> tcp socket
        self.neighbors: Dict[int, socket.socket] = {}

        # Neighbor objects (for downloadsize/interested/reset_preferences)
        self._neighbor_objs: Dict[int, Neighbor] = {}
        self._neighbors_lock = threading.Lock()

        # Local choke state per peer_id
        self.choke_state: Dict[int, ChokeState] = {}

        self._server_sock: Optional[socket.socket] = None
        self._stop_evt = threading.Event()

        self.connect_timeout_s = connect_timeout_s
        self.retry_interval_s = retry_interval_s

        # Start gate
        self._started = False

        # All peer IDs except self (required connections)
        self.required_peer_ids: Set[int] = {p.pid for p in self.all_peers if p.pid != self.my_id}

        # Map pid -> PeerEntry for dialing
        self._peer_by_id: Dict[int, PeerEntry] = {p.pid: p for p in self.all_peers}

        # -----------------------------
        # PiecesIHave bitfield (4 bytes)
        # -----------------------------
        self.PiecesIHave: bytes = b"\xFF\xFF\xFF\xFF" if self.has_file_initially else b"\x00\x00\x00\x00"

        # If hasFile==1: load the file into memory
        self.file_bytes: Optional[bytes] = None
        if self.has_file_initially:
            self._load_file_into_memory()

        # -----------------------------
        # Neighbor selection state
        # -----------------------------
        self.preferredneighbors: List[int] = []          # peer_ids
        self.optimisticneighbor: Optional[int] = None    # peer_id

        # Timer threads start only after Start()
        self._preferred_clock_thread: Optional[threading.Thread] = None
        self._optimistic_clock_thread: Optional[threading.Thread] = None

    # ---------------- Lifecycle ----------------

    def start(self) -> None:
        self._start_server()
        threading.Thread(target=self._dial_missing_peers_forever, name="DialMissingPeers", daemon=True).start()
        threading.Thread(target=self._wait_for_all_connections_then_start, name="WaitAllPeers", daemon=True).start()

    def stop(self) -> None:
        self._stop_evt.set()

        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass

        with self._neighbors_lock:
            objs = list(self._neighbor_objs.values())

        for n in objs:
            n.close()

    # ---------------- File handling ----------------

    def _load_file_into_memory(self) -> None:
        if not self.FileName:
            raise FileNotFoundError("FileName is empty in common config, but this node has hasFile=1")

        fp = Path(self.FileName)
        if not fp.exists():
            raise FileNotFoundError(f"FileName '{self.FileName}' not found on disk for hasFile=1")

        data = fp.read_bytes()

        if self.FileSize != len(data):
            print(f"[warn] FileSize={self.FileSize} but actual '{self.FileName}' size={len(data)} bytes")

        self.file_bytes = data
        print(f"[Node {self.my_id}] Loaded file into memory: '{self.FileName}' ({len(data)} bytes)")

    # ---------------- Server ----------------

    def _start_server(self) -> None:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.listen_ip, self.listen_port))
        s.listen()
        self._server_sock = s

        threading.Thread(target=self._accept_loop, name="AcceptLoop", daemon=True).start()
        print(f"[Node {self.my_id}] Listening on {self.listen_ip}:{self.listen_port}")
        print(f"[Node {self.my_id}] PiecesIHave={self.PiecesIHave.hex()} (4 bytes)")
        print(
            f"[Node {self.my_id}] "
            f"K={self.NumberOfPreferredNeighbors} "
            f"Unchoke={self.UnchokingInterval}s OptUnchoke={self.OptimisticUnchokingInterval}s "
            f"FileSize={self.FileSize} PieceSize={self.PieceSize} hasFile={int(self.has_file_initially)}"
        )

    def _accept_loop(self) -> None:
        assert self._server_sock is not None
        while not self._stop_evt.is_set():
            try:
                conn, addr = self._server_sock.accept()
            except OSError:
                break
            except Exception as e:
                print(f"[Node {self.my_id}] Accept error: {e}")
                continue

            threading.Thread(target=self._handle_inbound, args=(conn, addr), daemon=True).start()

    def _handle_inbound(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        neighbor = Neighbor(self, conn, addr, outbound=False)
        try:
            peer_id = neighbor.do_handshake()
            self._register_neighbor(peer_id, neighbor)
            print(f"[Node {self.my_id}] Inbound connected peer_id={peer_id} from {addr}")
            neighbor.start()
        except Exception as e:
            print(f"[Node {self.my_id}] Inbound handshake failed from {addr}: {e}")
            neighbor.close()

    # ---------------- Dialing logic ----------------

    def _dial_missing_peers_forever(self) -> None:
        for pid in self.required_peer_ids:
            threading.Thread(target=self._dial_peer_until_connected, args=(pid,), daemon=True).start()

        while not self._stop_evt.is_set():
            time.sleep(0.5)

    def _dial_peer_until_connected(self, peer_id: int) -> None:
        peer = self._peer_by_id.get(peer_id)
        if peer is None:
            return

        while not self._stop_evt.is_set():
            with self._neighbors_lock:
                if peer_id in self.neighbors:
                    return

            sock: Optional[socket.socket] = None
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.connect_timeout_s)
                sock.connect((peer.ip, peer.port))
                sock.settimeout(None)

                neighbor = Neighbor(self, sock, (peer.ip, peer.port), outbound=True)
                peer_id_from_hs = neighbor.do_handshake()

                if peer_id_from_hs != peer_id:
                    raise ConnectionError(
                        f"Connected to {peer.ip}:{peer.port} but handshake id={peer_id_from_hs}, expected {peer_id}"
                    )

                self._register_neighbor(peer_id, neighbor)
                print(f"[Node {self.my_id}] Outbound connected peer_id={peer_id} to {peer.ip}:{peer.port}")
                neighbor.start()
                return

            except Exception:
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass
                time.sleep(self.retry_interval_s)

    # ---------------- Start gate ----------------

    def _all_required_connected(self) -> bool:
        with self._neighbors_lock:
            return self.required_peer_ids.issubset(self.neighbors.keys())

    def _wait_for_all_connections_then_start(self) -> None:
        if not self.required_peer_ids:
            self._trigger_start_once()
            return

        while not self._stop_evt.is_set():
            if self._all_required_connected():
                self._trigger_start_once()
                return
            time.sleep(0.05)

    def _trigger_start_once(self) -> None:
        with self._neighbors_lock:
            if self._started:
                return
            self._started = True

        print("all connections have been successfully established")

        # On start: choose random optimistic + preferred (initial state)
        self._initialize_random_unchokes()

        self.Start()

        # Start interval logic only after process begins
        self._preferred_clock_thread = threading.Thread(
            target=self._preferred_interval_loop, name="PreferredIntervalLoop", daemon=True
        )
        self._optimistic_clock_thread = threading.Thread(
            target=self._optimistic_interval_loop, name="OptimisticIntervalLoop", daemon=True
        )
        self._preferred_clock_thread.start()
        self._optimistic_clock_thread.start()

    def Start(self) -> None:
        print("Start triggered")

    # ---------------- Neighbor registration ----------------

    def _register_neighbor(self, peer_id: int, neighbor: Neighbor) -> None:
        if peer_id == self.my_id:
            neighbor.close()
            return

        with self._neighbors_lock:
            if peer_id in self.neighbors:
                neighbor.close()
                return

            self.neighbors[peer_id] = neighbor.sock
            self._neighbor_objs[peer_id] = neighbor
            self.choke_state[peer_id] = ChokeState.CHOKED  # default choked until selected

    # ---------------- Selection logic ----------------

    # -----------------------------
    # Interest / Request helpers (new)
    # -----------------------------

    def request(self, peer_id: int) -> None:
        """
        Placeholder for request logic.
        Right now: does nothing.
        """
        _ = peer_id
        return

    def checkifinterested(self, their_bitfield: bytes) -> bool:
        """
        Return True if this node is missing at least one piece that the neighbor has.

        Bit rule (32-bit):
            interested if (their_bits & ~my_bits) != 0
        """
        if not isinstance(their_bitfield, (bytes, bytearray)) or len(their_bitfield) != 4:
            return False
        if not isinstance(self.PiecesIHave, (bytes, bytearray)) or len(self.PiecesIHave) != 4:
            return False

        my_bits = int.from_bytes(self.PiecesIHave, byteorder="big", signed=False)
        their_bits = int.from_bytes(their_bitfield, byteorder="big", signed=False)

        missing_mask = (~my_bits) & 0xFFFFFFFF
        return (their_bits & missing_mask) != 0


    def _initialize_random_unchokes(self) -> None:
        with self._neighbors_lock:
            ids = list(self._neighbor_objs.keys())

        if not ids:
            self.preferredneighbors = []
            self.optimisticneighbor = None
            return

        random.shuffle(ids)

        # optimistic: any one random
        self.optimisticneighbor = ids[0]

        # preferred: random K excluding optimistic
        remaining = [pid for pid in ids if pid != self.optimisticneighbor]
        k = max(0, self.NumberOfPreferredNeighbors)
        self.preferredneighbors = remaining[:k] if k > 0 else []

        self._apply_choke_states()

        print(f"[Node {self.my_id}] Initial preferred={self.preferredneighbors} optimistic={self.optimisticneighbor}")

    def _preferred_interval_loop(self) -> None:
        while not self._stop_evt.is_set():
            time.sleep(self.UnchokingInterval)
            if self._stop_evt.is_set():
                break
            self._recompute_preferred_neighbors()

    def _optimistic_interval_loop(self) -> None:
        while not self._stop_evt.is_set():
            time.sleep(self.OptimisticUnchokingInterval)
            if self._stop_evt.is_set():
                break
            self._recompute_optimistic_neighbor()

    def _recompute_preferred_neighbors(self) -> None:
        """
        Preferred interval:
        - consider ONLY interested neighbors
        - rank by downloadsize desc
        - pick top K; if tie for last slot, choose randomly among tied
        - if fewer than K interested, fill randomly from interested
        - reset downloadsize (preferredreset) on all neighbors
        - update choke_state
        """
        k = max(0, self.NumberOfPreferredNeighbors)
        if k == 0:
            self.preferredneighbors = []
            self._apply_choke_states()
            self._reset_preference_stats_all()
            print(f"[Node {self.my_id}] Preferred update: K=0 -> preferred=[]")
            return

        with self._neighbors_lock:
            candidates: List[Tuple[int, int]] = []
            for pid, n in self._neighbor_objs.items():
                if bool(getattr(n, "interested", False)):
                    candidates.append((pid, int(getattr(n, "downloadsize", 0))))

        if not candidates:
            self.preferredneighbors = []
            self._apply_choke_states()
            self._reset_preference_stats_all()
            print(f"[Node {self.my_id}] Preferred update: no interested peers -> preferred=[]")
            return

        # Sort by downloadsize desc
        candidates.sort(key=lambda t: t[1], reverse=True)

        chosen: List[int] = []
        i = 0
        while i < len(candidates) and len(chosen) < k:
            # collect tie group at this score
            score = candidates[i][1]
            tie_group: List[int] = []
            while i < len(candidates) and candidates[i][1] == score:
                tie_group.append(candidates[i][0])
                i += 1

            slots_left = k - len(chosen)
            if len(tie_group) <= slots_left:
                chosen.extend(tie_group)
            else:
                # tie spillover: choose randomly among tied to fill remaining slots
                random.shuffle(tie_group)
                chosen.extend(tie_group[:slots_left])

        # If still not full, fill randomly from remaining interested (shouldn't happen with loop above, but safe)
        if len(chosen) < k:
            remaining = [pid for pid, _sz in candidates if pid not in chosen]
            random.shuffle(remaining)
            chosen.extend(remaining[: (k - len(chosen))])

        # Exclude optimistic from preferred (standard)
        if self.optimisticneighbor is not None and self.optimisticneighbor in chosen:
            chosen = [pid for pid in chosen if pid != self.optimisticneighbor]
            if len(chosen) < k:
                pool = [pid for pid, _sz in candidates if pid not in chosen and pid != self.optimisticneighbor]
                random.shuffle(pool)
                if pool:
                    chosen.append(pool[0])

        self.preferredneighbors = chosen
        self._apply_choke_states()
        self._reset_preference_stats_all()

        print(f"[Node {self.my_id}] Preferred update: preferred={self.preferredneighbors}")

    def _recompute_optimistic_neighbor(self) -> None:
        """
        Optimistic interval:
        - choose random neighbor that is interested AND currently choked AND not preferred
        - if none, fallback to any interested not-preferred
        - update choke_state
        """
        with self._neighbors_lock:
            choked_interested = []
            fallback = []
            for pid, n in self._neighbor_objs.items():
                if not bool(getattr(n, "interested", False)):
                    continue
                if pid in self.preferredneighbors:
                    continue
                fallback.append(pid)
                if self.choke_state.get(pid) == ChokeState.CHOKED:
                    choked_interested.append(pid)

        candidates = choked_interested or fallback
        if not candidates:
            return

        self.optimisticneighbor = random.choice(candidates)
        self._apply_choke_states()

        print(f"[Node {self.my_id}] Optimistic update: optimistic={self.optimisticneighbor}")

    def _apply_choke_states(self) -> None:
        """
        Local-side state:
          UNCHOKED if preferred OR optimistic
          CHOKED otherwise
        """
        with self._neighbors_lock:
            for pid in self._neighbor_objs.keys():
                if pid in self.preferredneighbors or (self.optimisticneighbor is not None and pid == self.optimisticneighbor):
                    self.choke_state[pid] = ChokeState.UNCHOKED
                else:
                    self.choke_state[pid] = ChokeState.CHOKED

    def _reset_preference_stats_all(self) -> None:
        with self._neighbors_lock:
            for n in self._neighbor_objs.values():
                n.reset_preferences()

    # ---------------- Callbacks from Neighbor ----------------

    def on_packet(self, neighbor: Neighbor, payload: bytes) -> None:
        # Neighbor sends raw payload as: [type][body...]
        msg_type = payload[0] if payload else None
        print(f"[Node {self.my_id}] RX from peer_id={neighbor.peer_id} type={msg_type} bytes={len(payload)}")

    def on_neighbor_disconnected(self, neighbor: Neighbor, exc: Exception) -> None:
        pid = neighbor.peer_id
        print(f"[Node {self.my_id}] Disconnected peer_id={pid} addr={neighbor.addr} reason={exc}")
        if pid is None:
            return
        with self._neighbors_lock:
            cur_sock = self.neighbors.get(pid)
            if cur_sock is neighbor.sock:
                self.neighbors.pop(pid, None)
                self._neighbor_objs.pop(pid, None)
                self.choke_state.pop(pid, None)

        if self.optimisticneighbor == pid:
            self.optimisticneighbor = None
        if pid in self.preferredneighbors:
            self.preferredneighbors = [x for x in self.preferredneighbors if x != pid]


def main() -> None:
    ap = argparse.ArgumentParser(description="Node process (server+client) waiting for ALL peers except itself.")
    ap.add_argument("--ip", required=True, help="IP to listen on")
    ap.add_argument("--port", required=True, type=int, help="Port to listen on")
    ap.add_argument("--id", required=True, type=int, help="This process id (uint32)")
    ap.add_argument("--peerinfo", required=True, help="Path to PeerInfo.txt")
    ap.add_argument("--commonconfig", required=True, help="Path to Common.txt")
    args = ap.parse_args()

    common = load_common_config(args.commonconfig)
    all_entries = load_peerinfo(args.peerinfo)

    my_entry: Optional[PeerEntry] = next((e for e in all_entries if e.pid == args.id), None)
    if my_entry is None:
        raise SystemExit(f"Process id {args.id} not found in peerinfo file")

    if my_entry.ip != args.ip or my_entry.port != args.port:
        print(
            f"[warn] PeerInfo lists this node as {my_entry.ip}:{my_entry.port}, "
            f"but args are {args.ip}:{args.port}"
        )

    node = Node(
        my_id=args.id,
        listen_ip=args.ip,
        listen_port=args.port,
        all_peers=all_entries,
        peerinfo_path=args.peerinfo,
        common=common,
    )

    node.start()

    print(f"[Node {args.id}] Running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        print(f"[Node {args.id}] Stopped.")


if __name__ == "__main__":
    main()
