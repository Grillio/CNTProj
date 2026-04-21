from __future__ import annotations

import datetime
import random
import socket
import struct
import sys
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from Neighbor import (
    MSG_BITFIELD,
    MSG_CHOKE,
    MSG_HAVE,
    MSG_INTERESTED,
    MSG_NOT_INTERESTED,
    MSG_PIECE,
    MSG_REQUEST,
    MSG_UNCHOKE,
    Neighbor,
)


@dataclass(frozen=True)
class PeerEntry:
    pid: int
    ip: str
    port: int
    has_file: bool


@dataclass(frozen=True)
class CommonConfig:
    number_of_preferred_neighbors: int
    unchoking_interval: int
    optimistic_unchoking_interval: int
    file_name: str
    file_size: int
    piece_size: int


def load_peerinfo(path: str) -> List[PeerEntry]:
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
        "FileName",
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
    file_name = kv["FileName"].strip()
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


class ChokeState(str, Enum):
    CHOKED = "choked"
    UNCHOKED = "unchoked"


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

        self.common = common
        self.NumberOfPreferredNeighbors = common.number_of_preferred_neighbors
        self.UnchokingInterval = common.unchoking_interval
        self.OptimisticUnchokingInterval = common.optimistic_unchoking_interval
        self.FileSize = common.file_size
        self.PieceSize = common.piece_size
        self.FileName = common.file_name

        me = next((p for p in all_peers if p.pid == my_id), None)
        if me is None:
            raise ValueError(f"my_id {my_id} not found in peer list")
        self.has_file_initially = me.has_file

        self.neighbors: Dict[int, socket.socket] = {}
        self._neighbor_objs: Dict[int, Neighbor] = {}
        self._neighbors_lock = threading.Lock()

        self.choke_state: Dict[int, ChokeState] = {}
        self.am_choked_by: Set[int] = set()
        self._interest_sent: Dict[int, Optional[bool]] = {}

        self._server_sock: Optional[socket.socket] = None
        self._stop_evt = threading.Event()
        self._terminated = False

        self.connect_timeout_s = connect_timeout_s
        self.retry_interval_s = retry_interval_s
        self._started = False

        self.required_peer_ids: Set[int] = {p.pid for p in self.all_peers if p.pid != self.my_id}
        self._peer_by_id: Dict[int, PeerEntry] = {p.pid: p for p in self.all_peers}

        self.num_pieces = (self.FileSize + self.PieceSize - 1) // self.PieceSize if self.PieceSize > 0 else 0
        self.bitfield_len = (self.num_pieces + 7) // 8 if self.num_pieces > 0 else 0
        self.PiecesIHave = bytearray(self.bitfield_len)
        self._pieces: Dict[int, bytes] = {}
        self._pieces_lock = threading.Lock()
        self.pending_requests: Set[int] = set()
        self.requested_from: Dict[int, int] = {}

        self.preferredneighbors: List[int] = []
        self.optimisticneighbor: Optional[int] = None
        self._preferred_clock_thread: Optional[threading.Thread] = None
        self._optimistic_clock_thread: Optional[threading.Thread] = None

        self.peer_dir = Path(f"peer_{self.my_id}")
        self.peer_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = Path(f"log_peer_{self.my_id}.log")
        self.log_path.write_text("", encoding="utf-8")
        self.peer_completion: Dict[int, bool] = {p.pid: p.has_file for p in self.all_peers}
        self._completion_logged = self.has_file_initially

        if self.has_file_initially:
            self._load_initial_file()
            self.peer_completion[self.my_id] = True
        else:
            self.peer_completion[self.my_id] = False

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

    def _load_initial_file(self) -> None:
        if not self.FileName:
            raise FileNotFoundError("FileName is empty in common config, but this node has hasFile=1")
        fp = self.peer_dir / self.FileName
        if not fp.exists():
            fp = Path(self.FileName)
        if not fp.exists():
            raise FileNotFoundError(f"FileName '{self.FileName}' not found on disk for hasFile=1")
        data = fp.read_bytes()
        with self._pieces_lock:
            for i in range(self.num_pieces):
                start = i * self.PieceSize
                end = min(start + self.PieceSize, len(data))
                self._pieces[i] = data[start:end]
                self._set_piece_bit(i)

    def _start_server(self) -> None:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.listen_ip, self.listen_port))
        s.listen()
        self._server_sock = s
        threading.Thread(target=self._accept_loop, name="AcceptLoop", daemon=True).start()
        print(f"[Node {self.my_id}] Listening on {self.listen_ip}:{self.listen_port}")

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
            self._log(f"Peer {self.my_id} is connected from Peer {peer_id}.")
            self._send_initial_bitfield(neighbor)
            neighbor.start()
        except Exception as e:
            print(f"[Node {self.my_id}] Inbound handshake failed from {addr}: {e}")
            neighbor.close()

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
                self._log(f"Peer {self.my_id} makes a connection to Peer {peer_id}.")
                self._send_initial_bitfield(neighbor)
                neighbor.start()
                return
            except Exception:
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass
                time.sleep(self.retry_interval_s)

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
        self._initialize_random_unchokes()
        self.Start()
        self._preferred_clock_thread = threading.Thread(
            target=self._preferred_interval_loop, name="PreferredIntervalLoop", daemon=True
        )
        self._optimistic_clock_thread = threading.Thread(
            target=self._optimistic_interval_loop, name="OptimisticIntervalLoop", daemon=True
        )
        self._preferred_clock_thread.start()
        self._optimistic_clock_thread.start()

    def Start(self) -> None:
        return

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
            self.choke_state[peer_id] = ChokeState.CHOKED
            self._interest_sent[peer_id] = None
            self.am_choked_by.add(peer_id)

    def request(self, peer_id: int) -> None:
        with self._neighbors_lock:
            neighbor = self._neighbor_objs.get(peer_id)
        if neighbor is None:
            return
        if peer_id in self.am_choked_by:
            return
        idx = self._choose_request_index(peer_id)
        if idx < 0:
            return
        try:
            neighbor.send_request(idx)
            with self._pieces_lock:
                self.pending_requests.add(idx)
                self.requested_from[idx] = peer_id
        except Exception:
            return

    def checkifinterested(self, their_bitfield: bytes) -> bool:
        if not isinstance(their_bitfield, (bytes, bytearray)) or len(their_bitfield) != self.bitfield_len:
            return False
        for idx in range(self.num_pieces):
            if self._bit_is_set(their_bitfield, idx) and not self._has_piece_bit(idx):
                return True
        return False

    def _initialize_random_unchokes(self) -> None:
        self._recompute_preferred_neighbors()
        self._recompute_optimistic_neighbor()

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
        k = max(0, self.NumberOfPreferredNeighbors)
        with self._neighbors_lock:
            candidates: List[Tuple[int, int]] = []
            for pid, n in self._neighbor_objs.items():
                if bool(getattr(n, "interested", False)):
                    candidates.append((pid, int(getattr(n, "downloadsize", 0))))
        if k == 0 or not candidates:
            self.preferredneighbors = []
            self._apply_choke_states()
            self._reset_preference_stats_all()
            self._log(f"Peer {self.my_id} has the preferred neighbors .")
            return
        if self._is_complete_local():
            ids = [pid for pid, _ in candidates]
            random.shuffle(ids)
            chosen = ids[:k]
        else:
            candidates.sort(key=lambda t: t[1], reverse=True)
            chosen: List[int] = []
            i = 0
            while i < len(candidates) and len(chosen) < k:
                score = candidates[i][1]
                tie_group: List[int] = []
                while i < len(candidates) and candidates[i][1] == score:
                    tie_group.append(candidates[i][0])
                    i += 1
                slots_left = k - len(chosen)
                if len(tie_group) <= slots_left:
                    chosen.extend(tie_group)
                else:
                    random.shuffle(tie_group)
                    chosen.extend(tie_group[:slots_left])
            if len(chosen) < k:
                remaining = [pid for pid, _ in candidates if pid not in chosen]
                random.shuffle(remaining)
                chosen.extend(remaining[: (k - len(chosen))])
        self.preferredneighbors = chosen
        self._apply_choke_states()
        self._reset_preference_stats_all()
        pref_list = ",".join(str(pid) for pid in self.preferredneighbors)
        self._log(f"Peer {self.my_id} has the preferred neighbors {pref_list}.")

    def _recompute_optimistic_neighbor(self) -> None:
        with self._neighbors_lock:
            choked_interested: List[int] = []
            for pid, n in self._neighbor_objs.items():
                if not bool(getattr(n, "interested", False)):
                    continue
                if self.choke_state.get(pid, ChokeState.CHOKED) == ChokeState.CHOKED:
                    choked_interested.append(pid)
        if not choked_interested:
            self.optimisticneighbor = None
            self._apply_choke_states()
            return
        chosen = random.choice(choked_interested)
        if chosen != self.optimisticneighbor:
            self.optimisticneighbor = chosen
            self._log(f"Peer {self.my_id} has the optimistically unchoked neighbor {self.optimisticneighbor}.")
        self._apply_choke_states()

    def _apply_choke_states(self) -> None:
        with self._neighbors_lock:
            items = list(self._neighbor_objs.items())
        for pid, n in items:
            should_unchoke = pid in self.preferredneighbors or (
                self.optimisticneighbor is not None and pid == self.optimisticneighbor
            )
            desired = ChokeState.UNCHOKED if should_unchoke else ChokeState.CHOKED
            previous = self.choke_state.get(pid, ChokeState.CHOKED)
            if previous == desired:
                continue
            self.choke_state[pid] = desired
            try:
                if desired == ChokeState.UNCHOKED:
                    n.send_unchoke()
                else:
                    n.send_choke()
            except Exception:
                continue

    def _reset_preference_stats_all(self) -> None:
        with self._neighbors_lock:
            for n in self._neighbor_objs.values():
                n.reset_preferences()

    def on_packet(self, neighbor: Neighbor, payload: bytes) -> None:
        msg_type = payload[0] if payload else None
        body = payload[1:] if len(payload) > 1 else b""
        pid = neighbor.peer_id
        if pid is None or msg_type is None:
            return

        if msg_type == MSG_CHOKE:
            self.am_choked_by.add(pid)
            self._clear_pending_from_peer(pid)
            self._log(f"Peer {self.my_id} is choked by {pid}.")
            return

        if msg_type == MSG_UNCHOKE:
            self.am_choked_by.discard(pid)
            self._log(f"Peer {self.my_id} is unchoked by {pid}.")
            self.request(pid)
            return

        if msg_type == MSG_INTERESTED:
            neighbor.interested = True
            self._log(f"Peer {self.my_id} received the 'interested' message from {pid}.")
            return

        if msg_type == MSG_NOT_INTERESTED:
            neighbor.interested = False
            self._log(f"Peer {self.my_id} received the 'not interested' message from {pid}.")
            return

        if msg_type == MSG_HAVE:
            if len(body) != 4:
                return
            piece_index = struct.unpack("!I", body)[0]
            if piece_index >= self.num_pieces:
                return
            neighbor.bitfield = self._set_bit_in_bitfield_copy(neighbor.bitfield, piece_index)
            self._log(f"Peer {self.my_id} received the 'have' message from {pid} for the piece {piece_index}.")
            if self._is_bitfield_complete(neighbor.bitfield):
                self.peer_completion[pid] = True
            self._send_interest_state(pid)
            self._maybe_terminate()
            return

        if msg_type == MSG_BITFIELD:
            if len(body) != self.bitfield_len:
                return
            neighbor.bitfield = bytes(body)
            if self._is_bitfield_complete(neighbor.bitfield):
                self.peer_completion[pid] = True
            self._send_interest_state(pid)
            self._maybe_terminate()
            return

        if msg_type == MSG_REQUEST:
            if len(body) != 4:
                return
            piece_index = struct.unpack("!I", body)[0]
            if piece_index >= self.num_pieces:
                return
            if self.choke_state.get(pid, ChokeState.CHOKED) == ChokeState.CHOKED:
                return
            with self._pieces_lock:
                data = self._pieces.get(piece_index)
            if data is None:
                return
            try:
                neighbor.send_piece(piece_index, data)
            except Exception:
                return
            return

        if msg_type == MSG_PIECE:
            if len(body) < 4:
                return
            piece_index = struct.unpack("!I", body[:4])[0]
            piece_data = body[4:]
            if piece_index >= self.num_pieces:
                return
            if pid in self.am_choked_by:
                return
            new_piece = False
            with self._pieces_lock:
                self.pending_requests.discard(piece_index)
                self.requested_from.pop(piece_index, None)
                if piece_index not in self._pieces:
                    self._pieces[piece_index] = piece_data
                    self._set_piece_bit(piece_index)
                    new_piece = True
            if new_piece:
                neighbor.downloadsize += len(piece_data)
                cnt = self._piece_count_local()
                self._log(
                    f"Peer {self.my_id} has downloaded the piece {piece_index} from {pid}. "
                    f"Now the number of pieces it has is {cnt}."
                )
                self._broadcast_have(piece_index)
                if self._is_complete_local():
                    self.peer_completion[self.my_id] = True
                    self._write_completed_file()
                    if not self._completion_logged:
                        self._log(f"Peer {self.my_id} has downloaded the complete file.")
                        self._completion_logged = True
                self._refresh_all_interest_states()
            self.request(pid)
            self._maybe_terminate()
            return

    def on_neighbor_disconnected(self, neighbor: Neighbor, exc: Exception) -> None:
        pid = neighbor.peer_id
        if pid is None:
            return
        with self._neighbors_lock:
            cur_sock = self.neighbors.get(pid)
            if cur_sock is neighbor.sock:
                self.neighbors.pop(pid, None)
                self._neighbor_objs.pop(pid, None)
                self.choke_state.pop(pid, None)
                self._interest_sent.pop(pid, None)
        self._clear_pending_from_peer(pid)
        self.am_choked_by.discard(pid)
        if self.optimisticneighbor == pid:
            self.optimisticneighbor = None
        if pid in self.preferredneighbors:
            self.preferredneighbors = [x for x in self.preferredneighbors if x != pid]

    def _bit_is_set(self, bitfield: bytes | bytearray, piece_index: int) -> bool:
        if piece_index < 0 or piece_index >= self.num_pieces:
            return False
        byte_idx = piece_index // 8
        bit_idx = 7 - (piece_index % 8)
        return (bitfield[byte_idx] & (1 << bit_idx)) != 0

    def _set_piece_bit(self, piece_index: int) -> None:
        if piece_index < 0 or piece_index >= self.num_pieces:
            return
        byte_idx = piece_index // 8
        bit_idx = 7 - (piece_index % 8)
        self.PiecesIHave[byte_idx] |= 1 << bit_idx

    def _has_piece_bit(self, piece_index: int) -> bool:
        return self._bit_is_set(self.PiecesIHave, piece_index)

    def _set_bit_in_bitfield_copy(self, bitfield: bytes, piece_index: int) -> bytes:
        if len(bitfield) != self.bitfield_len:
            bitfield = bytes(self.bitfield_len)
        b = bytearray(bitfield)
        byte_idx = piece_index // 8
        bit_idx = 7 - (piece_index % 8)
        b[byte_idx] |= 1 << bit_idx
        return bytes(b)

    def _bitfield_bytes(self) -> bytes:
        return bytes(self.PiecesIHave)

    def _piece_count_local(self) -> int:
        with self._pieces_lock:
            return len(self._pieces)

    def _is_complete_local(self) -> bool:
        return self.num_pieces > 0 and self._piece_count_local() >= self.num_pieces

    def _is_bitfield_complete(self, bitfield: bytes) -> bool:
        if len(bitfield) != self.bitfield_len:
            return False
        for idx in range(self.num_pieces):
            if not self._bit_is_set(bitfield, idx):
                return False
        return True

    def _send_initial_bitfield(self, neighbor: Neighbor) -> None:
        if self.num_pieces == 0:
            return
        if self._piece_count_local() == 0:
            return
        try:
            neighbor.send_bitfield(self._bitfield_bytes())
        except Exception:
            return

    def _choose_request_index(self, peer_id: int) -> int:
        with self._neighbors_lock:
            neighbor = self._neighbor_objs.get(peer_id)
        if neighbor is None:
            return -1
        with self._pieces_lock:
            candidates = [
                idx
                for idx in range(self.num_pieces)
                if self._bit_is_set(neighbor.bitfield, idx) and idx not in self._pieces and idx not in self.pending_requests
            ]
        if not candidates:
            return -1
        return random.choice(candidates)

    def _broadcast_have(self, piece_index: int) -> None:
        with self._neighbors_lock:
            neighbors = list(self._neighbor_objs.values())
        for n in neighbors:
            try:
                n.send_have(piece_index)
            except Exception:
                continue

    def _send_interest_state(self, peer_id: int) -> None:
        with self._neighbors_lock:
            neighbor = self._neighbor_objs.get(peer_id)
        if neighbor is None:
            return
        interested = self.checkifinterested(neighbor.bitfield)
        prev = self._interest_sent.get(peer_id)
        if prev is interested:
            return
        try:
            if interested:
                neighbor.send_interested()
            else:
                neighbor.send_not_interested()
            self._interest_sent[peer_id] = interested
        except Exception:
            return

    def _refresh_all_interest_states(self) -> None:
        with self._neighbors_lock:
            ids = list(self._neighbor_objs.keys())
        for pid in ids:
            self._send_interest_state(pid)

    def _clear_pending_from_peer(self, peer_id: int) -> None:
        with self._pieces_lock:
            to_remove = [idx for idx, pid in self.requested_from.items() if pid == peer_id]
            for idx in to_remove:
                self.requested_from.pop(idx, None)
                self.pending_requests.discard(idx)

    def _write_completed_file(self) -> None:
        out_path = self.peer_dir / self.FileName
        with self._pieces_lock:
            data = b"".join(self._pieces[i] for i in range(self.num_pieces))
        out_path.write_bytes(data[: self.FileSize])

    def _maybe_terminate(self) -> None:
        if self._terminated:
            return
        if self._is_complete_local():
            self.peer_completion[self.my_id] = True
        with self._neighbors_lock:
            for pid, n in self._neighbor_objs.items():
                if self._is_bitfield_complete(n.bitfield):
                    self.peer_completion[pid] = True
        if all(self.peer_completion.get(p.pid, False) for p in self.all_peers):
            self._terminated = True
            self.stop()

    def _log(self, message: str) -> None:
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"{ts}: {message}\n"
        with self.log_path.open("a", encoding="utf-8") as f:
            f.write(line)
        print(line.strip())


def run_node(
    my_id: int,
    peerinfo_path: str,
    common_path: str,
) -> None:
    common = load_common_config(common_path)
    all_entries = load_peerinfo(peerinfo_path)
    my_entry: Optional[PeerEntry] = next((e for e in all_entries if e.pid == my_id), None)
    if my_entry is None:
        raise SystemExit(f"Process id {my_id} not found in peerinfo file")
    node = Node(
        my_id=my_id,
        listen_ip=my_entry.ip,
        listen_port=my_entry.port,
        all_peers=all_entries,
        peerinfo_path=peerinfo_path,
        common=common,
    )
    node.start()
    print(f"[Node {my_id}] Running. Ctrl+C to stop.")
    try:
        while not node._stop_evt.is_set():
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        print(f"[Node {my_id}] Stopped.")


def _first_existing(base: Path, names: tuple[str, ...]) -> Path:
    for name in names:
        candidate = base / name
        if candidate.is_file():
            return candidate
    tried = ", ".join(str(base / name) for name in names)
    raise SystemExit(f"Missing config file in {base} (tried: {tried})")


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: peerProcess.py <peerID>")
    try:
        my_id = int(sys.argv[1])
    except ValueError:
        raise SystemExit("usage: peerProcess.py <peerID> (peerID must be an integer)")

    config_dir = Path(__file__).resolve().parent
    peerinfo_path = _first_existing(config_dir, ("PeerInfo.txt", "peerinfo.txt"))
    common_path = _first_existing(config_dir, ("Common.txt", "common.txt"))
    run_node(
        my_id=my_id,
        peerinfo_path=str(peerinfo_path),
        common_path=str(common_path),
    )


if __name__ == "__main__":
    main()
