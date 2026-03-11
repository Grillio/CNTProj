#!/usr/bin/env python3
"""
Python 3.14+ single-file TCP P2P node (server + client) with 32-byte handshake.

What changed vs previous version:
- Reads a *peerinfo* config file, finds THIS node's entry (by --id),
  then connects ONLY to entries that appear *before* it in the file.
- Stores outbound+inbound established connections in:
      Node.neighbors : dict[int, socket.socket]
  where key = peer process id, value = the TCP socket.
- After dialing all prior peers (best-effort with retry), prints "done"
  once all required connections are established (or exits if impossible).

Config file format (peerinfo):
- Order matters (top to bottom).
- Blank lines and lines starting with # are ignored.
- Each valid line must include:  id ip port
  Examples:
    1001 127.0.0.1 6000
    1002 127.0.0.1 6001
"""

from __future__ import annotations

import argparse
import socket
import struct
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


HANDSHAKE_HEADER = b"P2PFILESHARINGPROJ"  # 18 bytes
HANDSHAKE_LEN = 32

FRAME_LEN_PREFIX = 4  # uint32 BE length prefix for packets (optional usage)


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Socket closed while receiving")
        data.extend(chunk)
    return bytes(data)


def build_handshake(my_id: int) -> bytes:
    if not (0 <= my_id <= 0xFFFFFFFF):
        raise ValueError("id must fit in uint32")
    return HANDSHAKE_HEADER + (b"\x00" * 10) + struct.pack("!I", my_id)


def parse_handshake(hs: bytes) -> int:
    if len(hs) != HANDSHAKE_LEN:
        raise ConnectionError(f"Bad handshake length: {len(hs)}")
    header = hs[0:18]
    reserved = hs[18:28]
    peer_id = struct.unpack("!I", hs[28:32])[0]
    if header != HANDSHAKE_HEADER:
        raise ConnectionError(f"Bad handshake header: {header!r}")
    if reserved != (b"\x00" * 10):
        raise ConnectionError("Bad handshake reserved bytes (expected 10 zeros)")
    return peer_id


@dataclass(frozen=True)
class PeerEntry:
    pid: int
    ip: str
    port: int


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
        if len(parts) != 3:
            raise ValueError(f"Invalid peerinfo line (expected: id ip port): {raw}")
        pid = int(parts[0])
        ip = parts[1]
        port = int(parts[2])
        entries.append(PeerEntry(pid=pid, ip=ip, port=port))
    return entries


class NeighborNode:
    """
    Wraps one TCP connection and does handshake + receive loop.
    Calls Node.on_packet(...) on framed packets (length-prefixed).
    """

    def __init__(self, node: "Node", sock: socket.socket, addr: Tuple[str, int], outbound: bool):
        self.node = node
        self.sock = sock
        self.addr = addr
        self.outbound = outbound
        self.peer_id: Optional[int] = None

        self._send_lock = threading.Lock()
        self._stop_evt = threading.Event()
        self._rx_thread = threading.Thread(target=self._rx_loop, daemon=True)

    def do_handshake(self) -> int:
        """
        Exchange 32-byte handshake.
        Outbound: send ours then read theirs
        Inbound : read theirs then send ours
        """
        self.sock.settimeout(10.0)
        if self.outbound:
            self._send_raw(build_handshake(self.node.my_id))
            peer_hs = _recv_exact(self.sock, HANDSHAKE_LEN)
        else:
            peer_hs = _recv_exact(self.sock, HANDSHAKE_LEN)
            self._send_raw(build_handshake(self.node.my_id))

        peer_id = parse_handshake(peer_hs)
        self.peer_id = peer_id

        self.sock.settimeout(None)
        return peer_id

    def start(self) -> None:
        self._rx_thread.start()

    def close(self) -> None:
        if self._stop_evt.is_set():
            return
        self._stop_evt.set()
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except Exception:
            pass
        try:
            self.sock.close()
        except Exception:
            pass

    def _send_raw(self, b: bytes) -> None:
        with self._send_lock:
            self.sock.sendall(b)

    def send_packet(self, payload: str | bytes) -> None:
        if isinstance(payload, str):
            payload_b = payload.encode("utf-8")
        else:
            payload_b = payload
        if len(payload_b) > 0xFFFFFFFF:
            raise ValueError("payload too large")
        frame = struct.pack("!I", len(payload_b)) + payload_b
        self._send_raw(frame)

    def _rx_loop(self) -> None:
        try:
            while not self._stop_evt.is_set():
                length_b = _recv_exact(self.sock, FRAME_LEN_PREFIX)
                (n,) = struct.unpack("!I", length_b)
                payload = _recv_exact(self.sock, n) if n > 0 else b""
                self.node.on_packet(self, payload)
        except Exception as e:
            self.node.on_neighbor_disconnected(self, e)
        finally:
            self.close()


class Node:
    def __init__(
        self,
        my_id: int,
        listen_ip: str,
        listen_port: int,
        prior_peers: List[PeerEntry],
        connect_timeout_s: float = 5.0,
        retry_interval_s: float = 1.0,
    ):
        self.my_id = my_id
        self.listen_ip = listen_ip
        self.listen_port = listen_port
        self.prior_peers = prior_peers

        # REQUIRED: neighbors dict indexed by peer process id -> tcp socket
        self.neighbors: Dict[int, socket.socket] = {}

        self._neighbor_nodes: Dict[int, NeighborNode] = {}  # internal convenience
        self._neighbors_lock = threading.Lock()

        self._server_sock: Optional[socket.socket] = None
        self._stop_evt = threading.Event()

        self.connect_timeout_s = connect_timeout_s
        self.retry_interval_s = retry_interval_s

        # Used to print "done" only once
        self._done_printed = False

    # ---------------- Server ----------------

    def start_server(self) -> None:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((self.listen_ip, self.listen_port))
        s.listen()
        self._server_sock = s

        t = threading.Thread(target=self._accept_loop, name="AcceptLoop", daemon=True)
        t.start()

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

            threading.Thread(
                target=self._handle_inbound,
                args=(conn, addr),
                daemon=True,
            ).start()

    def _handle_inbound(self, conn: socket.socket, addr: Tuple[str, int]) -> None:
        nn = NeighborNode(self, conn, addr, outbound=False)
        try:
            peer_id = nn.do_handshake()
            self._register_neighbor(peer_id, nn)
            print(f"[Node {self.my_id}] Inbound connected peer_id={peer_id} from {addr}")
            nn.start()
            self._check_done()
        except Exception as e:
            print(f"[Node {self.my_id}] Inbound handshake failed from {addr}: {e}")
            nn.close()

    # ---------------- Client (dial prior peers) ----------------

    def connect_to_prior_peers_blocking(self) -> None:
        """
        Connect to every peer entry that appears before us in the peerinfo file.
        This function blocks until all those connections are established.
        When finished, prints "done".
        """
        required_ids = [p.pid for p in self.prior_peers]
        if not required_ids:
            print("done")
            self._done_printed = True
            return

        # Start a dial thread per peer
        for p in self.prior_peers:
            threading.Thread(target=self._dial_peer_until_connected, args=(p,), daemon=True).start()

        # Block until all required peers are connected (by pid)
        while not self._stop_evt.is_set():
            with self._neighbors_lock:
                missing = [pid for pid in required_ids if pid not in self.neighbors]
            if not missing:
                self._check_done(force=True)
                return
            time.sleep(0.05)

    def _dial_peer_until_connected(self, peer: PeerEntry) -> None:
        # Don't dial ourselves even if config is wrong
        if peer.pid == self.my_id:
            return

        while not self._stop_evt.is_set():
            with self._neighbors_lock:
                if peer.pid in self.neighbors:
                    return

            sock: Optional[socket.socket] = None
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(self.connect_timeout_s)
                sock.connect((peer.ip, peer.port))
                sock.settimeout(None)

                nn = NeighborNode(self, sock, (peer.ip, peer.port), outbound=True)
                peer_id_from_hs = nn.do_handshake()

                # Safety: ensure handshake matches the config's pid
                if peer_id_from_hs != peer.pid:
                    raise ConnectionError(
                        f"Connected to {peer.ip}:{peer.port} but handshake id={peer_id_from_hs}, expected {peer.pid}"
                    )

                self._register_neighbor(peer.pid, nn)
                print(f"[Node {self.my_id}] Outbound connected peer_id={peer.pid} to {peer.ip}:{peer.port}")
                nn.start()
                self._check_done()
                return

            except Exception:
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass
                time.sleep(self.retry_interval_s)

    # ---------------- Shared neighbor management ----------------

    def _register_neighbor(self, peer_id: int, nn: NeighborNode) -> None:
        """
        Store required mapping:
          neighbors[peer_id] = tcp socket
        """
        with self._neighbors_lock:
            # If already connected, keep the existing one and close the new.
            if peer_id in self.neighbors:
                nn.close()
                return

            self.neighbors[peer_id] = nn.sock
            self._neighbor_nodes[peer_id] = nn

    def _check_done(self, force: bool = False) -> None:
        if self._done_printed:
            return
        required_ids = [p.pid for p in self.prior_peers]
        with self._neighbors_lock:
            ok = all(pid in self.neighbors for pid in required_ids)
        if force or ok:
            print("done")
            self._done_printed = True

    # ---------------- Callbacks ----------------

    def on_packet(self, neighbor: NeighborNode, payload: bytes) -> None:
        msg = payload.decode("utf-8", errors="replace")
        print(f"[Node {self.my_id}] RX from peer_id={neighbor.peer_id} addr={neighbor.addr}: {msg}")

    def on_neighbor_disconnected(self, neighbor: NeighborNode, exc: Exception) -> None:
        pid = neighbor.peer_id
        print(f"[Node {self.my_id}] Disconnected peer_id={pid} addr={neighbor.addr} reason={exc}")
        if pid is None:
            return
        with self._neighbors_lock:
            # Remove only if it still matches this socket object
            cur_sock = self.neighbors.get(pid)
            if cur_sock is neighbor.sock:
                self.neighbors.pop(pid, None)
                self._neighbor_nodes.pop(pid, None)

    def stop(self) -> None:
        self._stop_evt.set()
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass
        with self._neighbors_lock:
            nodes = list(self._neighbor_nodes.values())
        for n in nodes:
            n.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ip", required=True, help="IP to listen on")
    ap.add_argument("--port", required=True, type=int, help="Port to listen on")
    ap.add_argument("--id", required=True, type=int, help="This process id (uint32)")
    ap.add_argument("--peerinfo", required=True, help="Path to peerinfo file (ordered)")
    args = ap.parse_args()

    all_entries = load_peerinfo(args.peerinfo)

    # Find where this id appears
    idx = None
    my_entry: Optional[PeerEntry] = None
    for i, e in enumerate(all_entries):
        if e.pid == args.id:
            idx = i
            my_entry = e
            break
    if idx is None or my_entry is None:
        raise SystemExit(f"Process id {args.id} not found in peerinfo file")

    if my_entry.ip != args.ip or my_entry.port != args.port:
        print(
            f"[warn] peerinfo says this node is {my_entry.ip}:{my_entry.port} "
            f"but args are {args.ip}:{args.port}"
        )

    prior_peers = all_entries[:idx]

    node = Node(
        my_id=args.id,
        listen_ip=args.ip,
        listen_port=args.port,
        prior_peers=prior_peers,
    )

    node.start_server()

    try:
        node.connect_to_prior_peers_blocking()

        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        pass
    finally:
        node.stop()


if __name__ == "__main__":
    main()
