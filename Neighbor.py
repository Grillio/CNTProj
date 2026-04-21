from __future__ import annotations

import socket
import struct
import threading
from typing import Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from Node import Node


HANDSHAKE_HEADER = b"P2PFILESHARINGPROJ"
HANDSHAKE_LEN = 32
FRAME_LEN_PREFIX = 4

MSG_CHOKE = 0
MSG_UNCHOKE = 1
MSG_INTERESTED = 2
MSG_NOT_INTERESTED = 3
MSG_HAVE = 4
MSG_BITFIELD = 5
MSG_REQUEST = 6
MSG_PIECE = 7


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
        raise ValueError("id must fit in uint32 (0..4294967295)")
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


class Neighbor:
    def __init__(self, node: "Node", sock: socket.socket, addr: Tuple[str, int], outbound: bool):
        self.node = node
        self.sock = sock
        self.addr = addr
        self.outbound = outbound

        self.peer_id: Optional[int] = None
        self.downloadsize: int = 0
        self.interested: bool = False
        self.bitfield: bytes = bytes(self.node.bitfield_len)

        self._send_lock = threading.Lock()
        self._stop_evt = threading.Event()
        self._rx_thread = threading.Thread(
            target=self._rx_loop,
            name=f"NeighborRX-{addr[0]}:{addr[1]}",
            daemon=True,
        )

    def reset_preferences(self) -> None:
        self.downloadsize = 0

    def do_handshake(self) -> int:
        self.sock.settimeout(10.0)
        if self.outbound:
            self._send_raw(build_handshake(self.node.my_id))
            peer_hs = _recv_exact(self.sock, HANDSHAKE_LEN)
        else:
            peer_hs = _recv_exact(self.sock, HANDSHAKE_LEN)
            self._send_raw(build_handshake(self.node.my_id))
        self.peer_id = parse_handshake(peer_hs)
        self.sock.settimeout(None)
        return self.peer_id

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

    def send_packet(self, msg_type: int, body: bytes = b"") -> None:
        if not (0 <= msg_type <= 255):
            raise ValueError("msg_type must be 0..255")
        if len(body) > 0xFFFFFFFF - 1:
            raise ValueError("body too large for uint32 length prefix")
        msg_len = 1 + len(body)
        frame = struct.pack("!I", msg_len) + bytes([msg_type]) + body
        self._send_raw(frame)

    def send_choke(self) -> None:
        self.send_packet(MSG_CHOKE, b"")

    def send_unchoke(self) -> None:
        self.send_packet(MSG_UNCHOKE, b"")

    def send_interested(self) -> None:
        self.send_packet(MSG_INTERESTED, b"")

    def send_not_interested(self) -> None:
        self.send_packet(MSG_NOT_INTERESTED, b"")

    def send_bitfield(self, bitfield: bytes) -> None:
        self.send_packet(MSG_BITFIELD, bitfield)

    def send_have(self, piece_index: int) -> None:
        self.send_packet(MSG_HAVE, struct.pack("!I", piece_index))

    def send_request(self, piece_index: int) -> None:
        self.send_packet(MSG_REQUEST, struct.pack("!I", piece_index))

    def send_piece(self, piece_index: int, piece_data: bytes) -> None:
        self.send_packet(MSG_PIECE, struct.pack("!I", piece_index) + piece_data)

    def _send_raw(self, b: bytes) -> None:
        with self._send_lock:
            self.sock.sendall(b)

    def _rx_loop(self) -> None:
        try:
            while not self._stop_evt.is_set():
                length_b = _recv_exact(self.sock, FRAME_LEN_PREFIX)
                (msg_len,) = struct.unpack("!I", length_b)
                if msg_len < 1:
                    raise ConnectionError(f"Invalid message length {msg_len} (must be >= 1)")
                msg_type_b = _recv_exact(self.sock, 1)
                body_len = msg_len - 1
                body = _recv_exact(self.sock, body_len) if body_len > 0 else b""
                self.node.on_packet(self, msg_type_b + body)
        except Exception as e:
            self.node.on_neighbor_disconnected(self, e)
        finally:
            self.close()
