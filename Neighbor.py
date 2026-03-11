from __future__ import annotations

import socket
import struct
import threading
from typing import Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from Node import Node  # noqa: F401


HANDSHAKE_HEADER = b"P2PFILESHARINGPROJ"  # 18 bytes
HANDSHAKE_LEN = 32

# Framing (your current rule):
#   4 bytes: message length (big-endian uint32) -> counts type(1) + body(N)
#   1 byte : message type
#   N bytes: body, where N = (message_length - 1)
FRAME_LEN_PREFIX = 4

MSG_CHOKE = 0
MSG_UNCHOKE = 1
MSG_INTERESTED = 2
MSG_NOT_INTERESTED = 3
MSG_HAVE = 4          # body: 4 bytes (your bitfield per your latest instruction)
MSG_BITFIELD = 5      # body: 4 bytes (your bitfield per your latest instruction)

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
    """
    Wraps a single TCP connection to a neighbor.

    New behavior requested:
      - If we receive msg type 4 or 5 with body size 4:
          * treat the 4 body bytes as this neighbor's bitfield
          * call checkifinterested(bitfield) -> bool
          * if True, send Interested: msg_len=1, type=2, body=""
      - If we receive type 2: set self.interested = True
      - If we receive type 3: set self.interested = False

    Also keeps:
      - self.downloadsize updated when msg_type == 7 (stores msg_len, per your prior request)
    """

    def __init__(self, node: "Node", sock: socket.socket, addr: Tuple[str, int], outbound: bool):
        self.node = node
        self.sock = sock
        self.addr = addr
        self.outbound = outbound

        self.peer_id: Optional[int] = None

        self.downloadsize: int = 0
        self.interested: bool = False
        self.bitfield: bytes = b"\x00\x00\x00\x00"  # neighbor's latest bitfield (4 bytes)

        self._send_lock = threading.Lock()
        self._stop_evt = threading.Event()
        self._rx_thread = threading.Thread(
            target=self._rx_loop, name=f"NeighborRX-{addr[0]}:{addr[1]}", daemon=True
        )

    # -----------------------------
    # Interest logic
    # -----------------------------

    def get_requestindex(self, their_bitfield: bytes) -> int:
        """
        Placeholder: decide which piece index to request next given the neighbor's bitfield.
        You can fill this in later.
        """
        _ = their_bitfield
        return -1

    def checkifinterested(self, their_bitfield: bytes) -> bool:
        """
        Returns True if *we* are interested in the neighbor (they have something we don't).

        Since your bitfields are 4 bytes (=32 bits), simplest check:
          interested if (their_bits & ~my_bits) != 0

        Uses node.PiecesIHave (4 bytes).
        """
        if len(their_bitfield) != 4:
            return False
        my_bits = int.from_bytes(self.node.PiecesIHave, "big", signed=False)
        their_bits = int.from_bytes(their_bitfield, "big", signed=False)
        return (their_bits & (~my_bits & 0xFFFFFFFF)) != 0

    # -----------------------------
    # Public API
    # -----------------------------

    def reset_preferences(self) -> None:
        self.downloadsize = 0

    def do_handshake(self) -> int:
        """
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
        """
        Sends framed message:
          msg_len = 1 + len(body)
          [4-byte msg_len][1-byte type][body...]
        """
        if not (0 <= msg_type <= 255):
            raise ValueError("msg_type must be 0..255")
        if len(body) > 0xFFFFFFFF - 1:
            raise ValueError("body too large for uint32 length prefix")

        msg_len = 1 + len(body)
        frame = struct.pack("!I", msg_len) + bytes([msg_type]) + body
        self._send_raw(frame)

    def send_interested(self) -> None:
        # “message of size 0 and type 2” in your framing means: body_len=0, so msg_len=1
        self.send_packet(MSG_INTERESTED, b"")

    def send_not_interested(self) -> None:
        self.send_packet(MSG_NOT_INTERESTED, b"")

    # -----------------------------
    # Internal helpers
    # -----------------------------

    def _send_raw(self, b: bytes) -> None:
        with self._send_lock:
            self.sock.sendall(b)

    def _handle_bitfield_like(self, msg_type: int, body: bytes) -> None:
        """
        For msg_type 4 or 5 with body size 4:
          - store bitfield
          - check interest; if True, send Interested
        """
        if len(body) != 4:
            return

        self.bitfield = body

        if self.checkifinterested(body):
            # If we're interested, notify them
            self.send_interested()

    def _rx_loop(self) -> None:
        try:
            while not self._stop_evt.is_set():
                # length includes type byte
                length_b = _recv_exact(self.sock, FRAME_LEN_PREFIX)
                (msg_len,) = struct.unpack("!I", length_b)
                if msg_len < 1:
                    raise ConnectionError(f"Invalid message length {msg_len} (must be >= 1)")

                msg_type_b = _recv_exact(self.sock, 1)
                msg_type = msg_type_b[0]

                body_len = msg_len - 1
                body = _recv_exact(self.sock, body_len) if body_len > 0 else b""

                if msg_type in (MSG_HAVE, MSG_BITFIELD) and body_len == 4:
                    self._handle_bitfield_like(msg_type, body)

                elif msg_type == MSG_INTERESTED:
                    self.interested = True

                elif msg_type == MSG_NOT_INTERESTED:
                    self.interested = False

                if msg_type == MSG_PIECE:
                    self.downloadsize = msg_len

                # Forward to node (keep existing signature)
                self.node.on_packet(self, msg_type_b + body)

        except Exception as e:
            self.node.on_neighbor_disconnected(self, e)
        finally:
            self.close()
