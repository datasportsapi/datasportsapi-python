"""A small RFC 6455 WebSocket client on the standard library.

Client frames are masked; fragmented messages are joined; pings are answered;
the close handshake is honoured; the server's Sec-WebSocket-Accept is checked;
any frame or message over 16 MiB fails the connection (close 1009).

The socket belongs to one thread: the stream's I/O thread calls connect, fill,
next_message and every send. Reading is split in two so that a timeout never
loses half a frame: fill() appends whatever bytes arrived to a buffer, and
next_message() takes complete frames out of it.
"""

from __future__ import annotations

import base64
import hashlib
import os
import socket
import ssl
import struct
import time
import urllib.parse

__all__ = ["WebSocket", "WebSocketError", "HandshakeError", "MAX_MESSAGE"]

MAX_MESSAGE = 16 * 1024 * 1024
MAX_HANDSHAKE = 64 * 1024
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OP_CONT, OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG = 0x0, 0x1, 0x2, 0x8, 0x9, 0xA

class WebSocketError(Exception):
    """The connection failed; close_code is the code we closed it with, when we did."""

    def __init__(self, message: str, close_code: int = 1006) -> None:
        super().__init__(message)
        self.close_code = close_code


class HandshakeError(WebSocketError):
    """The server did not upgrade the connection (status is its HTTP status, 0 if none)."""

    def __init__(self, message: str, status: int = 0) -> None:
        super().__init__(message, 1006)
        self.status = status


def _mask(key: bytes, data: bytes) -> bytes:
    n = len(data)
    if not n:
        return b""
    stream = (key * (n // 4 + 1))[:n]
    return (int.from_bytes(data, "big") ^ int.from_bytes(stream, "big")).to_bytes(n, "big")


def accept_for(key: str) -> str:
    return base64.b64encode(hashlib.sha1((key + GUID).encode("ascii")).digest()).decode("ascii")


class WebSocket:
    def __init__(self, sock: socket.socket, buffered: bytes = b"") -> None:
        self.sock = sock
        self._buf = bytearray(buffered)
        self._frag_op: int | None = None
        self._frag = bytearray()
        self.close_sent = False
        self.close_received = False
        self.last_activity = time.monotonic()

    # ------------------------------------------------------------------ connect

    @classmethod
    def connect(cls, url: str, *, timeout: float, user_agent: str, allow_plain: bool) -> WebSocket:
        parts = urllib.parse.urlsplit(url)
        if parts.scheme == "wss":
            secure = True
        elif parts.scheme == "ws" and allow_plain:
            secure = False
        else:
            raise HandshakeError("the stream URL must be wss://")
        host = parts.hostname
        if not host:
            raise HandshakeError("the stream URL has no host")
        port = parts.port or (443 if secure else 80)
        target = (parts.path or "/") + ("?" + parts.query if parts.query else "")
        host_header = f"[{host}]" if ":" in host else host
        if parts.port and parts.port != (443 if secure else 80):
            host_header += f":{parts.port}"

        sock = socket.create_connection((host, port), timeout=timeout)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if secure:
                sock = ssl.create_default_context().wrap_socket(sock, server_hostname=host)
            key = base64.b64encode(os.urandom(16)).decode("ascii")
            request = (
                f"GET {target} HTTP/1.1\r\n"
                f"Host: {host_header}\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {key}\r\n"
                "Sec-WebSocket-Version: 13\r\n"
                f"User-Agent: {user_agent}\r\n"
                "\r\n"
            )
            sock.sendall(request.encode("ascii"))
            raw = b""
            while b"\r\n\r\n" not in raw:
                chunk = sock.recv(4096)
                if not chunk:
                    raise HandshakeError("the server closed the connection during the handshake")
                raw += chunk
                if len(raw) > MAX_HANDSHAKE:
                    raise HandshakeError("the handshake answer is too large")
            head, _, rest = raw.partition(b"\r\n\r\n")
            lines = head.decode("iso-8859-1").split("\r\n")
            status_line = lines[0].split(" ", 2)
            try:
                status = int(status_line[1])
            except (IndexError, ValueError):
                raise HandshakeError("the handshake answer is not HTTP") from None
            if status != 101:
                raise HandshakeError(f"the server answered the upgrade with HTTP {status}", status)
            headers: dict[str, str] = {}
            for line in lines[1:]:
                name, sep, value = line.partition(":")
                if sep:
                    headers[name.strip().lower()] = value.strip()
            if headers.get("upgrade", "").lower() != "websocket":
                raise HandshakeError("the server did not switch to the websocket protocol")
            if "upgrade" not in [t.strip().lower() for t in headers.get("connection", "").split(",")]:
                raise HandshakeError("the server did not upgrade the connection")
            if headers.get("sec-websocket-accept") != accept_for(key):
                raise HandshakeError("Sec-WebSocket-Accept does not match the key we sent")
            if headers.get("sec-websocket-extensions"):
                raise HandshakeError("the server enabled an extension we did not ask for")
            return cls(sock, rest)
        except BaseException:
            sock.close()
            raise

    # ---------------------------------------------------------------- sending

    def send_frame(self, opcode: int, payload: bytes = b"") -> None:
        n = len(payload)
        head = bytearray([0x80 | opcode])
        if n < 126:
            head.append(0x80 | n)
        elif n < 1 << 16:
            head.append(0x80 | 126)
            head += struct.pack("!H", n)
        else:
            head.append(0x80 | 127)
            head += struct.pack("!Q", n)
        key = os.urandom(4)
        self.sock.sendall(bytes(head) + key + _mask(key, payload))

    def send_text(self, text: str) -> None:
        self.send_frame(OP_TEXT, text.encode("utf-8"))

    def send_ping(self, payload: bytes = b"") -> None:
        self.send_frame(OP_PING, payload)

    def send_close(self, code: int = 1000, reason: str = "") -> None:
        if self.close_sent:
            return
        self.close_sent = True
        self.send_frame(OP_CLOSE, struct.pack("!H", code) + reason.encode("utf-8")[:123])

    def abort(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    # ---------------------------------------------------------------- reading

    def pending(self) -> int:
        """Bytes already decrypted by TLS and waiting (select() does not see them)."""
        return self.sock.pending() if isinstance(self.sock, ssl.SSLSocket) else 0

    def fill(self) -> None:
        """Reads once from the socket into the buffer; raises on EOF, errors and timeouts."""
        chunk = self.sock.recv(1 << 16)
        if not chunk:
            raise WebSocketError("the server closed the connection without a close frame", 1006)
        self._buf += chunk
        self.last_activity = time.monotonic()

    def _fail(self, code: int, message: str) -> None:
        try:
            self.send_close(code)
        except OSError:
            pass
        self.abort()
        raise WebSocketError(message, code)

    def _frame(self) -> tuple[bool, int, bytes] | None:
        b = self._buf
        if len(b) < 2:
            return None
        b0, b1 = b[0], b[1]
        fin, opcode, masked, n = bool(b0 & 0x80), b0 & 0x0F, b1 & 0x80, b1 & 0x7F
        if b0 & 0x70:
            self._fail(1002, "a frame set reserved bits")
        if masked:
            self._fail(1002, "the server masked a frame")
        i = 2
        if n == 126:
            if len(b) < 4:
                return None
            n, i = struct.unpack_from("!H", b, 2)[0], 4
        elif n == 127:
            if len(b) < 10:
                return None
            n, i = struct.unpack_from("!Q", b, 2)[0], 10
        if opcode >= 0x8 and (n > 125 or not fin):
            self._fail(1002, "a control frame is fragmented or too long")
        if n > MAX_MESSAGE:
            self._fail(1009, "a frame is larger than 16 MiB")
        if len(b) < i + n:
            return None
        payload = bytes(b[i : i + n])
        del b[: i + n]
        return fin, opcode, payload

    def next_message(self) -> tuple | None:
        """The next complete message from the buffer, or None until more bytes arrive.

        Pings are answered here; pongs are swallowed; a close frame is answered and
        returned as ("close", code, reason).
        """
        while True:
            frame = self._frame()
            if frame is None:
                return None
            fin, opcode, payload = frame
            if opcode == OP_PING:
                if not self.close_sent:
                    self.send_frame(OP_PONG, payload)
                continue
            if opcode == OP_PONG:
                continue
            if opcode == OP_CLOSE:
                self.close_received = True
                if len(payload) == 1:
                    self._fail(1002, "a close frame with a one-byte body")
                code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else 1005
                try:
                    reason = payload[2:].decode("utf-8")
                except UnicodeDecodeError:
                    reason = ""
                try:
                    self.send_close(code if code not in (1005, 1006, 1015) and code >= 1000 else 1000)
                except OSError:
                    pass
                return ("close", code, reason)
            if opcode in (OP_TEXT, OP_BINARY):
                if self._frag_op is not None:
                    self._fail(1002, "a new message began inside a fragmented one")
                if fin:
                    return self._message(opcode, payload)
                self._frag_op, self._frag = opcode, bytearray(payload)
                continue
            if opcode == OP_CONT:
                if self._frag_op is None:
                    self._fail(1002, "a continuation frame without a message")
                self._frag += payload
                if len(self._frag) > MAX_MESSAGE:
                    self._fail(1009, "a message is larger than 16 MiB")
                if fin:
                    op, data = self._frag_op, bytes(self._frag)
                    self._frag_op, self._frag = None, bytearray()
                    return self._message(op, data)
                continue
            self._fail(1002, f"unknown opcode {opcode}")

    def _message(self, opcode: int, payload: bytes) -> tuple:
        if opcode == OP_BINARY:
            return ("binary", payload)
        try:
            return ("text", payload.decode("utf-8"))
        except UnicodeDecodeError:
            self._fail(1007, "a text message is not valid UTF-8")
            raise  # unreachable: _fail raises
