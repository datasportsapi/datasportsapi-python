"""The live stream: tickets, subscriptions, sequence checks, reconnects and a
current copy of every fixture on the subscribed channels.

One I/O thread per stream owns the WebSocket. It mints a ticket, connects,
subscribes on welcome, sends heartbeats, asks for a resync when a seq is
skipped and reconnects when the socket drops. Frames reach the caller through
a queue; the fixture copies are updated as each frame is handed out, so
stream.fixtures() always matches the frames the caller has seen.
"""

from __future__ import annotations

import asyncio
import collections
import copy
import json
import queue
import random
import select
import socket
import threading
import time
import urllib.parse
from typing import TYPE_CHECKING, Any, Callable, Iterator

from ._errors import AuthenticationError, DataSportsAPIError, PermissionDeniedError, StreamClosedError
from ._ws import HandshakeError, WebSocket, WebSocketError

if TYPE_CHECKING:
    from ._client import _Core

__all__ = ["Stream", "AsyncStream"]

DEFAULT_HEARTBEAT = 30.0
DEAD_GRACE = 5.0  # dead after 2 x heartbeat + this, with nothing received
BACKOFF_FIRST = 1.0
BACKOFF_MAX = 30.0
CLOSE_WAIT = 2.0  # how long close() waits for the server's close frame
FINISHED = frozenset({"finished", "canceled", "postponed"})
LIST_PREFIXES = ("sport:", "league:", "team:")

Frame = dict  # a frame as the server sent it: {"type": ..., "channel": ..., ...}


class _End:
    pass


_END = _End()


class _Fatal:
    def __init__(self, error: BaseException) -> None:
        self.error = error


def _kind(channel: str) -> str | None:
    if channel == "livescores" or channel.startswith(LIST_PREFIXES):
        return "list"
    if channel.startswith("fixture:"):
        return "single"
    return None


def _merge(target: dict, changes: dict) -> None:
    """Objects merge key by key; any other value (null included) overwrites."""
    for key, value in changes.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _merge(target[key], value)
        else:
            target[key] = copy.deepcopy(value)


def _finished(fixture: dict) -> bool:
    status = fixture.get("status")
    return isinstance(status, dict) and status.get("type") in FINISHED


class _Fixtures:
    """The stream's copy of every fixture on the list and fixture channels."""

    def __init__(self) -> None:
        self.lists: dict[str, dict[Any, dict]] = {}
        self.single: dict[str, dict | None] = {}

    def drop(self, channel: str) -> None:
        self.lists.pop(channel, None)
        self.single.pop(channel, None)

    def apply(self, frame: dict) -> None:
        channel, kind = frame.get("channel"), None
        if isinstance(channel, str):
            kind = _kind(channel)
        if kind is None:
            return
        typ = frame.get("type")
        if kind == "list":
            held = self.lists.setdefault(channel, {})
            if typ == "snapshot":
                held.clear()
                data = frame.get("data")
                for fx in data if isinstance(data, list) else []:
                    if isinstance(fx, dict) and "id" in fx:
                        held[fx["id"]] = copy.deepcopy(fx)
            elif typ == "fixture":
                fx = frame.get("data")
                if isinstance(fx, dict):
                    held[fx.get("id", frame.get("id"))] = copy.deepcopy(fx)
            elif typ == "delta":
                fx = held.get(frame.get("id"))
                changes = frame.get("changes")
                if fx is not None and isinstance(changes, dict):
                    _merge(fx, changes)
                    if _finished(fx):
                        del held[frame.get("id")]
        else:
            if typ in ("snapshot", "fixture"):
                data = frame.get("data")
                self.single[channel] = copy.deepcopy(data) if isinstance(data, dict) else None
            elif typ == "delta":
                fx = self.single.get(channel)
                changes = frame.get("changes")
                if isinstance(fx, dict) and isinstance(changes, dict):
                    _merge(fx, changes)

    def get(self, channel: str) -> list[dict]:
        if channel in self.lists:
            return [copy.deepcopy(fx) for fx in self.lists[channel].values()]
        fx = self.single.get(channel)
        return [copy.deepcopy(fx)] if isinstance(fx, dict) else []


class _StreamIO:
    """The I/O thread: everything that touches the socket happens here."""

    def __init__(self, core: _Core, channels: list[str]) -> None:
        self.core = core
        self.channels = list(channels)
        self.lock = threading.Lock()
        self.seq: dict[str, int] = {}
        self.resyncing: set[str] = set()
        self.commands: collections.deque[dict] = collections.deque()
        self.closing = threading.Event()
        self.thread: threading.Thread | None = None
        self.sink: Callable[[Any], None] = lambda item: None
        self._wake_r: socket.socket | None = None
        self._wake_w: socket.socket | None = None

    # ---------------------------------------------------------- caller's side

    def start(self, sink: Callable[[Any], None]) -> None:
        self.sink = sink
        self._wake_r, self._wake_w = socket.socketpair()
        self._wake_r.setblocking(False)
        self._wake_w.setblocking(False)
        self.thread = threading.Thread(target=self._run, name="datasportsapi-stream", daemon=True)
        self.thread.start()

    def _wake(self) -> None:
        if self._wake_w is not None:
            try:
                self._wake_w.send(b"\0")
            except OSError:
                pass

    def subscribe(self, channel: str) -> None:
        with self.lock:
            if channel in self.channels:
                return
            self.channels.append(channel)
            self.commands.append({"type": "subscribe", "channel": channel})
        self._wake()

    def unsubscribe(self, channel: str) -> None:
        with self.lock:
            if channel not in self.channels:
                return
            self.channels.remove(channel)
            self.seq.pop(channel, None)
            self.resyncing.discard(channel)
            self.commands.append({"type": "unsubscribe", "channel": channel})
        self._wake()

    def subscribed(self, channel: Any) -> bool:
        with self.lock:
            return channel in self.channels

    def close(self) -> None:
        self.closing.set()
        self._wake()

    def join(self, timeout: float) -> None:
        if self.thread is not None and self.thread is not threading.current_thread():
            self.thread.join(timeout)

    # ------------------------------------------------------------ the thread

    def _run(self) -> None:
        try:
            self._loop()
        except Exception as e:  # noqa: BLE001 - a bug must end the stream, not hang it
            self.sink(_Fatal(StreamClosedError(f"the stream failed: {type(e).__name__}", code="stream_error")))
        finally:
            self.sink(_END)
            for s in (self._wake_r, self._wake_w):
                if s is not None:
                    s.close()

    def _backoff(self, delay: float) -> bool:
        """Waits about `delay` seconds; True when close() was called meanwhile."""
        return self.closing.wait(min(BACKOFF_MAX, delay * random.uniform(0.8, 1.2)))

    def _loop(self) -> None:
        delay = BACKOFF_FIRST
        while not self.closing.is_set():
            try:
                res = self.core.request("POST", "/v2/stream/ticket")
            except (AuthenticationError, PermissionDeniedError) as e:
                self.sink(_Fatal(e))
                return
            except DataSportsAPIError:
                if self._backoff(delay):
                    return
                delay = min(BACKOFF_MAX, delay * 2)
                continue
            data = res.get("data") if isinstance(res, dict) else None
            ticket = data.get("ticket") if isinstance(data, dict) else None
            url = data.get("url") if isinstance(data, dict) else None
            if not isinstance(ticket, str) or not ticket or not isinstance(url, str):
                self.sink(_Fatal(StreamClosedError("the ticket answer has no ticket or url", code="invalid_ticket")))
                return
            url = self._socket_url(url, ticket)
            if url is None:
                return
            try:
                ws = WebSocket.connect(url, timeout=self.core.timeout, user_agent=_user_agent(), allow_plain=self.core.local)
            except HandshakeError as e:
                if str(e).startswith("the stream URL"):
                    self.sink(_Fatal(StreamClosedError(str(e), code="invalid_stream_url")))
                    return
                if self._backoff(delay):
                    return
                delay = min(BACKOFF_MAX, delay * 2)
                continue
            except (OSError, WebSocketError):
                if self._backoff(delay):
                    return
                delay = min(BACKOFF_MAX, delay * 2)
                continue
            outcome, welcomed = self._session(ws)
            if welcomed:
                delay = BACKOFF_FIRST
            if outcome == "closed":
                return
            if outcome == "now":
                continue
            if self._backoff(delay):
                return
            delay = min(BACKOFF_MAX, delay * 2)

    def _socket_url(self, url: str, ticket: str) -> str | None:
        parts = urllib.parse.urlsplit(url)
        if not (parts.scheme == "wss" or (parts.scheme == "ws" and self.core.local)):
            self.sink(_Fatal(StreamClosedError("the stream URL must be wss://", code="invalid_stream_url")))
            return None
        if "ticket" in urllib.parse.parse_qs(parts.query, keep_blank_values=True):
            return url
        extra = urllib.parse.urlencode({"ticket": ticket})
        return urllib.parse.urlunsplit(parts._replace(query=f"{parts.query}&{extra}" if parts.query else extra))

    def _send(self, ws: WebSocket, frame: dict) -> None:
        ws.send_text(json.dumps(frame, separators=(",", ":")))

    def _session(self, ws: WebSocket) -> tuple[str, bool]:
        """Runs one connection. Returns ("closed" | "now" | "backoff", welcomed)."""
        heartbeat = DEFAULT_HEARTBEAT
        welcomed = False
        last_ping = time.monotonic()
        close_deadline: float | None = None
        wake = self._wake_r
        assert wake is not None
        try:
            while True:
                # Commands from the caller.
                while True:
                    try:
                        wake.recv(4096)
                    except (BlockingIOError, InterruptedError):
                        break
                    except OSError:
                        break
                with self.lock:
                    commands = list(self.commands)
                    self.commands.clear()
                if welcomed and close_deadline is None:
                    for c in commands:
                        self._send(ws, c)
                if self.closing.is_set() and close_deadline is None:
                    ws.send_close(1000)
                    close_deadline = time.monotonic() + CLOSE_WAIT

                # Every complete message in the buffer.
                while True:
                    msg = ws.next_message()
                    if msg is None:
                        break
                    if msg[0] == "close":
                        return self._on_close(msg[1], msg[2]), welcomed
                    if msg[0] != "text" or close_deadline is not None:
                        continue
                    try:
                        frame = json.loads(msg[1])
                    except ValueError:
                        continue
                    if not isinstance(frame, dict):
                        continue
                    typ = frame.get("type")
                    if typ == "pong":
                        continue
                    if typ == "welcome":
                        hb = frame.get("heartbeatMs")
                        heartbeat = hb / 1000.0 if isinstance(hb, (int, float)) and not isinstance(hb, bool) and hb > 0 else DEFAULT_HEARTBEAT
                        welcomed = True
                        last_ping = time.monotonic()
                        replay = frame.get("replay") is True
                        with self.lock:
                            self.commands.clear()  # the subscribes below cover them
                            self.resyncing.clear()
                            subs = []
                            for ch in self.channels:
                                sub: dict[str, Any] = {"type": "subscribe", "channel": ch}
                                if replay and ch in self.seq:
                                    sub["lastSeq"] = self.seq[ch]
                                subs.append(sub)
                        for sub in subs:
                            self._send(ws, sub)
                    else:
                        self._check_seq(ws, frame)
                    self.sink(frame)

                now = time.monotonic()
                if close_deadline is not None and now >= close_deadline:
                    return "closed", welcomed
                dead_at = ws.last_activity + 2 * heartbeat + DEAD_GRACE
                if now >= dead_at:
                    return ("closed" if self.closing.is_set() else "backoff"), welcomed
                if welcomed and close_deadline is None and now - last_ping >= heartbeat:
                    self._send(ws, {"type": "ping"})
                    last_ping = now
                if ws.pending():
                    ws.fill()
                    continue
                wait_until = dead_at
                if welcomed and close_deadline is None:
                    wait_until = min(wait_until, last_ping + heartbeat)
                if close_deadline is not None:
                    wait_until = min(wait_until, close_deadline)
                ready, _, _ = select.select([ws.sock, wake], [], [], max(0.0, wait_until - now))
                if ws.sock in ready:
                    ws.fill()
        except (OSError, WebSocketError):
            return ("closed" if self.closing.is_set() else "backoff"), welcomed
        finally:
            ws.abort()

    def _check_seq(self, ws: WebSocket, frame: dict) -> None:
        channel, seq, typ = frame.get("channel"), frame.get("seq"), frame.get("type")
        if not isinstance(channel, str) or not isinstance(seq, int) or isinstance(seq, bool):
            return
        resync = False
        with self.lock:
            if channel not in self.channels:
                return
            if typ in ("snapshot", "subscribed"):
                self.seq[channel] = seq
                self.resyncing.discard(channel)
                return
            last = self.seq.get(channel)
            if last is not None and seq > last + 1:
                if channel not in self.resyncing:
                    self.resyncing.add(channel)
                    resync = True
            elif last is None or seq > last:
                self.seq[channel] = seq
        if resync:
            self._send(ws, {"type": "resync", "channel": channel})

    def _on_close(self, code: int, reason: str) -> str:
        if self.closing.is_set():
            return "closed"
        if code == 1012:
            return "now"
        if code == 1008:
            if "reconnect" in reason.lower():
                return "now"
            self.sink(_Fatal(StreamClosedError(f"the server ended the stream (1008): {reason or 'policy violation'}", close_code=code, reason=reason)))
            return "closed"
        if code == 1013:
            self.sink(_Fatal(StreamClosedError(f"the server ended the stream (1013): {reason or 'too many connections'}", close_code=code, reason=reason)))
            return "closed"
        return "backoff"


def _user_agent() -> str:
    from ._client import USER_AGENT

    return USER_AGENT


class Stream:
    """The blocking live stream. Iterate it for frames; fixtures(channel) gives the
    current copy of a channel's fixtures with every delta applied.

    with client.stream(["livescores"]) as stream:
        for frame in stream:
            ...
    """

    def __init__(self, core: _Core, channels: list[str]) -> None:
        self._io = _StreamIO(core, channels)
        self._queue: queue.Queue[Any] = queue.Queue()
        self._fixtures = _Fixtures()
        self._started = False
        self._done = False

    def __repr__(self) -> str:
        return f"<Stream channels={self.channels!r}{' closed' if self._done else ''}>"

    @property
    def channels(self) -> list[str]:
        with self._io.lock:
            return list(self._io.channels)

    def _start(self) -> None:
        if not self._started and not self._done:
            self._started = True
            self._io.start(self._queue.put)

    def __enter__(self) -> Stream:
        self._start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def __iter__(self) -> Iterator[Frame]:
        self._start()
        return self

    def __next__(self) -> Frame:
        if self._done:
            raise StopIteration
        self._start()
        item = self._queue.get()
        if isinstance(item, _End):
            self._done = True
            raise StopIteration
        if isinstance(item, _Fatal):
            self._done = True
            raise item.error
        if self._io.subscribed(item.get("channel")):
            self._fixtures.apply(item)
        return item

    def subscribe(self, channel: str) -> None:
        """Adds a channel; its snapshot arrives as a frame."""
        self._io.subscribe(channel)

    def unsubscribe(self, channel: str) -> None:
        """Leaves a channel and forgets its fixtures."""
        self._io.unsubscribe(channel)
        self._fixtures.drop(channel)

    def fixtures(self, channel: str) -> list[dict]:
        """A copy of the channel's fixtures, deltas applied (list order; one or none for fixture:{id})."""
        return self._fixtures.get(channel)

    def close(self) -> None:
        """Closes the socket with a close handshake; the iteration then ends."""
        self._done = True
        if self._started:
            self._io.close()
            self._io.join(CLOSE_WAIT + 1.0)


class AsyncStream:
    """The asyncio live stream: async with, async for, and the same methods."""

    def __init__(self, core: _Core, channels: list[str]) -> None:
        self._io = _StreamIO(core, channels)
        self._fixtures = _Fixtures()
        self._queue: asyncio.Queue[Any] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._started = False
        self._done = False

    def __repr__(self) -> str:
        return f"<AsyncStream channels={self.channels!r}{' closed' if self._done else ''}>"

    @property
    def channels(self) -> list[str]:
        with self._io.lock:
            return list(self._io.channels)

    def _start(self) -> None:
        if self._started or self._done:
            return
        self._started = True
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue()
        self._io.start(self._put)

    def _put(self, item: Any) -> None:
        loop, q = self._loop, self._queue
        if loop is None or q is None:
            return
        try:
            loop.call_soon_threadsafe(q.put_nowait, item)
        except RuntimeError:
            pass  # the event loop is closed

    async def __aenter__(self) -> AsyncStream:
        self._start()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def __aiter__(self) -> AsyncStream:
        return self

    async def __anext__(self) -> Frame:
        if self._done:
            raise StopAsyncIteration
        self._start()
        assert self._queue is not None
        item = await self._queue.get()
        if isinstance(item, _End):
            self._done = True
            raise StopAsyncIteration
        if isinstance(item, _Fatal):
            self._done = True
            raise item.error
        if self._io.subscribed(item.get("channel")):
            self._fixtures.apply(item)
        return item

    def subscribe(self, channel: str) -> None:
        """Adds a channel; its snapshot arrives as a frame."""
        self._io.subscribe(channel)

    def unsubscribe(self, channel: str) -> None:
        """Leaves a channel and forgets its fixtures."""
        self._io.unsubscribe(channel)
        self._fixtures.drop(channel)

    def fixtures(self, channel: str) -> list[dict]:
        """A copy of the channel's fixtures, deltas applied (list order; one or none for fixture:{id})."""
        return self._fixtures.get(channel)

    async def close(self) -> None:
        """Closes the socket with a close handshake; the iteration then ends."""
        self._done = True
        if self._started:
            self._io.close()
            await asyncio.to_thread(self._io.join, CLOSE_WAIT + 1.0)
