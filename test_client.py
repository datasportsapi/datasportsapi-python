"""python3 -I test_client.py: the client against a local fake API and a local
fake WebSocket server. Standard library only; no network beyond 127.0.0.1."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import pickle
import queue
import socket
import socketserver
import struct
import sys
import threading
import time
import unittest
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

import datasportsapi  # noqa: E402
from datasportsapi import (  # noqa: E402
    APIConnectionError,
    AsyncDataSportsAPI,
    AuthenticationError,
    BadRequestError,
    DataSportsAPI,
    DataSportsAPIError,
    InternalServerError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
    ServiceUnavailableError,
    StreamClosedError,
)
from datasportsapi import _stream, _ws  # noqa: E402

KEY = "test-key-not-real"
ENV = ("DATASPORTSAPI_API_KEY", "DATASPORTSAPI_BASE_URL")

# Fast reconnects and dead-socket detection for the tests.
_stream.BACKOFF_FIRST = 0.05
_stream.DEAD_GRACE = 0.3


# --------------------------------------------------------------- fake HTTP API


class FakeAPI(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def _handle(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        srv = self.server
        path, _, qs = self.path.partition("?")
        record = {
            "method": self.command,
            "path": path,
            "raw": self.path,
            "query": urllib.parse.parse_qs(qs, keep_blank_values=True),
            "headers": dict(self.headers.items()),
            "body": body,
        }
        with srv.lock:
            srv.log.append(record)
            queue_ = srv.routes.get((self.command, path))
            if queue_ is None:
                answer = (404, {"error": {"code": "not_found", "message": "no route in the fake"}}, {})
            elif callable(queue_):
                answer = queue_(record)
            else:
                answer = queue_.pop(0) if len(queue_) > 1 else queue_[0]
        status, payload, headers = (list(answer) + [{}])[:3]
        if isinstance(payload, (bytes, bytearray)):
            data = bytes(payload)
        else:
            data = json.dumps(payload).encode()
        if status == "sleep":
            time.sleep(payload)
            return
        self.send_response(status)
        if not isinstance(payload, (bytes, bytearray)):
            self.send_header("Content-Type", "application/json")
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = _handle


def start_http():
    srv = ThreadingHTTPServer(("127.0.0.1", 0), FakeAPI)
    srv.daemon_threads = True
    srv.lock = threading.Lock()
    srv.log = []
    srv.routes = {}
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


# --------------------------------------------------------- fake WebSocket server


def is_close(msg):
    return isinstance(msg, tuple) and msg[0] == "close"


class WSConn:
    """The server's side of one WebSocket connection."""

    def __init__(self, sock, path, headers):
        self.sock = sock
        self.path = path
        self.headers = headers
        self.buf = b""
        self.received = []

    def _read(self, n, deadline):
        while len(self.buf) < n:
            self.sock.settimeout(max(0.01, deadline - time.monotonic()))
            chunk = self.sock.recv(65536)
            if not chunk:
                raise EOFError
            self.buf += chunk
        out, self.buf = self.buf[:n], self.buf[n:]
        return out

    def recv_frame(self, timeout=3.0):
        deadline = time.monotonic() + timeout
        b0, b1 = self._read(2, deadline)
        n = b1 & 0x7F
        if n == 126:
            n = struct.unpack("!H", self._read(2, deadline))[0]
        elif n == 127:
            n = struct.unpack("!Q", self._read(8, deadline))[0]
        if not b1 & 0x80:
            raise AssertionError("client frame not masked")
        key = self._read(4, deadline)
        data = bytes(c ^ key[i % 4] for i, c in enumerate(self._read(n, deadline)))
        return b0 & 0x0F, data

    def recv(self, timeout=3.0):
        """The next text frame as JSON, ("close", code) for a close frame, ("pong", data) for a pong."""
        while True:
            op, data = self.recv_frame(timeout)
            if op == 0x1:
                msg = json.loads(data)
                self.received.append(msg)
                return msg
            if op == 0x8:
                code = struct.unpack("!H", data[:2])[0] if len(data) >= 2 else 1005
                self.received.append(("close", code))
                return ("close", code)
            if op == 0xA:
                self.received.append(("pong", data))
                return ("pong", data)

    def recv_until(self, pred, timeout=3.0):
        deadline = time.monotonic() + timeout
        while True:
            msg = self.recv(max(0.01, deadline - time.monotonic()))
            if pred(msg):
                return msg

    def frame(self, op, payload=b"", fin=True):
        n = len(payload)
        head = bytes([(0x80 if fin else 0) | op])
        if n < 126:
            head += bytes([n])
        elif n < 65536:
            head += bytes([126]) + struct.pack("!H", n)
        else:
            head += bytes([127]) + struct.pack("!Q", n)
        self.sock.sendall(head + payload)

    def send(self, obj):
        self.frame(0x1, json.dumps(obj).encode())

    def close(self, code=1000, reason=""):
        self.frame(0x8, struct.pack("!H", code) + reason.encode())
        try:
            self.recv_until(is_close, timeout=1.0)
        except (EOFError, OSError):
            pass


class WSHandler(socketserver.BaseRequestHandler):
    def handle(self):
        sock = self.request
        raw = b""
        while b"\r\n\r\n" not in raw:
            chunk = sock.recv(4096)
            if not chunk:
                return
            raw += chunk
        head, _, rest = raw.partition(b"\r\n\r\n")
        lines = head.decode().split("\r\n")
        path = lines[0].split(" ")[1]
        headers = {}
        for line in lines[1:]:
            k, _, v = line.partition(":")
            headers[k.strip().lower()] = v.strip()
        accept = base64.b64encode(hashlib.sha1((headers["sec-websocket-key"] + _ws.GUID).encode()).digest()).decode()
        srv = self.server
        if srv.bad_accept:
            accept = base64.b64encode(b"0" * 20).decode()
        sock.sendall(
            (
                "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            ).encode()
        )
        conn = WSConn(sock, path, headers)
        conn.buf = rest
        with srv.lock:
            index = len(srv.conns)
            srv.conns.append(conn)
        script = srv.scripts[min(index, len(srv.scripts) - 1)] if srv.scripts else None
        try:
            if script:
                script(conn)
            else:
                conn.recv_until(is_close, timeout=10)
        except (EOFError, OSError, AssertionError) as e:
            srv.errors.append(repr(e))
        finally:
            try:
                sock.close()
            except OSError:
                pass


class WSServer(socketserver.ThreadingTCPServer):
    daemon_threads = True
    allow_reuse_address = True


def start_ws():
    srv = WSServer(("127.0.0.1", 0), WSHandler)
    srv.lock = threading.Lock()
    srv.conns = []
    srv.scripts = []
    srv.errors = []
    srv.bad_accept = False
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def fixture(fid, home="Home", away="Away", score=(0, 0), status="inprogress"):
    return {
        "id": fid,
        "sport": "football",
        "status": {"type": status, "description": "1st half"},
        "home": {"id": fid * 10, "name": home},
        "away": {"id": fid * 10 + 1, "name": away},
        "homeScore": {"current": score[0], "period1": score[0]},
        "awayScore": {"current": score[1], "period1": score[1]},
    }


def welcome(conn, replay=False, heartbeat=30000):
    conn.send({"type": "welcome", "protocol": 5, "heartbeatMs": heartbeat, "maxChannels": 50, "replay": replay})


def take(stream, pred, limit=50):
    """Frames from the stream until pred(frame) holds."""
    seen = []
    for frame in stream:
        seen.append(frame)
        if pred(frame) or len(seen) >= limit:
            break
    return seen


# ---------------------------------------------------------------------- tests


class Base(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.http = start_http()
        cls.ws = start_ws()
        cls.base = f"http://127.0.0.1:{cls.http.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.http.shutdown()
        cls.http.server_close()
        cls.ws.shutdown()
        cls.ws.server_close()

    def setUp(self):
        self.saved_env = {k: os.environ.pop(k, None) for k in ENV}
        with self.http.lock:
            self.http.log.clear()
            self.http.routes.clear()
        self.ws.conns.clear()
        self.ws.scripts = []
        self.ws.errors.clear()
        self.ws.bad_accept = False
        self.tickets = 0
        self.route("POST", "/v2/stream/ticket", self.ticket)

    def tearDown(self):
        for k, v in self.saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def ticket(self, record):
        self.tickets += 1
        return (200, {"data": {"ticket": f"tk{self.tickets}", "url": f"ws://127.0.0.1:{self.ws.server_address[1]}/v2/stream", "expiresInSeconds": 30}})

    def route(self, method, path, *answers):
        self.http.routes[(method, path)] = answers[0] if len(answers) == 1 and callable(answers[0]) else list(answers)

    def client(self, **kw):
        kw.setdefault("base_url", self.base)
        return DataSportsAPI(KEY, **kw)

    def calls(self, path=None):
        return [r for r in self.http.log if path is None or r["path"] == path]


class ConfigTest(Base):
    def test_auth_headers(self):
        self.route("GET", "/v2/livescores", (200, {"data": []}))
        self.assertEqual(self.client().livescores.list(sport="football"), {"data": []})
        h = self.calls()[0]["headers"]
        self.assertEqual(h["Authorization"], f"Bearer {KEY}")
        self.assertEqual(h["Accept"], "application/json")
        self.assertEqual(h["User-Agent"], f"datasportsapi-python/{datasportsapi.__version__}")
        self.assertNotIn("Content-Type", h)
        self.assertEqual(datasportsapi.__version__, "0.2.0")

    def test_env_key_and_base_url(self):
        os.environ["DATASPORTSAPI_API_KEY"] = KEY
        os.environ["DATASPORTSAPI_BASE_URL"] = self.base + "///"
        self.route("GET", "/v2/sports", (200, {"data": [{"slug": "football"}]}))
        c = DataSportsAPI()
        self.assertEqual(c.base_url, self.base)
        c.sports.list()
        self.assertEqual(self.calls()[0]["headers"]["Authorization"], f"Bearer {KEY}")

    def test_defaults(self):
        c = DataSportsAPI(KEY)
        self.assertEqual((c.base_url, c.timeout, c.max_retries), ("https://api.datasportsapi.com", 30.0, 2))

    def test_missing_key(self):
        with self.assertRaises(ValueError):
            DataSportsAPI()
        with self.assertRaises(ValueError):
            AsyncDataSportsAPI(base_url=self.base)

    def test_base_url_must_be_https(self):
        for bad in ("http://api.example.com", "http://10.0.0.1:8080", "ftp://localhost", "api.datasportsapi.com", "https://user:pw@api.example.com"):
            with self.assertRaises(ValueError, msg=bad):
                DataSportsAPI(KEY, base_url=bad)
        for good in ("https://api.example.com/", "http://localhost:9000", "http://127.0.0.1", "http://[::1]:8080"):
            DataSportsAPI(KEY, base_url=good)

    def test_key_hidden(self):
        c = self.client()
        a = AsyncDataSportsAPI(KEY, base_url=self.base)
        for text in (repr(c), str(c), repr(a), repr(vars(c)), repr(c._core._key), repr(c.fixtures)):
            self.assertNotIn(KEY, text)
        with self.assertRaises(TypeError):
            pickle.dumps(c)
        with self.assertRaises(ValueError) as e:
            DataSportsAPI(KEY + "\r\nX-Evil: 1", base_url=self.base)
        self.assertNotIn(KEY, str(e.exception))

    def test_bad_settings(self):
        for kw in ({"timeout": 0}, {"timeout": -1}, {"max_retries": -1}, {"max_retries": 1.5}):
            with self.assertRaises(ValueError):
                self.client(**kw)


class RequestTest(Base):
    def test_path_and_query_encoding(self):
        self.route("GET", "/v2/rankings/fifa%20men%2Fa%3Fb%26c", (200, {"data": []}))
        self.client().rankings.get("fifa men/a?b&c")
        self.assertEqual(self.calls()[0]["raw"], "/v2/rankings/fifa%20men%2Fa%3Fb%26c")

        self.route("GET", "/v2/fixtures", (200, {"data": [], "meta": {"hasNextPage": False}}))
        self.client().fixtures.list(date="2026-11-26", sport="ice hockey", league=None, page=2, tz="Europe/Istanbul")
        q = self.calls("/v2/fixtures")[0]["query"]
        self.assertEqual(q, {"date": ["2026-11-26"], "sport": ["ice hockey"], "page": ["2"], "tz": ["Europe/Istanbul"]})

        self.route("GET", "/v2/sports", (200, {"data": []}))
        self.client().get("/v2/sports", live=True, other=False, n=1.5, skip=None)
        self.assertEqual(self.calls("/v2/sports")[0]["raw"], "/v2/sports?live=true&other=false&n=1.5")

    def test_site_samples(self):
        self.route("GET", "/v2/leagues/17/standings", (200, {"data": [], "meta": {"season": 61627}}))
        self.route("GET", "/v2/fixtures/211521/heatmap/7", (200, {"data": None}))
        c = self.client()
        table = c.leagues.standings(17, season=61627)["data"]
        self.assertEqual(table, [])
        self.assertEqual(self.calls()[0]["raw"], "/v2/leagues/17/standings?season=61627")
        c.fixtures.heatmap(211521, 7)
        self.assertEqual(self.calls()[1]["path"], "/v2/fixtures/211521/heatmap/7")

    def test_exports_create_json_body(self):
        self.route("POST", "/v2/exports", (202, {"data": {"id": "ex1", "status": "queued"}}))
        res = self.client().exports.create(league=17, season=61627)
        self.assertEqual(res["data"]["id"], "ex1")
        r = self.calls()[0]
        self.assertEqual(r["headers"]["Content-Type"], "application/json")
        self.assertEqual(json.loads(r["body"]), {"league": 17, "season": 61627})

    def test_every_error_class(self):
        table = [
            (400, BadRequestError, "bad_request"),
            (401, AuthenticationError, "unauthorized"),
            (403, PermissionDeniedError, "endpoint_not_in_plan"),
            (404, NotFoundError, "no_data"),
            (429, RateLimitError, "quota_exceeded"),
            (500, InternalServerError, "internal_error"),
            (503, ServiceUnavailableError, "upstream_unavailable"),
        ]
        c = self.client(max_retries=0)
        for status, cls, code in table:
            body = {"error": {"code": code, "message": f"m{status}", "upgradeUrl": "https://datasportsapi.com/pricing/"}}
            self.route("GET", "/v2/fixtures/1/momentum", (status, body, {"Retry-After": "7"}))
            with self.assertRaises(cls) as e:
                c.fixtures.momentum(1)
            err = e.exception
            self.assertIsInstance(err, DataSportsAPIError)
            self.assertEqual((err.status, err.code, err.message, err.retry_after), (status, code, f"m{status}", 7.0))
            self.assertEqual(err.upgrade_url, "https://datasportsapi.com/pricing/")
            self.assertIn(code, str(err))
        # A status outside the table → the base class; a body that is not JSON → its text.
        self.route("GET", "/v2/fixtures/1/momentum", (418, b"<html>teapot</html>"))
        with self.assertRaises(DataSportsAPIError) as e:
            c.fixtures.momentum(1)
        self.assertIs(type(e.exception), DataSportsAPIError)
        self.assertEqual((e.exception.status, e.exception.code, e.exception.retry_after), (418, "http_418", None))
        self.assertIn("teapot", e.exception.message)
        # Errors survive pickling (multiprocessing) and stay readable.
        again = pickle.loads(pickle.dumps(e.exception))
        self.assertEqual((again.status, again.code), (418, "http_418"))

    def test_key_never_in_errors(self):
        def echo(record):
            return (400, {"error": {"code": "bad_request", "message": "you sent " + record["headers"]["Authorization"]}})

        self.route("GET", "/v2/sports", echo)
        with self.assertRaises(BadRequestError) as e:
            self.client().sports.list()
        self.assertNotIn(KEY, str(e.exception))
        self.assertNotIn(KEY, repr(e.exception))

    def test_connection_errors(self):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        with self.assertRaises(APIConnectionError) as e:
            DataSportsAPI(KEY, base_url=f"http://127.0.0.1:{port}").sports.list()
        self.assertEqual((e.exception.status, e.exception.code), (0, "connection_error"))
        self.route("GET", "/v2/sports", ("sleep", 1.0))
        t = time.monotonic()
        with self.assertRaises(APIConnectionError) as e:
            self.client(timeout=0.2).sports.list()
        self.assertEqual(e.exception.code, "timeout")
        self.assertLess(time.monotonic() - t, 0.9)
        self.assertEqual(len(self.calls("/v2/sports")), 1)  # never retried

    def test_redirect_not_followed(self):
        other = start_http()
        try:
            other.routes[("GET", "/steal")] = [(200, {"data": "stolen"})]
            self.route("GET", "/v2/sports", (302, b"", {"Location": f"http://127.0.0.1:{other.server_port}/steal"}))
            with self.assertRaises(DataSportsAPIError) as e:
                self.client().sports.list()
            self.assertEqual((e.exception.status, e.exception.code), (302, "http_302"))
            self.assertEqual(other.log, [])
        finally:
            other.shutdown()
            other.server_close()


class RetryTest(Base):
    def test_rate_limited_is_retried_after_retry_after(self):
        self.route(
            "GET",
            "/v2/sports",
            (429, {"error": {"code": "rate_limited", "message": "slow down"}}, {"Retry-After": "0.2"}),
            (200, {"data": ["ok"]}),
        )
        t = time.monotonic()
        self.assertEqual(self.client().sports.list(), {"data": ["ok"]})
        self.assertGreaterEqual(time.monotonic() - t, 0.19)
        self.assertEqual(len(self.calls()), 2)

    def test_quota_exceeded_is_not_retried(self):
        self.route("GET", "/v2/sports", (429, {"error": {"code": "quota_exceeded", "message": "quota"}}, {"Retry-After": "0"}))
        with self.assertRaises(RateLimitError) as e:
            self.client().sports.list()
        self.assertEqual(e.exception.code, "quota_exceeded")
        self.assertEqual(len(self.calls()), 1)

    def test_max_retries(self):
        self.route("GET", "/v2/sports", (503, {"error": {"code": "upstream_unavailable", "message": "down"}}, {"Retry-After": "0"}))
        with self.assertRaises(ServiceUnavailableError):
            self.client().sports.list()
        self.assertEqual(len(self.calls()), 3)
        self.http.log.clear()
        with self.assertRaises(ServiceUnavailableError):
            self.client(max_retries=0).sports.list()
        self.assertEqual(len(self.calls()), 1)

    def test_long_retry_after_is_not_waited(self):
        self.route("GET", "/v2/sports", (429, {"error": {"code": "rate_limited", "message": "later"}}, {"Retry-After": "120"}))
        t = time.monotonic()
        with self.assertRaises(RateLimitError) as e:
            self.client().sports.list()
        self.assertEqual(e.exception.retry_after, 120.0)
        self.assertLess(time.monotonic() - t, 1.0)
        self.assertEqual(len(self.calls()), 1)

    def test_other_errors_not_retried(self):
        self.route("GET", "/v2/sports", (500, {"error": {"code": "internal_error", "message": "x"}}, {"Retry-After": "0"}))
        with self.assertRaises(InternalServerError):
            self.client().sports.list()
        self.assertEqual(len(self.calls()), 1)

    def test_backoff_schedule(self):
        core = self.client(max_retries=10)._core
        err = ServiceUnavailableError("down", code="upstream_unavailable")
        self.assertEqual([core.retry_delay(err, i) for i in range(6)], [1.0, 2.0, 4.0, 8.0, 8.0, 8.0])
        self.assertIsNone(self.client(max_retries=1)._core.retry_delay(err, 1))


class PagingTest(Base):
    def test_pages_by_page_with_max_pages(self):
        def page(record):
            n = int(record["query"].get("page", ["0"])[0])
            return (200, {"data": [n], "meta": {"type": "recent", "page": n, "hasNextPage": True}})

        self.route("GET", "/v2/teams/42/fixtures", page)
        pages = list(self.client().teams.fixtures_pages(42, type="recent", max_pages=3))
        self.assertEqual([p["data"] for p in pages], [[0], [1], [2]])
        self.assertEqual([r["query"].get("page") for r in self.calls()], [None, ["1"], ["2"]])
        self.assertTrue(all(r["query"]["type"] == ["recent"] for r in self.calls()))

    def test_pages_end_with_the_list(self):
        self.route(
            "GET",
            "/v2/referees/5/fixtures",
            (200, {"data": [1], "meta": {"page": 3, "hasNextPage": True}}),
            (200, {"data": [2], "meta": {"page": 4, "hasNextPage": False}}),
        )
        pages = list(self.client().referees.fixtures_pages(5, page=3))
        self.assertEqual(len(pages), 2)
        self.assertEqual(self.calls()[1]["query"]["page"], ["4"])

    def test_pages_by_cursor(self):
        self.route(
            "GET",
            "/v2/fixtures",
            (200, {"data": [1], "meta": {"page": 0, "hasNextPage": True, "nextCursor": "c2"}}),
            (200, {"data": [2], "meta": {"hasNextPage": True, "nextCursor": "c3"}}),
            (200, {"data": [3], "meta": {"hasNextPage": False, "nextCursor": None}}),
        )
        pages = list(self.client().fixtures.list_pages(date="2026-11-26", page=0))
        self.assertEqual([p["data"] for p in pages], [[1], [2], [3]])
        q = [r["query"] for r in self.calls()]
        self.assertEqual(q[0], {"date": ["2026-11-26"], "page": ["0"]})
        self.assertEqual(q[1], {"date": ["2026-11-26"], "cursor": ["c2"]})
        self.assertEqual(q[2], {"date": ["2026-11-26"], "cursor": ["c3"]})


class AsyncTest(Base):
    def test_async_surface(self):
        self.route("GET", "/v2/fixtures/211521", (200, {"data": fixture(211521)}))
        self.route(
            "GET",
            "/v2/search",
            (200, {"data": ["a"], "meta": {"page": 0, "hasNextPage": True}}),
            (200, {"data": ["b"], "meta": {"page": 1, "hasNextPage": False}}),
        )
        self.route("POST", "/v2/exports", (202, {"data": {"id": "ex2"}}))
        self.route(
            "GET",
            "/v2/sports",
            (429, {"error": {"code": "rate_limited", "message": "slow"}}, {"Retry-After": "0.05"}),
            (200, {"data": []}),
        )
        self.route("GET", "/v2/fixtures/9/momentum", (403, {"error": {"code": "endpoint_not_in_plan", "message": "upgrade"}}))

        async def main():
            async with AsyncDataSportsAPI(KEY, base_url=self.base) as client:
                self.assertNotIn(KEY, repr(client))
                fx = await client.fixtures.get(211521)
                self.assertEqual(fx["data"]["status"]["description"], "1st half")
                pages = [p async for p in client.search.query_pages(q="arsenal")]
                self.assertEqual([p["data"] for p in pages], [["a"], ["b"]])
                self.assertEqual((await client.exports.create(league=1, season=2, format="csv"))["data"]["id"], "ex2")
                self.assertEqual(await client.sports.list(), {"data": []})
                with self.assertRaises(PermissionDeniedError) as e:
                    await client.fixtures.momentum(9)
                self.assertEqual(e.exception.code, "endpoint_not_in_plan")

        asyncio.run(main())
        self.assertEqual(json.loads(self.calls("/v2/exports")[0]["body"]), {"league": 1, "season": 2, "format": "csv"})
        self.assertEqual(len(self.calls("/v2/sports")), 2)


class WebSocketClientTest(Base):
    def test_accept_header_is_checked(self):
        self.ws.bad_accept = True
        with self.assertRaises(_ws.HandshakeError):
            _ws.WebSocket.connect(f"ws://127.0.0.1:{self.ws.server_address[1]}/x", timeout=2, user_agent="t", allow_plain=True)

    def test_plain_ws_needs_local(self):
        with self.assertRaises(_ws.HandshakeError):
            _ws.WebSocket.connect("ws://127.0.0.1:1/x", timeout=2, user_agent="t", allow_plain=False)

    def test_masking_round_trip(self):
        key = b"\x01\x02\x03\x04"
        data = os.urandom(1001)
        self.assertEqual(_ws._mask(key, _ws._mask(key, data)), data)


class StreamTest(Base):
    def test_stream_state_and_protocol(self):
        got = queue.Queue()

        def script(conn):
            welcome(conn)
            subs = [conn.recv(), conn.recv()]
            got.put(("subs", subs))
            conn.send({"type": "snapshot", "channel": "livescores", "seq": 1, "ts": 1, "data": [fixture(1), fixture(2)]})
            conn.send({"type": "snapshot", "channel": "fixture:211521", "seq": 1, "ts": 1, "data": fixture(211521)})
            conn.send({"type": "delta", "channel": "livescores", "seq": 2, "ts": 2, "id": 1, "sport": "football", "changes": {"homeScore": {"current": 1}, "clock": None}})
            conn.send({"type": "delta", "channel": "livescores", "seq": 3, "ts": 3, "id": 2, "sport": "football", "changes": {"status": {"type": "finished"}}})
            conn.send({"type": "fixture", "channel": "livescores", "seq": 4, "ts": 4, "id": 3, "sport": "football", "data": fixture(3)})
            conn.send({"type": "delta", "channel": "fixture:211521", "seq": 2, "ts": 4, "id": 211521, "sport": "football", "changes": {"awayScore": {"current": 2}}})
            conn.send({"type": "pong", "ts": 5})
            conn.send({"type": "error", "channel": "fixture:211521", "ts": 5, "code": "channel_forbidden", "message": "x"})
            conn.send({"type": "statistics", "channel": "fixture:211521", "seq": 3, "ts": 6, "id": 211521, "sport": "football", "statistics": []})
            got.put(("unsub", conn.recv()))
            conn.send({"type": "unsubscribed", "channel": "fixture:211521", "ts": 7})
            got.put(("end", conn.recv_until(is_close)))

        self.ws.scripts = [script]
        client = self.client()
        frames = []

        def body():
            with client.stream(["livescores", "fixture:211521"]) as stream:
                frames.extend(take(stream, lambda f: f["type"] == "statistics"))
                live = stream.fixtures("livescores")
                self.assertEqual([f["id"] for f in live], [1, 3])
                self.assertEqual(live[0]["homeScore"], {"current": 1, "period1": 0})  # merged key by key
                self.assertIsNone(live[0]["clock"])
                self.assertEqual(live[0]["home"]["name"], "Home")
                one = stream.fixtures("fixture:211521")
                self.assertEqual(one[0]["awayScore"]["current"], 2)
                one[0]["id"] = -1  # a copy: the stream's own state is untouched
                self.assertEqual(stream.fixtures("fixture:211521")[0]["id"], 211521)
                stream.unsubscribe("fixture:211521")
                self.assertEqual(stream.fixtures("fixture:211521"), [])
                frames.extend(take(stream, lambda f: f["type"] == "unsubscribed"))

        self.run_with_watchdog(body)
        types = [f["type"] for f in frames]
        self.assertEqual(types[0], "welcome")
        self.assertNotIn("pong", types)
        self.assertIn("error", types)
        self.assertNotIn("resync", [m.get("type") for m in self.ws.conns[0].received if isinstance(m, dict)])
        msgs = dict(got.get(timeout=3) for _ in range(3))
        self.assertEqual(msgs["subs"], [{"type": "subscribe", "channel": "livescores"}, {"type": "subscribe", "channel": "fixture:211521"}])
        self.assertEqual(msgs["unsub"], {"type": "unsubscribe", "channel": "fixture:211521"})
        self.assertEqual(msgs["end"], ("close", 1000))
        # The ticket went through the REST path with the key; the socket carried only the ticket.
        ticket_call = self.calls("/v2/stream/ticket")[0]
        self.assertEqual((ticket_call["method"], ticket_call["headers"]["Authorization"]), ("POST", f"Bearer {KEY}"))
        conn = self.ws.conns[0]
        self.assertEqual(conn.path, "/v2/stream?ticket=tk1")
        self.assertNotIn(KEY, conn.path + json.dumps(conn.headers))
        self.assertNotIn("authorization", conn.headers)

    def run_with_watchdog(self, body, timeout=6.0):
        done = threading.Event()
        result = {}

        def runner():
            try:
                body()
            except BaseException as e:  # noqa: BLE001
                result["error"] = e
            finally:
                done.set()

        t = threading.Thread(target=runner, daemon=True)
        t.start()
        if not done.wait(timeout):
            self.fail("the stream test timed out")
        if "error" in result:
            raise result["error"]

    def test_gap_sends_resync(self):
        got = queue.Queue()

        def script(conn):
            welcome(conn)
            conn.recv()
            conn.send({"type": "snapshot", "channel": "livescores", "seq": 1, "ts": 1, "data": [fixture(1)]})
            conn.send({"type": "delta", "channel": "livescores", "seq": 3, "ts": 3, "id": 1, "sport": "football", "changes": {"homeScore": {"current": 5}}})
            conn.send({"type": "delta", "channel": "livescores", "seq": 4, "ts": 4, "id": 1, "sport": "football", "changes": {"homeScore": {"current": 6}}})
            got.put(conn.recv())
            conn.send({"type": "snapshot", "channel": "livescores", "seq": 10, "ts": 5, "data": [fixture(1, score=(7, 0))]})
            conn.recv_until(is_close)

        self.ws.scripts = [script]

        def body():
            with self.client().stream("livescores") as stream:
                frames = take(stream, lambda f: f["type"] == "snapshot" and f["seq"] == 10)
                self.assertEqual(stream.fixtures("livescores")[0]["homeScore"]["current"], 7)
                self.assertEqual(len([f for f in frames if f["type"] == "delta"]), 2)

        self.run_with_watchdog(body)
        self.assertEqual(got.get(timeout=3), {"type": "resync", "channel": "livescores"})
        # One resync per gap, not one per frame after it.
        sent = [m for m in self.ws.conns[0].received if isinstance(m, dict) and m["type"] == "resync"]
        self.assertEqual(len(sent), 1)

    def test_close_1012_reconnects_with_last_seq(self):
        def first(conn):
            welcome(conn, replay=True)
            conn.recv()
            conn.send({"type": "snapshot", "channel": "livescores", "seq": 41, "ts": 1, "data": [fixture(1)]})
            conn.send({"type": "delta", "channel": "livescores", "seq": 42, "ts": 2, "id": 1, "sport": "football", "changes": {"homeScore": {"current": 1}}})
            conn.close(1012, "server restarting; reconnect")

        def second(conn):
            welcome(conn, replay=True)
            sub = conn.recv()
            conn.send({"type": "delta", "channel": "livescores", "seq": 43, "ts": 3, "id": 1, "sport": "football", "changes": {"homeScore": {"current": 2}}})
            conn.send({"type": "subscribed", "channel": "livescores", "seq": 43, "ts": 3, "replayed": 1})
            conn.recv_until(is_close)
            self.second_sub = sub

        self.ws.scripts = [first, second]
        self.second_sub = None

        def body():
            with self.client().stream(["livescores"]) as stream:
                take(stream, lambda f: f["type"] == "subscribed")
                self.assertEqual(stream.fixtures("livescores")[0]["homeScore"]["current"], 2)

        t = time.monotonic()
        self.run_with_watchdog(body)
        self.assertLess(time.monotonic() - t, 3.0)
        deadline = time.monotonic() + 2
        while self.second_sub is None and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(self.second_sub, {"type": "subscribe", "channel": "livescores", "lastSeq": 42})
        self.assertEqual(self.tickets, 2)
        self.assertEqual([c.path for c in self.ws.conns], ["/v2/stream?ticket=tk1", "/v2/stream?ticket=tk2"])

    def test_close_1008_is_fatal(self):
        def script(conn):
            welcome(conn)
            conn.recv()
            conn.close(1008, "api key revoked")

        self.ws.scripts = [script]

        def body():
            with self.client().stream(["livescores"]) as stream:
                with self.assertRaises(StreamClosedError) as e:
                    for _ in stream:
                        pass
                self.assertEqual((e.exception.close_code, e.exception.reason), (1008, "api key revoked"))
                self.assertEqual(list(stream), [])  # the stream has ended

        self.run_with_watchdog(body)
        self.assertEqual(self.tickets, 1)

    def test_close_1008_with_reconnect_and_1013(self):
        def plan_changed(conn):
            welcome(conn)
            conn.recv()
            conn.close(1008, "plan changed; reconnect")

        def too_many(conn):
            welcome(conn)
            conn.recv()
            conn.close(1013, "connection limit")

        self.ws.scripts = [plan_changed, too_many]

        def body():
            with self.client().stream(["livescores"]) as stream:
                with self.assertRaises(StreamClosedError) as e:
                    list(stream)
                self.assertEqual(e.exception.close_code, 1013)

        self.run_with_watchdog(body)
        self.assertEqual(self.tickets, 2)

    def test_ticket_403_is_fatal(self):
        self.route("POST", "/v2/stream/ticket", (403, {"error": {"code": "ws_not_available", "message": "Business and up"}}))

        def body():
            with self.client().stream(["livescores"]) as stream:
                with self.assertRaises(PermissionDeniedError) as e:
                    next(iter(stream))
                self.assertEqual(e.exception.code, "ws_not_available")

        self.run_with_watchdog(body)
        self.assertEqual(self.ws.conns, [])

    def test_ticket_url_must_be_secure(self):
        self.route("POST", "/v2/stream/ticket", (200, {"data": {"ticket": "tk", "url": "ws://stream.example.com/v2/stream"}}))
        client = DataSportsAPI(KEY, base_url=self.base)
        client._core.local = False  # as if the base URL were a remote https one

        def body():
            with client.stream(["livescores"]) as stream:
                with self.assertRaises(StreamClosedError) as e:
                    list(stream)
                self.assertEqual(e.exception.code, "invalid_stream_url")

        self.run_with_watchdog(body)

    def test_heartbeat_ping_and_dead_socket(self):
        pings = queue.Queue()

        def silent(conn):
            welcome(conn, heartbeat=100)
            conn.recv()
            pings.put(conn.recv_until(lambda m: isinstance(m, dict) and m["type"] == "ping"))
            time.sleep(1.5)  # nothing more: the client must give up on this socket

        def second(conn):
            welcome(conn)
            conn.recv()
            conn.send({"type": "snapshot", "channel": "livescores", "seq": 1, "ts": 1, "data": []})
            conn.recv_until(is_close)

        self.ws.scripts = [silent, second]

        def body():
            with self.client().stream(["livescores"]) as stream:
                take(stream, lambda f: f["type"] == "snapshot")

        self.run_with_watchdog(body)
        self.assertEqual(pings.get(timeout=2), {"type": "ping"})
        self.assertEqual(self.tickets, 2)

    def test_fragments_pings_and_oversized_frames(self):
        def script(conn):
            welcome(conn)
            conn.recv()
            text = json.dumps({"type": "snapshot", "channel": "livescores", "seq": 1, "ts": 1, "data": [fixture(1)]}).encode()
            conn.frame(0x1, text[:10], fin=False)
            conn.frame(0x9, b"hb")  # a ping between fragments
            conn.frame(0x0, text[10:20], fin=False)
            conn.frame(0x0, text[20:], fin=True)
            conn.recv_until(lambda m: isinstance(m, tuple) and m[0] == "pong")
            # A frame announcing more than 16 MiB: the client closes with 1009.
            conn.sock.sendall(bytes([0x81, 127]) + struct.pack("!Q", 17 * 1024 * 1024))
            conn.recv_until(is_close)

        def after(conn):
            welcome(conn)
            conn.recv()
            conn.send({"type": "subscribed", "channel": "livescores", "seq": 1, "ts": 2, "replayed": 0})
            conn.recv_until(is_close)

        self.ws.scripts = [script, after]

        def body():
            with self.client().stream(["livescores"]) as stream:
                frames = take(stream, lambda f: f["type"] == "subscribed")
                self.assertEqual(frames[1]["data"][0]["id"], 1)

        self.run_with_watchdog(body)
        first = self.ws.conns[0].received
        self.assertIn(("pong", b"hb"), first)
        self.assertIn(("close", 1009), first)

    def test_subscribe_later(self):
        got = queue.Queue()

        def script(conn):
            welcome(conn)
            conn.recv()
            conn.send({"type": "snapshot", "channel": "livescores", "seq": 1, "ts": 1, "data": []})
            got.put(conn.recv())
            conn.send({"type": "snapshot", "channel": "league:17", "seq": 1, "ts": 2, "data": [fixture(5)]})
            conn.recv_until(is_close)

        self.ws.scripts = [script]

        def body():
            with self.client().stream(["livescores"]) as stream:
                take(stream, lambda f: f["type"] == "snapshot")
                stream.subscribe("league:17")
                take(stream, lambda f: f.get("channel") == "league:17")
                self.assertEqual([f["id"] for f in stream.fixtures("league:17")], [5])
                self.assertEqual(stream.channels, ["livescores", "league:17"])

        self.run_with_watchdog(body)
        self.assertEqual(got.get(timeout=2), {"type": "subscribe", "channel": "league:17"})

    def test_async_stream(self):
        def script(conn):
            welcome(conn)
            conn.recv()
            conn.send({"type": "snapshot", "channel": "livescores", "seq": 1, "ts": 1, "data": [fixture(1), fixture(2)]})
            conn.send({"type": "delta", "channel": "livescores", "seq": 2, "ts": 2, "id": 2, "sport": "football", "changes": {"status": {"type": "canceled"}}})
            conn.recv_until(is_close)

        self.ws.scripts = [script]

        async def main():
            async with AsyncDataSportsAPI(KEY, base_url=self.base) as client:
                async with client.stream(["livescores"]) as stream:
                    async for frame in stream:
                        if frame["type"] == "delta":
                            break
                    self.assertEqual([f["id"] for f in stream.fixtures("livescores")], [1])
                self.assertEqual([f async for f in stream], [])

        asyncio.run(asyncio.wait_for(main(), 6))
        self.assertEqual(self.ws.conns[0].received[-1], ("close", 1000))


if __name__ == "__main__":
    unittest.main(verbosity=1)
