"""python3 -I test_client.py — checks the client against a local fake API."""

import json
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))
from datasportsapi import DataSportsAPI, DataSportsAPIError  # noqa: E402

SEEN = []


class Fake(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        SEEN.append((self.path, self.headers.get("Authorization"), self.headers.get("User-Agent")))
        if self.path.startswith("/v2/sports"):
            self._send(200, {"data": [{"id": 1, "slug": "football"}]})
        elif self.path.startswith("/v2/limited"):
            self._send(429, {"error": {"code": "rate_limited", "message": "Too many requests"}}, {"Retry-After": "7"})
        else:
            body = b"<html>bad gateway</html>"
            self.send_response(502)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    def _send(self, status, obj, headers=None):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = HTTPServer(("127.0.0.1", 0), Fake)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}/"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()

    def test_get(self):
        c = DataSportsAPI("dsa_live_test", base_url=self.base)
        self.assertEqual(c.get("/v2/sports", sport="football", live=True, skip=None), {"data": [{"id": 1, "slug": "football"}]})
        path, auth, ua = SEEN[-1]
        self.assertEqual(path, "/v2/sports?sport=football&live=true")
        self.assertEqual(auth, "Bearer dsa_live_test")
        self.assertTrue(ua.startswith("datasportsapi-python/"))

    def test_errors(self):
        c = DataSportsAPI("dsa_live_test", base_url=self.base)
        with self.assertRaises(DataSportsAPIError) as e:
            c.get("v2/limited")
        self.assertEqual((e.exception.status, e.exception.code, e.exception.retry_after), (429, "rate_limited", 7.0))
        with self.assertRaises(DataSportsAPIError) as e:
            c.get("/v2/other")
        self.assertEqual((e.exception.status, e.exception.code, e.exception.retry_after), (502, "http_502", None))

    def test_env(self):
        os.environ.pop("DATASPORTSAPI_API_KEY", None)
        with self.assertRaises(ValueError):
            DataSportsAPI()
        os.environ["DATASPORTSAPI_API_KEY"] = "dsa_live_env"
        os.environ["DATASPORTSAPI_BASE_URL"] = self.base
        DataSportsAPI().get("/v2/sports")
        self.assertEqual(SEEN[-1][1], "Bearer dsa_live_env")


if __name__ == "__main__":
    unittest.main(verbosity=1)
