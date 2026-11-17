"""datasportsapi 0.0.x: a minimal client for the DataSportsAPI REST API.

One authenticated GET that returns the parsed JSON envelope and raises
DataSportsAPIError for any non-2xx answer. No dependencies. The full SDK (a
typed method per endpoint, paging, retries, asyncio and the WebSocket stream)
arrives with 0.1.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Mapping

__version__ = "0.0.1"
__all__ = ["DataSportsAPI", "DataSportsAPIError", "__version__"]

DEFAULT_BASE_URL = "https://api.datasportsapi.com"


class DataSportsAPIError(Exception):
    """A non-2xx answer: the HTTP status, the API's error code and Retry-After."""

    def __init__(self, status: int, code: str, message: str, retry_after: float | None = None) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.retry_after = retry_after


def _query_value(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


class DataSportsAPI:
    """client = DataSportsAPI()  # reads DATASPORTSAPI_API_KEY"""

    def __init__(self, api_key: str | None = None, *, base_url: str | None = None, timeout: float = 30.0) -> None:
        key = api_key or os.environ.get("DATASPORTSAPI_API_KEY")
        if not key:
            raise ValueError("DataSportsAPI: pass api_key or set DATASPORTSAPI_API_KEY")
        self._key = key
        self._base = (base_url or os.environ.get("DATASPORTSAPI_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self._timeout = timeout

    def get(self, path: str, params: Mapping[str, Any] | None = None, **kwargs: Any) -> Any:
        """GET a path of the API, e.g. client.get("/v2/livescores", sport="football")."""
        query = {k: v for k, v in {**(params or {}), **kwargs}.items() if v is not None}
        url = self._base + (path if path.startswith("/") else "/" + path)
        if query:
            url += "?" + urllib.parse.urlencode({k: _query_value(v) for k, v in query.items()})
        request = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self._key}",
                "Accept": "application/json",
                "User-Agent": f"datasportsapi-python/{__version__}",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                error = (json.loads(raw) or {}).get("error") or {}
            except (ValueError, AttributeError):
                error = {}
            retry = e.headers.get("Retry-After")
            try:
                retry_after = float(retry) if retry is not None else None
            except ValueError:
                retry_after = None
            message = error.get("message") or raw[:200].decode("utf-8", "replace") or e.reason or f"HTTP {e.code}"
            raise DataSportsAPIError(e.code, error.get("code") or f"http_{e.code}", str(message), retry_after) from None
        return json.loads(raw) if raw else None
