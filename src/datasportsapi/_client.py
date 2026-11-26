"""The clients: configuration, the HTTP transport, retries and paging.

The transport is urllib with an opener that never follows a redirect, so the
Authorization header can only ever reach the configured base URL. The asyncio
client runs the same transport in a worker thread (asyncio.to_thread) and waits
between retries with asyncio.sleep.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import math
import os
import socket
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, AsyncIterator, Iterable, Iterator, Mapping

from ._errors import APIConnectionError, DataSportsAPIError, error_for_status
from ._resources import _AsyncResources, _SyncResources
from ._stream import AsyncStream, Stream
from ._version import __version__

__all__ = ["DataSportsAPI", "AsyncDataSportsAPI", "DEFAULT_BASE_URL"]

DEFAULT_BASE_URL = "https://api.datasportsapi.com"
ENV_API_KEY = "DATASPORTSAPI_API_KEY"
ENV_BASE_URL = "DATASPORTSAPI_BASE_URL"
USER_AGENT = f"datasportsapi-python/{__version__}"

LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
MAX_RETRY_AFTER = 60.0
MAX_BACKOFF = 8.0


class _Secret:
    """Holds the API key so that no repr, vars() dump or pickle shows it."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def scrub(self, text: str) -> str:
        return text.replace(self._value, "[redacted]") if self._value else text

    def __repr__(self) -> str:
        return "<api key hidden>"

    __str__ = __repr__

    def __reduce__(self) -> Any:
        raise TypeError("the API key cannot be pickled")


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Turns every 3xx into an HTTPError: the request and its key go nowhere else."""

    def redirect_request(self, req: Any, fp: Any, code: int, msg: str, headers: Any, newurl: str) -> None:
        return None


def _check_key(key: Any) -> str:
    if not isinstance(key, str) or not key:
        raise ValueError(f"DataSportsAPI: pass api_key or set {ENV_API_KEY}")
    if any(not (0x21 <= ord(c) <= 0x7E) for c in key):
        # The message must not repeat the key.
        raise ValueError("DataSportsAPI: api_key holds characters an HTTP header cannot carry")
    return key


def _check_base_url(url: Any) -> tuple[str, bool]:
    """The base URL without trailing slashes, and whether it is a local one."""
    if not isinstance(url, str) or not url.strip():
        raise ValueError("DataSportsAPI: base_url must be a non-empty string")
    url = url.strip().rstrip("/")
    try:
        parts = urllib.parse.urlsplit(url)
        host = (parts.hostname or "").lower()
        parts.port  # noqa: B018 - raises ValueError on a malformed port
    except ValueError:
        raise ValueError("DataSportsAPI: base_url is not a valid URL") from None
    if not host:
        raise ValueError("DataSportsAPI: base_url has no host")
    if parts.username is not None or parts.password is not None:
        raise ValueError("DataSportsAPI: base_url must not carry credentials")
    if parts.query or parts.fragment:
        raise ValueError("DataSportsAPI: base_url must not carry a query or a fragment")
    local = host in LOCAL_HOSTS
    if parts.scheme == "https":
        return url, local
    if parts.scheme == "http" and local:
        return url, True
    raise ValueError("DataSportsAPI: base_url must be https:// (http:// only for localhost, 127.0.0.1 and [::1])")


def _query_value(v: Any) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        if not math.isfinite(v):
            raise ValueError("DataSportsAPI: a query value is not a finite number")
        if v.is_integer() and abs(v) < 1e16:
            return str(int(v))
        text = repr(v)
        if "e" in text or "E" in text:
            text = format(v, "f").rstrip("0").rstrip(".")
        return text
    return str(v)


def _query_string(query: Mapping[str, Any] | None) -> str:
    if not query:
        return ""
    pairs = [(k, _query_value(v)) for k, v in query.items() if v is not None]
    return urllib.parse.urlencode(pairs) if pairs else ""


def _retry_after(headers: Any) -> float | None:
    if headers is None:
        return None
    raw = headers.get("Retry-After")
    if raw is None:
        return None
    raw = raw.strip()
    try:
        value = float(raw)
    except ValueError:
        return None  # an HTTP date: not a number of seconds
    if not math.isfinite(value) or value < 0:
        return None
    return value


class _Core:
    """Configuration and the transport shared by the sync and async clients."""

    __slots__ = ("_key", "base_url", "local", "timeout", "max_retries", "_opener")

    def __init__(self, api_key: str | None, base_url: str | None, timeout: float, max_retries: int) -> None:
        self._key = _Secret(_check_key(api_key if api_key is not None else os.environ.get(ENV_API_KEY)))
        self.base_url, self.local = _check_base_url(base_url if base_url is not None else os.environ.get(ENV_BASE_URL) or DEFAULT_BASE_URL)
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not timeout > 0 or not math.isfinite(timeout):
            raise ValueError("DataSportsAPI: timeout must be a positive number of seconds")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("DataSportsAPI: max_retries must be a whole number, 0 or more")
        self.timeout = float(timeout)
        self.max_retries = max_retries
        handlers: list[Any] = [_NoRedirect()]
        if self.local:
            handlers.append(urllib.request.ProxyHandler({}))  # a local base URL never goes through a proxy
        self._opener = urllib.request.build_opener(*handlers)

    # ---------------------------------------------------------------- one attempt

    def url(self, path: str, query: Mapping[str, Any] | None = None) -> str:
        if not isinstance(path, str) or not path.startswith("/") or path.startswith("//"):
            raise ValueError("DataSportsAPI: a path starts with a single /")
        qs = _query_string(query)
        return self.base_url + path + ("?" + qs if qs else "")

    def send(self, method: str, path: str, query: Mapping[str, Any] | None = None, body: Mapping[str, Any] | None = None) -> Any:
        """One attempt: the parsed envelope, or an exception."""
        headers = {
            "Authorization": f"Bearer {self._key.reveal()}",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }
        data = None
        if body is not None:
            data = json.dumps({k: v for k, v in body.items() if v is not None}, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.url(path, query), data=data, headers=headers, method=method)
        try:
            with self._opener.open(request, timeout=self.timeout) as response:
                status = response.status
                raw = response.read()
        except urllib.error.HTTPError as e:
            try:
                raw = e.read()
            except Exception:  # noqa: BLE001 - the body is optional
                raw = b""
            finally:
                e.close()
            raise self._api_error(e.code, e.headers, raw, e.reason) from None
        except urllib.error.URLError as e:
            reason = e.reason
            if isinstance(reason, (TimeoutError, socket.timeout)):
                raise APIConnectionError(f"timed out after {self.timeout:g} s", code="timeout") from None
            raise APIConnectionError(self._key.scrub(f"could not reach the API: {reason}"), code="connection_error") from None
        except (TimeoutError, socket.timeout):
            raise APIConnectionError(f"timed out after {self.timeout:g} s", code="timeout") from None
        except (OSError, http.client.HTTPException) as e:
            raise APIConnectionError(self._key.scrub(f"connection failed: {type(e).__name__}: {e}"), code="connection_error") from None
        if not raw:
            return None
        try:
            return json.loads(raw)
        except ValueError:
            raise DataSportsAPIError("the API answered with something that is not JSON", status=status, code="invalid_response") from None

    def _api_error(self, status: int, headers: Any, raw: bytes, reason: Any) -> DataSportsAPIError:
        code = message = upgrade = None
        try:
            parsed = json.loads(raw) if raw else None
        except ValueError:
            parsed = None
        err = parsed.get("error") if isinstance(parsed, dict) else None
        if isinstance(err, dict):
            code = err.get("code") if isinstance(err.get("code"), str) and err.get("code") else None
            message = err.get("message") if isinstance(err.get("message"), str) else None
            upgrade = err.get("upgradeUrl") if isinstance(err.get("upgradeUrl"), str) else None
        if not message:
            text = raw[:200].decode("utf-8", "replace").strip() if raw and not isinstance(parsed, dict) else ""
            message = text or (str(reason) if reason else "") or f"HTTP {status}"
        cls = error_for_status(status)
        return cls(
            self._key.scrub(message),
            status=status,
            code=code or f"http_{status}",
            retry_after=_retry_after(headers),
            upgrade_url=upgrade,
        )

    # -------------------------------------------------------------------- retries

    def retry_delay(self, err: DataSportsAPIError, attempt: int) -> float | None:
        """Seconds to wait before attempt+1, or None when the error is final."""
        if attempt >= self.max_retries:
            return None
        if not ((err.status == 429 and err.code == "rate_limited") or err.status == 503):
            return None
        if err.retry_after is not None:
            return None if err.retry_after > MAX_RETRY_AFTER else err.retry_after
        return min(MAX_BACKOFF, 2.0**attempt)

    def request(self, method: str, path: str, query: Mapping[str, Any] | None = None, body: Mapping[str, Any] | None = None) -> Any:
        attempt = 0
        while True:
            try:
                return self.send(method, path, query, body)
            except APIConnectionError:
                raise
            except DataSportsAPIError as e:
                delay = self.retry_delay(e, attempt)
                if delay is None:
                    raise
            time.sleep(delay)
            attempt += 1

    async def arequest(self, method: str, path: str, query: Mapping[str, Any] | None = None, body: Mapping[str, Any] | None = None) -> Any:
        attempt = 0
        while True:
            try:
                return await asyncio.to_thread(self.send, method, path, query, body)
            except APIConnectionError:
                raise
            except DataSportsAPIError as e:
                delay = self.retry_delay(e, attempt)
                if delay is None:
                    raise
            await asyncio.sleep(delay)
            attempt += 1

    # --------------------------------------------------------------------- paging

    @staticmethod
    def next_query(query: dict[str, Any], res: Any, cursor: bool) -> dict[str, Any] | None:
        """The query of the page after `res`, or None when the list has ended."""
        meta = res.get("meta") if isinstance(res, dict) else None
        if not isinstance(meta, dict):
            return None
        next_cursor = meta.get("nextCursor")
        if cursor and isinstance(next_cursor, str) and next_cursor:
            q = {k: v for k, v in query.items() if k != "page"}
            q["cursor"] = next_cursor
            return q
        if meta.get("hasNextPage") is True:
            page = meta.get("page")
            if isinstance(page, bool) or not isinstance(page, int):
                page = query.get("page")
                page = page if isinstance(page, int) and not isinstance(page, bool) else 0
            q = {k: v for k, v in query.items() if k != "cursor"}
            q["page"] = page + 1
            return q
        return None

    def pages(self, method: str, path: str, query: Mapping[str, Any] | None, cursor: bool, max_pages: int | None) -> Iterator[Any]:
        q: dict[str, Any] | None = dict(query or {})
        count = 0
        while q is not None and (max_pages is None or count < max_pages):
            res = self.request(method, path, q)
            count += 1
            yield res
            q = self.next_query(q, res, cursor)

    async def apages(self, method: str, path: str, query: Mapping[str, Any] | None, cursor: bool, max_pages: int | None) -> AsyncIterator[Any]:
        q: dict[str, Any] | None = dict(query or {})
        count = 0
        while q is not None and (max_pages is None or count < max_pages):
            res = await self.arequest(method, path, q)
            count += 1
            yield res
            q = self.next_query(q, res, cursor)


def _check_max_pages(max_pages: int | None) -> None:
    if max_pages is not None and (isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 0):
        raise ValueError("DataSportsAPI: max_pages must be a whole number, 0 or more")


def _channels(channels: Iterable[str] | str) -> list[str]:
    if isinstance(channels, str):
        channels = [channels]
    out = list(dict.fromkeys(channels))
    for c in out:
        if not isinstance(c, str) or not c:
            raise ValueError("DataSportsAPI: a channel is a non-empty string, e.g. 'livescores' or 'fixture:211521'")
    return out


class DataSportsAPI(_SyncResources):
    """The blocking client.

    client = DataSportsAPI()  # reads DATASPORTSAPI_API_KEY

    api_key      your secret key; default: the DATASPORTSAPI_API_KEY environment variable
    base_url     default: DATASPORTSAPI_BASE_URL, else https://api.datasportsapi.com
    timeout      seconds for each attempt (30)
    max_retries  further attempts after a 429 rate_limited or a 503 (2)
    """

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 2,
    ) -> None:
        self._core = _Core(api_key, base_url, timeout, max_retries)
        self._init_resources()

    @property
    def base_url(self) -> str:
        return self._core.base_url

    @property
    def timeout(self) -> float:
        return self._core.timeout

    @property
    def max_retries(self) -> int:
        return self._core.max_retries

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self.base_url!r}, timeout={self.timeout!r}, max_retries={self.max_retries!r})"

    def __reduce__(self) -> Any:
        raise TypeError(f"{type(self).__name__} cannot be pickled: it holds an API key")

    def _request(self, method: str, path: str, query: Mapping[str, Any] | None = None, body: Mapping[str, Any] | None = None) -> Any:
        return self._core.request(method, path, query, body)

    def _pages(self, method: str, path: str, query: Mapping[str, Any] | None, cursor: bool, max_pages: int | None) -> Iterator[Any]:
        _check_max_pages(max_pages)
        return self._core.pages(method, path, query, cursor, max_pages)

    def get(self, path: str, params: Mapping[str, Any] | None = None, **query: Any) -> Any:
        """GET any path of the API, e.g. client.get("/v2/livescores", sport="football").

        The typed methods cover every endpoint; this stays for code written against 0.0.x.
        Query names are the API's own (camelCase).
        """
        return self._core.request("GET", path, {**(params or {}), **query})

    def stream(self, channels: Iterable[str] | str) -> Stream:
        """The live WebSocket stream (Business and up) for these channels.

        with client.stream(["livescores", "fixture:211521"]) as stream:
            for frame in stream:
                ...
        """
        return Stream(self._core, _channels(channels))

    def close(self) -> None:
        """Nothing to release (every request opens its own connection); here for symmetry."""

    def __enter__(self) -> DataSportsAPI:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


class AsyncDataSportsAPI(_AsyncResources):
    """The asyncio client: the same methods as DataSportsAPI, awaited.

    async with AsyncDataSportsAPI() as client:
        fixture = await client.fixtures.get(211521)
    """

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 2,
    ) -> None:
        self._core = _Core(api_key, base_url, timeout, max_retries)
        self._init_resources()

    @property
    def base_url(self) -> str:
        return self._core.base_url

    @property
    def timeout(self) -> float:
        return self._core.timeout

    @property
    def max_retries(self) -> int:
        return self._core.max_retries

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self.base_url!r}, timeout={self.timeout!r}, max_retries={self.max_retries!r})"

    def __reduce__(self) -> Any:
        raise TypeError(f"{type(self).__name__} cannot be pickled: it holds an API key")

    async def _request(self, method: str, path: str, query: Mapping[str, Any] | None = None, body: Mapping[str, Any] | None = None) -> Any:
        return await self._core.arequest(method, path, query, body)

    def _pages(self, method: str, path: str, query: Mapping[str, Any] | None, cursor: bool, max_pages: int | None) -> AsyncIterator[Any]:
        _check_max_pages(max_pages)
        return self._core.apages(method, path, query, cursor, max_pages)

    async def get(self, path: str, params: Mapping[str, Any] | None = None, **query: Any) -> Any:
        """GET any path of the API; see DataSportsAPI.get."""
        return await self._core.arequest("GET", path, {**(params or {}), **query})

    def stream(self, channels: Iterable[str] | str) -> AsyncStream:
        """The live WebSocket stream (Business and up).

        async with client.stream(["livescores"]) as stream:
            async for frame in stream:
                ...
        """
        return AsyncStream(self._core, _channels(channels))

    async def close(self) -> None:
        """Nothing to release (every request opens its own connection); here for async with."""

    async def __aenter__(self) -> AsyncDataSportsAPI:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()
