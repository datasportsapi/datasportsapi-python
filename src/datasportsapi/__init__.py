"""datasportsapi: the official Python client for the DataSportsAPI sports data API.

    from datasportsapi import DataSportsAPI

    client = DataSportsAPI()  # reads DATASPORTSAPI_API_KEY
    live = client.livescores.list(sport="football")

A method per endpoint on a blocking client (DataSportsAPI) and on an asyncio
client (AsyncDataSportsAPI); <name>_pages iterators for paged lists; retries on
429 rate_limited and 503; the live WebSocket stream (client.stream). The
response types are in datasportsapi.types. No dependencies.

Docs: https://datasportsapi.com/sdks/python/
"""

from ._client import DEFAULT_BASE_URL, AsyncDataSportsAPI, DataSportsAPI
from ._errors import (
    APIConnectionError,
    AuthenticationError,
    BadRequestError,
    DataSportsAPIError,
    InternalServerError,
    NotFoundError,
    PermissionDeniedError,
    RateLimitError,
    ServiceUnavailableError,
    StreamClosedError,
)
from ._stream import AsyncStream, Stream
from ._version import __version__

__all__ = [
    "DataSportsAPI",
    "AsyncDataSportsAPI",
    "Stream",
    "AsyncStream",
    "DataSportsAPIError",
    "APIConnectionError",
    "StreamClosedError",
    "BadRequestError",
    "AuthenticationError",
    "PermissionDeniedError",
    "NotFoundError",
    "RateLimitError",
    "InternalServerError",
    "ServiceUnavailableError",
    "DEFAULT_BASE_URL",
    "__version__",
]
