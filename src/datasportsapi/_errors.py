"""The exceptions the client raises: one class per HTTP status the API uses."""

from __future__ import annotations

from typing import Any

__all__ = [
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
]


class DataSportsAPIError(Exception):
    """Any failure of a call.

    status       the HTTP status (0 when no answer arrived)
    code         the API's error code, e.g. "endpoint_not_in_plan"; "http_<status>" when
                 the answer carried none
    message      a sentence for people; write your handling against code instead
    retry_after  seconds from a numeric Retry-After header, else None
    upgrade_url  where to upgrade the plan, when the API sends one
    """

    status: int = 0

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        code: str | None = None,
        retry_after: float | None = None,
        upgrade_url: str | None = None,
    ) -> None:
        super().__init__(message)
        if status is not None:
            self.status = status
        self.code = code or f"http_{self.status}"
        self.message = message
        self.retry_after = retry_after
        self.upgrade_url = upgrade_url

    def __str__(self) -> str:
        return f"{self.status} {self.code}: {self.message}"

    def __repr__(self) -> str:
        return f"{type(self).__name__}(status={self.status}, code={self.code!r}, message={self.message!r})"

    def __reduce__(self) -> Any:
        return (_rebuild, (type(self), self.message, self.status, self.code, self.retry_after, self.upgrade_url))


def _rebuild(cls: type, message: str, status: int, code: str, retry_after: float | None, upgrade_url: str | None) -> Any:
    return cls(message, status=status, code=code, retry_after=retry_after, upgrade_url=upgrade_url)


class APIConnectionError(DataSportsAPIError):
    """No answer: the connection failed (code "connection_error") or timed out ("timeout")."""

    status = 0


class StreamClosedError(DataSportsAPIError):
    """The live stream ended for good: the server closed it with a code that rules out a
    reconnect (1008 with a revoked key or a lost plan, 1013 over the connection limit), or
    it sent a ticket URL the client will not open. close_code and reason are the server's."""

    status = 0

    def __init__(self, message: str, *, close_code: int = 0, reason: str = "", code: str = "stream_closed") -> None:
        super().__init__(message, status=0, code=code)
        self.close_code = close_code
        self.reason = reason

    def __reduce__(self) -> Any:
        return (_rebuild_stream, (self.message, self.close_code, self.reason, self.code))


def _rebuild_stream(message: str, close_code: int, reason: str, code: str) -> StreamClosedError:
    return StreamClosedError(message, close_code=close_code, reason=reason, code=code)


class BadRequestError(DataSportsAPIError):
    """400: a parameter is missing or malformed (bad_request)."""

    status = 400


class AuthenticationError(DataSportsAPIError):
    """401: the key is missing, unknown or revoked (unauthorized)."""

    status = 401


class PermissionDeniedError(DataSportsAPIError):
    """403: the plan does not include this endpoint or feature (endpoint_not_in_plan, …)."""

    status = 403


class NotFoundError(DataSportsAPIError):
    """404: not_found, no_data or not_supported."""

    status = 404


class RateLimitError(DataSportsAPIError):
    """429: rate_limited (retried for you), quota_exceeded or export_limit (never retried)."""

    status = 429


class InternalServerError(DataSportsAPIError):
    """500: internal_error."""

    status = 500


class ServiceUnavailableError(DataSportsAPIError):
    """503: the data source is unavailable for the moment (retried for you)."""

    status = 503


_BY_STATUS: dict[int, type[DataSportsAPIError]] = {
    cls.status: cls
    for cls in (
        BadRequestError,
        AuthenticationError,
        PermissionDeniedError,
        NotFoundError,
        RateLimitError,
        InternalServerError,
        ServiceUnavailableError,
    )
}


def error_for_status(status: int) -> type[DataSportsAPIError]:
    """The class for an HTTP status; DataSportsAPIError for any status outside the table."""
    return _BY_STATUS.get(status, DataSportsAPIError)
