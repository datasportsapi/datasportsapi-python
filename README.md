# datasportsapi

The official Python client for [DataSportsAPI](https://datasportsapi.com): live scores,
fixtures, lineups, statistics, standings and more for 25 sports.

Every endpoint is a method, on a blocking client and on an asyncio client with the same
surface. Answers are plain dicts described by TypedDicts, so your editor knows the fields.
Paged lists come with iterators, rate-limit answers are retried for you, and Business plans
get the live WebSocket stream with a fixture copy that stays current. No dependencies;
Python 3.10 or newer.

- SDK guide: <https://datasportsapi.com/sdks/python/>
- API docs: <https://datasportsapi.com/docs/>

## Install

```bash
pip install datasportsapi
```

## Your key

Create a key in your dashboard (a free one at <https://datasportsapi.com/signup/>). The client
reads it from `DATASPORTSAPI_API_KEY` when you pass none:

```python
from datasportsapi import DataSportsAPI

client = DataSportsAPI()                      # DATASPORTSAPI_API_KEY
client = DataSportsAPI(api_key="sk_live_…")   # or in code
```

## First calls

Values from the URL path come first, in path order; the query follows as keyword
arguments named like the API's parameters, in snake_case. You get back the same
`{ "data": …, "meta": … }` envelope the HTTP API returns.

```python
live = client.livescores.list(sport="football")
for fixture in live["data"]:
    print(fixture["home"]["name"], fixture["homeScore"]["current"], "–",
          fixture["awayScore"]["current"], fixture["away"]["name"])

table = client.leagues.standings(17, season=61627)["data"]
lineups = client.fixtures.lineups(211521)["data"]
export = client.exports.create(league=17, season=61627, format="csv")
```

The types live in `datasportsapi.types` (`Fixture`, `TeamRef`, `LeaguesStandingsResponse`, …).
They describe the dicts and validate nothing at runtime: when the API adds a field, it simply
shows up in the dict.

## Errors

Each HTTP status has its own exception class, and all of them extend `DataSportsAPIError`.
Branch on `e.code`, the API's stable error code; `e.message` is written for people and may be
reworded.

```python
from datasportsapi import DataSportsAPIError, NotFoundError, PermissionDeniedError

try:
    client.fixtures.momentum(211521)
except PermissionDeniedError as e:   # 403: e.code == "endpoint_not_in_plan", …
    print(e.upgrade_url)
except NotFoundError as e:           # 404: not_found, no_data, not_supported
    ...
except DataSportsAPIError as e:      # anything else: e.status, e.code, e.message, e.retry_after
    ...
```

| Status | Class |
|---|---|
| 400 | `BadRequestError` |
| 401 | `AuthenticationError` |
| 403 | `PermissionDeniedError` |
| 404 | `NotFoundError` |
| 429 | `RateLimitError` |
| 500 | `InternalServerError` |
| 503 | `ServiceUnavailableError` |
| other | `DataSportsAPIError` |
| no answer | `APIConnectionError` (`status` 0, `code` `connection_error` or `timeout`) |

## Retries and settings

Two answers are worth another attempt: `429` with the code `rate_limited`, and `503`. The
client waits for the seconds in their `Retry-After` header (1, 2, 4 … up to 8 seconds when
there is none) and tries again, two more times by default. A `Retry-After` above 60 seconds is
not waited out: you get the error straight away. `429 quota_exceeded` is never retried (a
quota does not refill within seconds), and neither is any other status or a network failure.

```python
DataSportsAPI(
    api_key=...,      # or DATASPORTSAPI_API_KEY
    base_url=...,     # or DATASPORTSAPI_BASE_URL
    timeout=30.0,     # seconds, for each attempt
    max_retries=2,
)
```

## Paging

Each paged endpoint has a twin with a `_pages` suffix. It fetches page after page, by page
number or by cursor as the endpoint works, and yields every envelope until the list ends or
`max_pages` is reached.

```python
for page in client.teams.fixtures_pages(42, type="recent", max_pages=3):
    for fixture in page["data"]:
        ...
```

## asyncio

`AsyncDataSportsAPI` has every method of the blocking client; await the calls and use
`async for` with the `_pages` iterators.

```python
import asyncio
from datasportsapi import AsyncDataSportsAPI


async def main() -> None:
    async with AsyncDataSportsAPI() as client:
        fixture = await client.fixtures.get(211521)
        print(fixture["data"]["status"]["description"])
        async for page in client.search.query_pages(q="arsenal", max_pages=2):
            ...


asyncio.run(main())
```

## Live stream (Business and up)

`client.stream(channels)` opens the WebSocket and does the bookkeeping: it fetches a
single-use ticket, subscribes when the server says hello, asks for a resync when a sequence
number is skipped, and reconnects after a dropped socket or a server restart, replaying what
was missed where the plan allows. It also keeps its own copy of every fixture on the list and
fixture channels with each delta applied; `stream.fixtures(channel)` hands you a copy.

```python
with client.stream(["livescores", "fixture:211521"]) as stream:
    for frame in stream:
        if frame["type"] == "delta":
            print(frame["id"], frame["changes"])
        in_play = stream.fixtures("livescores")   # deltas applied, finished matches dropped
```

`stream.subscribe(channel)` and `stream.unsubscribe(channel)` change the channels while it
runs. With `AsyncDataSportsAPI`, use `async with client.stream([...]) as stream:` and
`async for frame in stream:`. When the server ends the stream for good (a revoked key, a plan
without the stream, too many connections), the loop raises once (`StreamClosedError`, or the
ticket's `AuthenticationError` / `PermissionDeniedError`) and the stream is over.

## Security

- The key travels only in the `Authorization` header, and only to the base URL you
  configured. It is never put in a URL, an error message or a `repr()`; the WebSocket URL
  carries a single-use ticket instead.
- The base URL must use `https://` (plain `http://` is accepted only for `localhost`,
  `127.0.0.1` and `[::1]`, for tests and local proxies).
- Redirects are never followed: a `3xx` answer becomes an error, so the key cannot be carried
  to another host.
- This is a server-side library. Keep a secret key out of browsers, mobile apps and anything
  else you ship to users.

## Help

Questions, ideas and what you built: the [developer Discord](https://datasportsapi.com/discord) (#python), or
[hello@datasportsapi.com](mailto:hello@datasportsapi.com). Guide: https://datasportsapi.com/sdks/python/

## License

MIT. © DataSportsAPI.
