# datasportsapi

The official Python client for [DataSportsAPI](https://datasportsapi.com), a sports data API:
live scores, fixtures, lineups, statistics and standings for 25 sports.

> **Live API, minimal client.** The API answers today (free key: https://datasportsapi.com/signup/).
> This 0.0.x release is a minimal client: one authenticated `get()` with typed errors and no
> dependencies. The full SDK (a typed method for every endpoint, paging, retries, asyncio and
> the WebSocket stream) arrives with 0.1. Docs: <https://datasportsapi.com/docs/sdks/>

```bash
pip install datasportsapi
```

```python
from datasportsapi import DataSportsAPI, DataSportsAPIError

client = DataSportsAPI()  # reads DATASPORTSAPI_API_KEY

try:
    res = client.get("/v2/livescores", sport="football")
    print(res["data"])
except DataSportsAPIError as e:
    print(e.status, e.code, e.retry_after)
```

- `api_key` defaults to `DATASPORTSAPI_API_KEY`; `base_url` to `DATASPORTSAPI_BASE_URL`, then
  `https://api.datasportsapi.com`.
- Python 3.10+.
- Get a key at <https://datasportsapi.com/signup/>.

MIT licensed. © DataSportsAPI.
