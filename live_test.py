"""python3 -I live_test.py: a smoke test against the real API.

Runs only when DATASPORTSAPI_API_KEY is set (never in CI). It walks real ids
(livescores or today's fixtures → one fixture → its teams, league, lineups,
statistics and events; the league's standings; a search; a few pages of an
iterator) and checks that every answer decodes into a { data } envelope.
404 and 403 answers (no data for this fixture, not in this plan) are reported
as skipped, not failed. Uses a few dozen requests of the key's quota.
"""

from __future__ import annotations

import asyncio
import datetime
import os
import sys
from pathlib import Path
from typing import Any, Callable

sys.path.insert(0, str(Path(__file__).parent / "src"))

from datasportsapi import (  # noqa: E402
    AsyncDataSportsAPI,
    DataSportsAPI,
    DataSportsAPIError,
    NotFoundError,
    PermissionDeniedError,
)

passed: list[str] = []
skipped: list[str] = []
failed: list[str] = []


def check(name: str, call: Callable[[], Any]) -> Any:
    try:
        res = call()
    except (NotFoundError, PermissionDeniedError) as e:
        skipped.append(f"{name}: {e.status} {e.code}")
        return None
    except DataSportsAPIError as e:
        failed.append(f"{name}: {e.status} {e.code}: {e.message}")
        return None
    if not isinstance(res, dict) or "data" not in res:
        failed.append(f"{name}: the answer is not a {{ data }} envelope")
        return None
    passed.append(name)
    return res


def first_fixture(client: DataSportsAPI) -> dict | None:
    live = check("livescores.list", lambda: client.livescores.list(sport="football"))
    if live and live["data"]:
        return live["data"][0]
    today = datetime.date.today().isoformat()
    listed = check("fixtures.list", lambda: client.fixtures.list(date=today, sport="football"))
    if listed and listed["data"]:
        return listed["data"][0]
    return None


def main() -> int:
    if not os.environ.get("DATASPORTSAPI_API_KEY"):
        print("live_test: DATASPORTSAPI_API_KEY is not set; nothing to do.")
        return 0
    client = DataSportsAPI()
    print(f"live_test against {client.base_url}")

    check("sports.list", client.sports.list)
    fx = first_fixture(client)
    if fx is None:
        failed.append("no football fixture live or today to walk from")
    else:
        fid = fx["id"]
        check("fixtures.get", lambda: client.fixtures.get(fid))
        check("fixtures.lineups", lambda: client.fixtures.lineups(fid))
        check("fixtures.statistics", lambda: client.fixtures.statistics(fid))
        check("fixtures.events", lambda: client.fixtures.events(fid))
        for side in ("home", "away"):
            team = fx.get(side)
            if team:
                check(f"teams.get ({side})", lambda: client.teams.get(team["id"]))
        home = fx.get("home")
        if home:
            pages = 0
            try:
                for page in client.teams.fixtures_pages(home["id"], type="recent", max_pages=2):
                    if "data" not in page:
                        failed.append("teams.fixtures_pages: a page without data")
                    pages += 1
                passed.append(f"teams.fixtures_pages ({pages} pages)")
            except DataSportsAPIError as e:
                failed.append(f"teams.fixtures_pages: {e}")
            check("search.query", lambda: client.search.query(q=home["name"]))
        league, season = fx.get("league"), fx.get("season")
        if league:
            check("leagues.get", lambda: client.leagues.get(league["id"]))
            if season:
                check("leagues.standings", lambda: client.leagues.standings(league["id"], season=season["id"]))

    async def async_part() -> None:
        async with AsyncDataSportsAPI() as aclient:
            res = await aclient.sports.list()
            if isinstance(res, dict) and "data" in res:
                passed.append("async sports.list")
            else:
                failed.append("async sports.list: not an envelope")

    try:
        asyncio.run(async_part())
    except DataSportsAPIError as e:
        failed.append(f"async sports.list: {e}")

    for line in passed:
        print("  ok  ", line)
    for line in skipped:
        print("  skip", line)
    for line in failed:
        print("  FAIL", line)
    print(f"live_test: {len(passed)} passed, {len(skipped)} skipped, {len(failed)} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
