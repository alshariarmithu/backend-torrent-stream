from time import perf_counter
from typing import Optional
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from auth import get_current_user
from models import User

router = APIRouter(tags=["Search"])

TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.tracker.cl:1337/announce",
    "udp://9.rarbg.com:2810/announce",
    "udp://tracker.openbits.net:1337/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
]

APIBAY_BASE = "https://apibay.org"
CATEGORY_MAP = {
    "all": "0",
    "movies": "207",
    "tv": "208",
    "4k": "211",
    "music": "101",
    "games": "400",
    "apps": "300",
    "audio": "100",
    "video": "200",
    "other": "600",
}


def build_magnet(info_hash: str, name: str) -> str:
    trackers = "&tr=".join(quote(t, safe="") for t in TRACKERS)
    return f"magnet:?xt=urn:btih:{info_hash}&dn={quote(name, safe='')}&tr={trackers}"


def fmt(t: dict) -> dict:
    info_hash = t.get("info_hash", "")
    name = t.get("name", "")
    return {
        "name": name,
        "size": t.get("size", "0"),
        "seeders": t.get("seeders", "0"),
        "leechers": t.get("leechers", "0"),
        "magnet": build_magnet(info_hash, name),
        "hash": info_hash,
        "poster": "",
        "category": t.get("category", "0"),
        "site": "apibay",
        "url": "",
        "date": t.get("added", ""),
        "uploader": None,
        "screenshot": [],
        "files": [],
    }


async def fetch(url: str, params: dict | None = None) -> list[dict]:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list) and len(data) == 1 and data[0].get("id") == "0":
                return []
            return data if isinstance(data, list) else []
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"Torrent API error: {exc.response.text}",
        ) from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Torrent API unreachable: {exc}") from exc


def wrap(items: list[dict], page: int = 1) -> dict:
    return {
        "data": items,
        "current_page": page,
        "total_pages": 1,
        "total": len(items),
        "time": None,
    }


def _category_code(category: Optional[str]) -> str:
    return CATEGORY_MAP.get((category or "all").lower(), "0")


def _to_int(value: object) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def _rank_live_items(items: list[dict], mode: str) -> list[dict]:
    if mode == "recent":
        key = lambda item: (
            _to_int(item.get("added")),
            _to_int(item.get("seeders")),
            _to_int(item.get("leechers")),
        )
    else:
        key = lambda item: (
            _to_int(item.get("seeders")),
            _to_int(item.get("leechers")),
            _to_int(item.get("added")),
        )

    return sorted(items, key=key, reverse=True)


async def fetch_live_category_feed(category: Optional[str], mode: str, limit: int = 100) -> list[dict]:
    cat_code = _category_code(category)
    data = await fetch(f"{APIBAY_BASE}/q.php", {"q": f"category:{cat_code}"})
    return _rank_live_items(data, mode)[:limit]


@router.get("/")
async def search(
    query: str = Query(..., min_length=1),
    category: Optional[str] = Query(
        "all", description="all|movies|tv|4k|music|games|apps|audio|video|other"
    ),
    _user: User = Depends(get_current_user),
):
    started = perf_counter()
    cat_code = CATEGORY_MAP.get(category.lower(), "0")
    data = await fetch(f"{APIBAY_BASE}/q.php", {"q": query, "cat": cat_code})
    result = wrap([fmt(item) for item in data])
    result["time"] = round(perf_counter() - started, 3)
    return result


@router.get("/trending")
async def trending(
    category: Optional[str] = Query("all"),
    _user: User = Depends(get_current_user),
):
    data = await fetch_live_category_feed(category, mode="trending")
    return wrap([fmt(item) for item in data])


@router.get("/recent")
async def recent(_user: User = Depends(get_current_user)):
    data = await fetch_live_category_feed("all", mode="recent")
    return wrap([fmt(item) for item in data])


@router.get("/top/movies")
async def top_movies(_user: User = Depends(get_current_user)):
    data = await fetch_live_category_feed("movies", mode="trending")
    return wrap([fmt(item) for item in data])


@router.get("/top/tv")
async def top_tv(_user: User = Depends(get_current_user)):
    data = await fetch_live_category_feed("tv", mode="trending")
    return wrap([fmt(item) for item in data])
