import os
from time import perf_counter
from typing import Optional
from urllib.parse import quote

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pymongo.errors import PyMongoError

from activity import record_recent_torrent, touch_torrent
from auth import get_current_user
from content_policy import assert_allowed_text, filter_torrent_items
from models import TorrentItem, TorrentPayload, User, UserRecentTorrent

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
MFU_TRENDING_SIZE = int(os.getenv("MFU_TRENDING_SIZE", "20"))
SEARCH_HISTORY_CAPTURE = int(os.getenv("SEARCH_HISTORY_CAPTURE", "5"))


def build_magnet(info_hash: str, name: str) -> str:
    trackers = "&tr=".join(quote(t, safe="") for t in TRACKERS)
    return f"magnet:?xt=urn:btih:{info_hash}&dn={quote(name, safe='')}&tr={trackers}"


def fmt(t: dict) -> dict:
    info_hash = t.get("info_hash", t.get("hash", ""))
    name = t.get("name", "")
    return {
        "name": name,
        "size": t.get("size", "0"),
        "seeders": t.get("seeders", "0"),
        "leechers": t.get("leechers", "0"),
        "magnet": t.get("magnet") or build_magnet(info_hash, name),
        "hash": info_hash,
        "poster": t.get("poster", ""),
        "category": t.get("category", "0"),
        "site": t.get("site", "apibay"),
        "url": t.get("url", ""),
        "date": t.get("added", t.get("date", "")),
        "uploader": t.get("uploader"),
        "screenshot": t.get("screenshot", []),
        "files": t.get("files", []),
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


def _payload_from_item(item: dict) -> TorrentPayload:
    return TorrentPayload(
        name=item.get("name", ""),
        size=item.get("size", ""),
        seeders=item.get("seeders", "0"),
        leechers=item.get("leechers", "0"),
        magnet=item.get("magnet", ""),
        hash=item.get("hash", ""),
        poster=item.get("poster", ""),
        category=item.get("category", ""),
        site=item.get("site", ""),
        url=item.get("url", ""),
    )


def _normalized_category_value(category: Optional[str]) -> str | None:
    if not category or category.lower() == "all":
        return None
    return CATEGORY_MAP.get(category.lower(), category)


def _torrent_to_item(torrent: TorrentItem) -> dict:
    return {
        "name": torrent.name,
        "size": torrent.size,
        "seeders": torrent.seeders,
        "leechers": torrent.leechers,
        "magnet": torrent.magnet,
        "hash": torrent.hash,
        "poster": torrent.poster,
        "category": torrent.category,
        "site": torrent.site,
        "url": torrent.url,
        "date": str(int(torrent.added_at.timestamp())) if torrent.added_at else "",
        "uploader": None,
        "screenshot": [],
        "files": [],
    }


async def _capture_recent_history(user_id: str, items: list[dict], source: str = "search") -> None:
    for item in items[:SEARCH_HISTORY_CAPTURE]:
        torrent = await touch_torrent(_payload_from_item(item), hit_increment=1)
        if torrent is not None:
            await record_recent_torrent(user_id, str(torrent.id), source=source)


async def _db_trending_items(category: Optional[str]) -> list[dict]:
    category_value = _normalized_category_value(category)
    try:
        query = TorrentItem.find({"hit_count": {"$gt": 0}})
        if category_value is not None:
            query = query.find(TorrentItem.category == category_value)
        rows = await query.sort([("hit_count", -1), ("last_hit_at", -1), ("added_at", -1)]).limit(MFU_TRENDING_SIZE).to_list()
        return [_torrent_to_item(row) for row in rows]
    except PyMongoError:
        return []


async def _db_recent_items(user_id: str, category: Optional[str]) -> list[dict]:
    category_value = _normalized_category_value(category)
    try:
        recent_rows = await UserRecentTorrent.find(
            UserRecentTorrent.user_id == user_id
        ).sort([("last_seen_at", -1)]).limit(100).to_list()
    except PyMongoError:
        return []

    items: list[dict] = []
    for row in recent_rows:
        torrent = await TorrentItem.get(row.torrent_id)
        if not torrent:
            continue
        if category_value is not None and torrent.category != category_value:
            continue
        items.append(_torrent_to_item(torrent))
    return filter_torrent_items(items)


@router.get("/")
async def search(
    query: str = Query(..., min_length=1),
    category: Optional[str] = Query(
        "all", description="all|movies|tv|4k|music|games|apps|audio|video|other"
    ),
    user: User = Depends(get_current_user),
):
    started = perf_counter()
    assert_allowed_text(query, "query")
    cat_code = CATEGORY_MAP.get(category.lower(), "0")
    data = await fetch(f"{APIBAY_BASE}/q.php", {"q": query, "cat": cat_code})
    filtered = filter_torrent_items([fmt(item) for item in data])
    await _capture_recent_history(str(user.id), filtered, source="search")
    result = wrap(filtered)
    result["time"] = round(perf_counter() - started, 3)
    return result


@router.get("/trending")
async def trending(
    category: Optional[str] = Query("all"),
    user: User = Depends(get_current_user),
):
    data = await _db_trending_items(category)
    if not data:
        data = await _db_recent_items(str(user.id), category)
    return wrap([fmt(item) for item in data[:MFU_TRENDING_SIZE]])


@router.get("/recent")
async def recent(
    category: Optional[str] = Query("all"),
    user: User = Depends(get_current_user),
):
    data = await _db_recent_items(str(user.id), category)
    return wrap([fmt(item) for item in data])


@router.get("/top/movies")
async def top_movies(user: User = Depends(get_current_user)):
    data = await _db_trending_items("movies")
    if not data:
        data = await _db_recent_items(str(user.id), "movies")
    return wrap([fmt(item) for item in data[:MFU_TRENDING_SIZE]])


@router.get("/top/tv")
async def top_tv(user: User = Depends(get_current_user)):
    data = await _db_trending_items("tv")
    if not data:
        data = await _db_recent_items(str(user.id), "tv")
    return wrap([fmt(item) for item in data[:MFU_TRENDING_SIZE]])
