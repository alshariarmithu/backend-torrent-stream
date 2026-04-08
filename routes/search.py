# from time import perf_counter
# from typing import Optional
# from urllib.parse import quote

# import httpx
# from fastapi import APIRouter, Depends, HTTPException, Query

# from auth import get_current_user
# from models import User

# router = APIRouter(tags=["Search"])

# TRACKERS = [
#     "udp://tracker.opentrackr.org:1337/announce",
#     "udp://open.tracker.cl:1337/announce",
#     "udp://9.rarbg.com:2810/announce",
#     "udp://tracker.openbits.net:1337/announce",
#     "udp://exodus.desync.com:6969/announce",
#     "udp://tracker.torrent.eu.org:451/announce",
# ]

# APIBAY_BASE = "https://apibay.org"
# CATEGORY_MAP = {
#     "all": "0",
#     "movies": "207",
#     "tv": "208",
#     "4k": "211",
#     "music": "101",
#     "games": "400",
#     "apps": "300",
#     "audio": "100",
#     "video": "200",
#     "other": "600",
# }


# def build_magnet(info_hash: str, name: str) -> str:
#     trackers = "&tr=".join(quote(t, safe="") for t in TRACKERS)
#     return f"magnet:?xt=urn:btih:{info_hash}&dn={quote(name, safe='')}&tr={trackers}"


# def fmt(t: dict) -> dict:
#     info_hash = t.get("info_hash", "")
#     name = t.get("name", "")
#     return {
#         "name": name,
#         "size": t.get("size", "0"),
#         "seeders": t.get("seeders", "0"),
#         "leechers": t.get("leechers", "0"),
#         "magnet": build_magnet(info_hash, name),
#         "hash": info_hash,
#         "poster": "",
#         "category": t.get("category", "0"),
#         "site": "apibay",
#         "url": "",
#         "date": t.get("added", ""),
#         "uploader": None,
#         "screenshot": [],
#         "files": [],
#     }


# async def fetch(url: str, params: dict | None = None) -> list[dict]:
#     try:
#         async with httpx.AsyncClient(timeout=20) as client:
#             response = await client.get(url, params=params)
#             response.raise_for_status()
#             data = response.json()
#             if isinstance(data, list) and len(data) == 1 and data[0].get("id") == "0":
#                 return []
#             return data if isinstance(data, list) else []
#     except httpx.HTTPStatusError as exc:
#         raise HTTPException(
#             status_code=exc.response.status_code,
#             detail=f"Torrent API error: {exc.response.text}",
#         ) from exc
#     except Exception as exc:
#         raise HTTPException(status_code=503, detail=f"Torrent API unreachable: {exc}") from exc


# def wrap(items: list[dict], page: int = 1) -> dict:
#     return {
#         "data": items,
#         "current_page": page,
#         "total_pages": 1,
#         "total": len(items),
#         "time": None,
#     }


# @router.get("/")
# async def search(
#     query: str = Query(..., min_length=1),
#     category: Optional[str] = Query(
#         "all", description="all|movies|tv|4k|music|games|apps|audio|video|other"
#     ),
#     _user: User = Depends(get_current_user),
# ):
#     started = perf_counter()
#     cat_code = CATEGORY_MAP.get(category.lower(), "0")
#     data = await fetch(f"{APIBAY_BASE}/q.php", {"q": query, "cat": cat_code})
#     result = wrap([fmt(item) for item in data])
#     result["time"] = round(perf_counter() - started, 3)
#     return result


# @router.get("/trending")
# async def trending(
#     category: Optional[str] = Query("all"),
#     _user: User = Depends(get_current_user),
# ):
#     cat = category.lower() if category.lower() in {
#         "all", "audio", "video", "apps", "games", "porn", "other"
#     } else "all"
#     data = await fetch(f"{APIBAY_BASE}/precompiled/data_top100_{cat}.json")
#     return wrap([fmt(item) for item in data])


# @router.get("/recent")
# async def recent(_user: User = Depends(get_current_user)):
#     data = await fetch(f"{APIBAY_BASE}/precompiled/data_top100_recent.json")
#     return wrap([fmt(item) for item in data])


# @router.get("/top/movies")
# async def top_movies(_user: User = Depends(get_current_user)):
#     data = await fetch(f"{APIBAY_BASE}/precompiled/data_top100_207.json")
#     return wrap([fmt(item) for item in data])


# @router.get("/top/tv")
# async def top_tv(_user: User = Depends(get_current_user)):
#     data = await fetch(f"{APIBAY_BASE}/precompiled/data_top100_208.json")
#     return wrap([fmt(item) for item in data])
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
    # Search results return torrent metadata plus magnet only.
    # The client now starts the torrent session with the magnet,
    # waits for metadata, asks the server for file listing, then streams a chosen file.
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
    # Replace the existing GET "/" handler with a demo response.
    router.routes = [
        r for r in router.routes
        if not (getattr(r, "path", None) == "/" and "GET" in getattr(r, "methods", set()))
    ]


    @router.get("/")
    async def search_demo(
        query: str = Query(..., min_length=1),
        category: Optional[str] = Query("all"),
        _user: User = Depends(get_current_user),
    ):
        return {
            "data": [
                {
                    "name": f"Demo result for: {query}",
                    "size": "1073741824",
                    "seeders": "123",
                    "leechers": "7",
                    "magnet": "magnet:?xt=urn:btih:DEMOHASH1234567890ABCDEF1234567890ABCDEF",
                    "hash": "DEMOHASH1234567890ABCDEF1234567890ABCDEF",
                    "poster": "",
                    "category": CATEGORY_MAP.get(category.lower(), "0"),
                    "site": "demo",
                    "url": "",
                    "date": "1710000000",
                    "uploader": "demo-user",
                    "screenshot": [],
                    "files": [],
                }
            ],
            "current_page": 1,
            "total_pages": 1,
            "total": 1,
            "time": 0.001,
        }


@router.get("/trending")
async def trending(
    category: Optional[str] = Query("all"),
    _user: User = Depends(get_current_user),
):
    cat = category.lower() if category.lower() in {
        "all", "audio", "video", "apps", "games", "porn", "other"
    } else "all"
    data = await fetch(f"{APIBAY_BASE}/precompiled/data_top100_{cat}.json")
    return wrap([fmt(item) for item in data])


@router.get("/recent")
async def recent(_user: User = Depends(get_current_user)):
    data = await fetch(f"{APIBAY_BASE}/precompiled/data_top100_recent.json")
    return wrap([fmt(item) for item in data])


@router.get("/top/movies")
async def top_movies(_user: User = Depends(get_current_user)):
    data = await fetch(f"{APIBAY_BASE}/precompiled/data_top100_207.json")
    return wrap([fmt(item) for item in data])


@router.get("/top/tv")
async def top_tv(_user: User = Depends(get_current_user)):
    data = await fetch(f"{APIBAY_BASE}/precompiled/data_top100_208.json")
    return wrap([fmt(item) for item in data])