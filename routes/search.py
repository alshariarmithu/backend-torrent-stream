"""
Search route – proxies requests to APIBay (The Pirate Bay unofficial API)
"""

from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional, List
import httpx

from auth import get_current_user
from database import User

router = APIRouter(tags=["Search"])


# 🔧 Helper: build magnet link
def build_magnet(info_hash: str, name: str) -> str:
    return f"magnet:?xt=urn:btih:{info_hash}&dn={name}"


# 🔧 Helper: fetch data safely
async def fetch(url: str, params: dict = None):
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            return r.json()
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Torrent API error: {e.response.text}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Torrent API unreachable: {e}",
        )


# 🔍 1. SEARCH
@router.get("/")
async def search(
    query: str = Query(...),
    _user: User = Depends(get_current_user),
):
    url = "https://apibay.org/q.php"
    data = await fetch(url, {"q": query})

    # clean response
    return [
        {
            "name": t.get("name"),
            "seeders": t.get("seeders"),
            "leechers": t.get("leechers"),
            "size": t.get("size"),
            "magnet": build_magnet(t.get("info_hash"), t.get("name")),
        }
        for t in data
    ]


# 🔥 2. TRENDING (Top 100)
@router.get("/trending")
async def trending(
    _user: User = Depends(get_current_user),
):
    url = "https://apibay.org/precompiled/data_top100_all.json"
    data = await fetch(url)

    return [
        {
            "name": t.get("name"),
            "seeders": t.get("seeders"),
            "leechers": t.get("leechers"),
            "size": t.get("size"),
            "magnet": build_magnet(t.get("info_hash"), t.get("name")),
        }
        for t in data
    ]


# 🆕 3. RECENT
@router.get("/recent")
async def recent(
    _user: User = Depends(get_current_user),
):
    url = "https://apibay.org/precompiled/data_recent.json"
    data = await fetch(url)

    return [
        {
            "name": t.get("name"),
            "seeders": t.get("seeders"),
            "leechers": t.get("leechers"),
            "size": t.get("size"),
            "magnet": build_magnet(t.get("info_hash"), t.get("name")),
        }
        for t in data
    ]