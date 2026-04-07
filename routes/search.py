"""
Search route – proxies requests to APIBay (The Pirate Bay unofficial API)

Category codes (apibay):
  0  = All
  100= Audio, 101=Music, 102=Audio Books, 103=Sound Clips, 104=FLAC, 199=Other
  200= Video, 201=Movies, 202=Movies DVDR, 203=Music Videos, 204=Movie Clips,
       205=TV Shows, 206=Handheld, 207=HD Movies, 208=HD TV Shows, 209=3D,
       210=CAM/TS, 211=UHD/4K Movies, 212=UHD/4K TV Shows, 299=Other
  300= Apps, 400=Games, 500=Porn, 600=Other
"""
from fastapi import APIRouter, Depends, Query, HTTPException
from typing import Optional
import httpx
from urllib.parse import quote

from auth import get_current_user
from database import User

router = APIRouter(tags=["Search"])

# ─── Well-known public trackers (improves connectivity) ──────────────────────
TRACKERS = [
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://open.tracker.cl:1337/announce",
    "udp://9.rarbg.com:2810/announce",
    "udp://tracker.openbits.net:1337/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
]

APIBAY_BASE = "https://apibay.org"

# APIBay category string map (human-readable → numeric)
CATEGORY_MAP = {
    "all":        "0",
    "movies":     "207",   # HD Movies (best for streaming)
    "tv":         "208",   # HD TV Shows
    "4k":         "211",   # UHD/4K Movies
    "music":      "101",
    "games":      "400",
    "apps":       "300",
    "audio":      "100",
    "video":      "200",
    "other":      "600",
}


# ─── Helpers ─────────────────────────────────────────────────────────────────

def build_magnet(info_hash: str, name: str) -> str:
    trackers = "&tr=".join(quote(t, safe="") for t in TRACKERS)
    return f"magnet:?xt=urn:btih:{info_hash}&dn={quote(name, safe='')}&tr={trackers}"


def fmt(t: dict) -> dict:
    info_hash = t.get("info_hash", "")
    name      = t.get("name", "")
    return {
        "name":      name,
        "info_hash": info_hash,
        "seeders":   t.get("seeders", "0"),
        "leechers":  t.get("leechers", "0"),
        "size":      t.get("size", "0"),
        "category":  t.get("category", "0"),
        "added":     t.get("added", ""),
        "magnet":    build_magnet(info_hash, name),
    }


async def fetch(url: str, params: dict = None) -> list:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, params=params)
            r.raise_for_status()
            data = r.json()
            # APIBay returns [{"id":"0","name":"No results returned",...}] on empty
            if isinstance(data, list) and len(data) == 1 and data[0].get("id") == "0":
                return []
            return data if isinstance(data, list) else []
    except httpx.HTTPStatusError as e:
        raise HTTPException(
            status_code=e.response.status_code,
            detail=f"Torrent API error: {e.response.text}",
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Torrent API unreachable: {e}")


# ─── Routes ──────────────────────────────────────────────────────────────────

@router.get("/")
async def search(
    query: str = Query(..., min_length=1),
    category: Optional[str] = Query("all", description="all|movies|tv|4k|music|games|apps|audio|video|other"),
    _user: User = Depends(get_current_user),
):
    """Search torrents on The Pirate Bay via APIBay."""
    cat_code = CATEGORY_MAP.get(category.lower(), "0")
    data = await fetch(APIBAY_BASE + "/q.php", {"q": query, "cat": cat_code})
    return [fmt(t) for t in data]


@router.get("/trending")
async def trending(
    category: Optional[str] = Query("all"),
    _user: User = Depends(get_current_user),
):
    """
    Top 100 trending torrents.
    category: all | audio | video | apps | games | porn | other
    (maps to apibay precompiled filenames)
    """
    # apibay precompiled files: data_top100_{category}.json
    # Valid: all, audio, video, apps, games, porn, other
    cat = category.lower() if category.lower() in {
        "all", "audio", "video", "apps", "games", "porn", "other"
    } else "all"
    url = f"{APIBAY_BASE}/precompiled/data_top100_{cat}.json"
    data = await fetch(url)
    return [fmt(t) for t in data]


@router.get("/recent")
async def recent(
    _user: User = Depends(get_current_user),
):
    """Most recently added torrents."""
    # Correct apibay recent endpoint
    url = f"{APIBAY_BASE}/precompiled/data_top100_recent.json"
    data = await fetch(url)
    return [fmt(t) for t in data]


@router.get("/top/movies")
async def top_movies(_user: User = Depends(get_current_user)):
    """Top 100 movies."""
    data = await fetch(f"{APIBAY_BASE}/precompiled/data_top100_207.json")
    return [fmt(t) for t in data]


@router.get("/top/tv")
async def top_tv(_user: User = Depends(get_current_user)):
    """Top 100 TV shows."""
    data = await fetch(f"{APIBAY_BASE}/precompiled/data_top100_208.json")
    return [fmt(t) for t in data]