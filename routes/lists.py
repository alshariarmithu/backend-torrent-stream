import os
from datetime import datetime
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pymongo.errors import PyMongoError

from activity import record_recent_torrent, touch_torrent
from auth import get_current_user
from content_policy import assert_allowed_torrent
from models import (
    Playlist,
    PlaylistCreate,
    PlaylistEntry,
    PlaylistItemAdd,
    TorrentItem,
    TorrentPayload,
    User,
    WatchLaterItem,
    WatchlistItem,
    WishlistItem,
)

router = APIRouter()

LISTS_DEMO_MODE = os.getenv("LISTS_DEMO_MODE", "false").lower() in {"1", "true", "yes"}

DEMO_TORRENTS: dict[str, dict] = {}
DEMO_TORRENTS_BY_HASH: dict[str, str] = {}
DEMO_LISTS: dict[str, dict[str, list[dict]]] = {
    "watchlist": {},
    "wishlist": {},
    "watchlater": {},
}
DEMO_PLAYLISTS: dict[str, list[dict]] = {}


def _now() -> datetime:
    return datetime.utcnow()


def _new_id() -> str:
    return uuid4().hex


def _list_bucket(name: str, user_id: str) -> list[dict]:
    return DEMO_LISTS[name].setdefault(user_id, [])


def _playlist_bucket(user_id: str) -> list[dict]:
    return DEMO_PLAYLISTS.setdefault(user_id, [])


def torrent_dict(t: TorrentItem | dict) -> dict:
    if isinstance(t, dict):
        return {
            "id": t.get("id", ""),
            "name": t.get("name", ""),
            "size": t.get("size", ""),
            "seeders": t.get("seeders", "0"),
            "leechers": t.get("leechers", "0"),
            "magnet": t.get("magnet", ""),
            "hash": t.get("hash", ""),
            "poster": t.get("poster", ""),
            "category": t.get("category", ""),
            "site": t.get("site", ""),
            "url": t.get("url", ""),
            "added_at": t.get("added_at"),
        }

    return {
        "id": str(t.id),
        "name": t.name,
        "size": t.size or "",
        "seeders": t.seeders or "0",
        "leechers": t.leechers or "0",
        "magnet": t.magnet or "",
        "hash": t.hash or "",
        "poster": t.poster or "",
        "category": t.category or "",
        "site": t.site or "",
        "url": t.url or "",
        "added_at": t.added_at,
    }


def _demo_torrent_from_payload(payload: TorrentPayload) -> dict:
    torrent_id = _new_id()
    torrent = {
        "id": torrent_id,
        "name": payload.name,
        "size": payload.size or "",
        "seeders": payload.seeders or "0",
        "leechers": payload.leechers or "0",
        "magnet": payload.magnet or "",
        "hash": payload.hash or "",
        "poster": payload.poster or "",
        "category": payload.category or "",
        "site": payload.site or "",
        "url": payload.url or "",
        "added_at": _now(),
    }
    DEMO_TORRENTS[torrent_id] = torrent
    if torrent["hash"]:
        DEMO_TORRENTS_BY_HASH[torrent["hash"]] = torrent_id
    return torrent


def _demo_get_or_create_torrent(payload: TorrentPayload) -> dict:
    if payload.hash:
        existing_id = DEMO_TORRENTS_BY_HASH.get(payload.hash)
        if existing_id:
            return DEMO_TORRENTS[existing_id]
    return _demo_torrent_from_payload(payload)


def _demo_find_row(bucket: str, user_id: str, torrent_id: str | None = None, item_id: str | None = None) -> dict | None:
    rows = _list_bucket(bucket, user_id)
    for row in rows:
        if torrent_id and row["torrent_id"] == torrent_id:
            return row
        if item_id and row["id"] == item_id:
            return row
    return None


def _demo_add_row(bucket: str, user_id: str, torrent_id: str, watched: bool = False) -> dict:
    row = {
        "id": _new_id(),
        "user_id": user_id,
        "torrent_id": torrent_id,
        "added_at": _now(),
    }
    if bucket == "watchlist":
        row["watched"] = watched
    _list_bucket(bucket, user_id).append(row)
    return row


def _demo_delete_row(bucket: str, user_id: str, item_id: str) -> bool:
    rows = _list_bucket(bucket, user_id)
    for index, row in enumerate(rows):
        if row["id"] == item_id:
            rows.pop(index)
            return True
    return False


def _demo_find_playlist(user_id: str, playlist_id: str) -> dict | None:
    for playlist in _playlist_bucket(user_id):
        if playlist["id"] == playlist_id:
            return playlist
    return None


async def _resolve_torrent(torrent_id: str) -> TorrentItem | dict | None:
    if LISTS_DEMO_MODE:
        return DEMO_TORRENTS.get(torrent_id)

    try:
        return await TorrentItem.get(torrent_id)
    except PyMongoError:
        return DEMO_TORRENTS.get(torrent_id)


async def get_or_create_torrent(payload: TorrentPayload) -> TorrentItem | dict:
    assert_allowed_torrent(payload)

    if LISTS_DEMO_MODE:
        return _demo_get_or_create_torrent(payload)

    try:
        tracked = await touch_torrent(payload, hit_increment=1)
        if tracked is not None:
            return tracked
        if payload.hash:
            existing = await TorrentItem.find_one(TorrentItem.hash == payload.hash)
            if existing:
                return existing

        item = TorrentItem(**payload.model_dump())
        await item.insert()
        return item
    except PyMongoError:
        return _demo_get_or_create_torrent(payload)


async def materialize_rows(rows: list[WatchlistItem | WishlistItem | WatchLaterItem | dict]) -> list[dict]:
    result: list[dict] = []
    for row in rows:
        torrent_id = row["torrent_id"] if isinstance(row, dict) else row.torrent_id
        torrent = await _resolve_torrent(torrent_id)
        if torrent is None:
            continue

        entry = {
            "list_id": row["id"] if isinstance(row, dict) else str(row.id),
            "added_at": row["added_at"] if isinstance(row, dict) else row.added_at,
            "torrent": torrent_dict(torrent),
        }
        if isinstance(row, dict):
            if "watched" in row:
                entry["watched"] = row.get("watched", False)
        elif hasattr(row, "watched"):
            entry["watched"] = getattr(row, "watched", False)
        result.append(entry)
    return result


async def _get_rows(bucket: str, user_id: str, model) -> list[WatchlistItem | WishlistItem | WatchLaterItem | dict]:
    if LISTS_DEMO_MODE:
        return sorted(_list_bucket(bucket, user_id), key=lambda row: row["added_at"], reverse=True)

    try:
        return await model.find(model.user_id == user_id).sort([("added_at", -1)]).to_list()
    except PyMongoError:
        return sorted(_list_bucket(bucket, user_id), key=lambda row: row["added_at"], reverse=True)


async def _find_existing_row(bucket: str, user_id: str, torrent_id: str, model) -> WatchlistItem | WishlistItem | WatchLaterItem | dict | None:
    if LISTS_DEMO_MODE:
        return _demo_find_row(bucket, user_id, torrent_id=torrent_id)

    try:
        return await model.find_one(model.user_id == user_id, model.torrent_id == torrent_id)
    except PyMongoError:
        return _demo_find_row(bucket, user_id, torrent_id=torrent_id)


async def _get_row_by_id(bucket: str, user_id: str, item_id: str, model) -> WatchlistItem | WishlistItem | WatchLaterItem | dict | None:
    if LISTS_DEMO_MODE:
        return _demo_find_row(bucket, user_id, item_id=item_id)

    try:
        item = await model.get(item_id)
    except PyMongoError:
        item = None

    if item and item.user_id == user_id:
        return item
    return _demo_find_row(bucket, user_id, item_id=item_id)


@router.get("/watchlist")
async def get_watchlist(user: User = Depends(get_current_user)):
    rows = await _get_rows("watchlist", str(user.id), WatchlistItem)
    return await materialize_rows(rows)


@router.post("/watchlist", status_code=201)
async def add_watchlist(payload: TorrentPayload, user: User = Depends(get_current_user)):
    user_id = str(user.id)
    torrent = await get_or_create_torrent(payload)
    torrent_id = torrent["id"] if isinstance(torrent, dict) else str(torrent.id)

    existing = await _find_existing_row("watchlist", user_id, torrent_id, WatchlistItem)
    if existing:
        raise HTTPException(status_code=400, detail="Already in watchlist")

    if LISTS_DEMO_MODE or isinstance(torrent, dict):
        _demo_add_row("watchlist", user_id, torrent_id)
        return {"message": "Added to watchlist"}

    try:
        await WatchlistItem(user_id=user_id, torrent_id=torrent_id).insert()
    except PyMongoError:
        _demo_add_row("watchlist", user_id, torrent_id)
    await record_recent_torrent(user_id, torrent_id, source="watchlist")
    return {"message": "Added to watchlist"}


@router.patch("/watchlist/{item_id}/watched")
async def mark_watched(
    item_id: str,
    watched: bool = True,
    user: User = Depends(get_current_user),
):
    user_id = str(user.id)
    item = await _get_row_by_id("watchlist", user_id, item_id, WatchlistItem)
    if not item:
        raise HTTPException(status_code=404, detail="Not found")

    if isinstance(item, dict):
        item["watched"] = watched
        return {"message": "Updated"}

    item.watched = watched
    try:
        await item.save()
    except PyMongoError:
        demo_item = _demo_find_row("watchlist", user_id, item_id=item_id)
        if demo_item:
            demo_item["watched"] = watched
        else:
            _demo_add_row("watchlist", user_id, item.torrent_id, watched=watched)["id"] = item_id
    torrent_id = item["torrent_id"] if isinstance(item, dict) else item.torrent_id
    await record_recent_torrent(user_id, torrent_id, source="watchlist")
    return {"message": "Updated"}


@router.delete("/watchlist/{item_id}", status_code=204)
async def remove_watchlist(item_id: str, user: User = Depends(get_current_user)):
    user_id = str(user.id)
    item = await _get_row_by_id("watchlist", user_id, item_id, WatchlistItem)
    if not item:
        raise HTTPException(status_code=404, detail="Not found")

    if isinstance(item, dict):
        _demo_delete_row("watchlist", user_id, item_id)
        return

    try:
        await item.delete()
    except PyMongoError:
        _demo_delete_row("watchlist", user_id, item_id)


@router.get("/wishlist")
async def get_wishlist(user: User = Depends(get_current_user)):
    rows = await _get_rows("wishlist", str(user.id), WishlistItem)
    return await materialize_rows(rows)


@router.post("/wishlist", status_code=201)
async def add_wishlist(payload: TorrentPayload, user: User = Depends(get_current_user)):
    user_id = str(user.id)
    torrent = await get_or_create_torrent(payload)
    torrent_id = torrent["id"] if isinstance(torrent, dict) else str(torrent.id)

    existing = await _find_existing_row("wishlist", user_id, torrent_id, WishlistItem)
    if existing:
        raise HTTPException(status_code=400, detail="Already in wishlist")

    if LISTS_DEMO_MODE or isinstance(torrent, dict):
        _demo_add_row("wishlist", user_id, torrent_id)
        return {"message": "Added to wishlist"}

    try:
        await WishlistItem(user_id=user_id, torrent_id=torrent_id).insert()
    except PyMongoError:
        _demo_add_row("wishlist", user_id, torrent_id)
    return {"message": "Added to wishlist"}


@router.delete("/wishlist/{item_id}", status_code=204)
async def remove_wishlist(item_id: str, user: User = Depends(get_current_user)):
    user_id = str(user.id)
    item = await _get_row_by_id("wishlist", user_id, item_id, WishlistItem)
    if not item:
        raise HTTPException(status_code=404, detail="Not found")

    if isinstance(item, dict):
        _demo_delete_row("wishlist", user_id, item_id)
        return

    try:
        await item.delete()
    except PyMongoError:
        _demo_delete_row("wishlist", user_id, item_id)


@router.get("/watchlater")
async def get_watchlater(user: User = Depends(get_current_user)):
    rows = await _get_rows("watchlater", str(user.id), WatchLaterItem)
    return await materialize_rows(rows)


@router.post("/watchlater", status_code=201)
async def add_watchlater(payload: TorrentPayload, user: User = Depends(get_current_user)):
    user_id = str(user.id)
    torrent = await get_or_create_torrent(payload)
    torrent_id = torrent["id"] if isinstance(torrent, dict) else str(torrent.id)

    existing = await _find_existing_row("watchlater", user_id, torrent_id, WatchLaterItem)
    if existing:
        raise HTTPException(status_code=400, detail="Already in watch later")

    if LISTS_DEMO_MODE or isinstance(torrent, dict):
        _demo_add_row("watchlater", user_id, torrent_id)
        return {"message": "Added to watch later"}

    try:
        await WatchLaterItem(user_id=user_id, torrent_id=torrent_id).insert()
    except PyMongoError:
        _demo_add_row("watchlater", user_id, torrent_id)
    await record_recent_torrent(user_id, torrent_id, source="watchlater")
    return {"message": "Added to watch later"}


@router.delete("/watchlater/{item_id}", status_code=204)
async def remove_watchlater(item_id: str, user: User = Depends(get_current_user)):
    user_id = str(user.id)
    item = await _get_row_by_id("watchlater", user_id, item_id, WatchLaterItem)
    if not item:
        raise HTTPException(status_code=404, detail="Not found")

    if isinstance(item, dict):
        _demo_delete_row("watchlater", user_id, item_id)
        return

    try:
        await item.delete()
    except PyMongoError:
        _demo_delete_row("watchlater", user_id, item_id)


@router.get("/playlists")
async def get_playlists(user: User = Depends(get_current_user)):
    user_id = str(user.id)
    if LISTS_DEMO_MODE:
        playlists = _playlist_bucket(user_id)
    else:
        try:
            playlists = await Playlist.find(Playlist.user_id == user_id).to_list()
        except PyMongoError:
            playlists = _playlist_bucket(user_id)

    return [
        {
            "id": playlist["id"] if isinstance(playlist, dict) else str(playlist.id),
            "name": playlist["name"] if isinstance(playlist, dict) else playlist.name,
            "description": playlist["description"] if isinstance(playlist, dict) else playlist.description,
            "created_at": playlist["created_at"] if isinstance(playlist, dict) else playlist.created_at,
            "item_count": len(playlist["items"] if isinstance(playlist, dict) else playlist.items),
        }
        for playlist in playlists
    ]


@router.post("/playlists", status_code=201)
async def create_playlist(body: PlaylistCreate, user: User = Depends(get_current_user)):
    user_id = str(user.id)
    if LISTS_DEMO_MODE:
        playlist = {
            "id": _new_id(),
            "user_id": user_id,
            "name": body.name,
            "description": body.description or "",
            "created_at": _now(),
            "items": [],
        }
        _playlist_bucket(user_id).append(playlist)
        return {
            "id": playlist["id"],
            "name": playlist["name"],
            "description": playlist["description"],
            "created_at": playlist["created_at"],
            "item_count": 0,
        }

    try:
        pl = Playlist(user_id=user_id, name=body.name, description=body.description or "")
        await pl.insert()
        return {
            "id": str(pl.id),
            "name": pl.name,
            "description": pl.description,
            "created_at": pl.created_at,
            "item_count": 0,
        }
    except PyMongoError:
        playlist = {
            "id": _new_id(),
            "user_id": user_id,
            "name": body.name,
            "description": body.description or "",
            "created_at": _now(),
            "items": [],
        }
        _playlist_bucket(user_id).append(playlist)
        return {
            "id": playlist["id"],
            "name": playlist["name"],
            "description": playlist["description"],
            "created_at": playlist["created_at"],
            "item_count": 0,
        }


@router.get("/playlists/{playlist_id}")
async def get_playlist(playlist_id: str, user: User = Depends(get_current_user)):
    user_id = str(user.id)

    if LISTS_DEMO_MODE:
        playlist = _demo_find_playlist(user_id, playlist_id)
    else:
        try:
            playlist = await Playlist.get(playlist_id)
            if playlist and playlist.user_id != user_id:
                playlist = None
        except PyMongoError:
            playlist = _demo_find_playlist(user_id, playlist_id)

    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist not found")

    items = sorted(playlist["items"], key=lambda x: x["position"]) if isinstance(playlist, dict) else sorted(playlist.items, key=lambda x: x.position)
    materialized = []
    for item in items:
        torrent_id = item["torrent_id"] if isinstance(item, dict) else item.torrent_id
        torrent = await _resolve_torrent(torrent_id)
        if torrent is None:
            continue
        materialized.append(
            {
                "item_id": item["id"] if isinstance(item, dict) else item.id,
                "position": item["position"] if isinstance(item, dict) else item.position,
                "torrent": torrent_dict(torrent),
            }
        )

    return {
        "id": playlist["id"] if isinstance(playlist, dict) else str(playlist.id),
        "name": playlist["name"] if isinstance(playlist, dict) else playlist.name,
        "description": playlist["description"] if isinstance(playlist, dict) else playlist.description,
        "created_at": playlist["created_at"] if isinstance(playlist, dict) else playlist.created_at,
        "items": materialized,
    }


@router.post("/playlists/{playlist_id}/items", status_code=201)
async def add_playlist_item(
    playlist_id: str,
    body: PlaylistItemAdd,
    user: User = Depends(get_current_user),
):
    user_id = str(user.id)
    torrent = await get_or_create_torrent(body.torrent)
    torrent_id = torrent["id"] if isinstance(torrent, dict) else str(torrent.id)

    if LISTS_DEMO_MODE:
        playlist = _demo_find_playlist(user_id, playlist_id)
        if not playlist:
            raise HTTPException(status_code=404, detail="Playlist not found")
        playlist["items"].append(
            {
                "id": _new_id(),
                "torrent_id": torrent_id,
                "position": body.position or len(playlist["items"]),
                "added_at": _now(),
            }
        )
        await record_recent_torrent(user_id, torrent_id, source="playlist")
        return {"message": "Added to playlist"}

    try:
        pl = await Playlist.get(playlist_id)
        if not pl or pl.user_id != user_id:
            raise HTTPException(status_code=404, detail="Playlist not found")
        pl.items.append(
            PlaylistEntry(
                torrent_id=torrent_id,
                position=body.position or len(pl.items),
                added_at=_now(),
            )
        )
        await pl.save()
        await record_recent_torrent(user_id, torrent_id, source="playlist")
    except PyMongoError:
        playlist = _demo_find_playlist(user_id, playlist_id)
        if not playlist:
            raise HTTPException(status_code=404, detail="Playlist not found")
        playlist["items"].append(
            {
                "id": _new_id(),
                "torrent_id": torrent_id,
                "position": body.position or len(playlist["items"]),
                "added_at": _now(),
            }
        )
        await record_recent_torrent(user_id, torrent_id, source="playlist")
    return {"message": "Added to playlist"}


@router.delete("/playlists/{playlist_id}/items/{item_id}", status_code=204)
async def remove_playlist_item(
    playlist_id: str,
    item_id: str,
    user: User = Depends(get_current_user),
):
    user_id = str(user.id)

    if LISTS_DEMO_MODE:
        playlist = _demo_find_playlist(user_id, playlist_id)
        if not playlist:
            raise HTTPException(status_code=404, detail="Playlist not found")
        new_items = [item for item in playlist["items"] if item["id"] != item_id]
        if len(new_items) == len(playlist["items"]):
            raise HTTPException(status_code=404, detail="Item not found")
        playlist["items"] = new_items
        return

    try:
        pl = await Playlist.get(playlist_id)
        if not pl or pl.user_id != user_id:
            raise HTTPException(status_code=404, detail="Playlist not found")

        new_items = [item for item in pl.items if item.id != item_id]
        if len(new_items) == len(pl.items):
            raise HTTPException(status_code=404, detail="Item not found")
        pl.items = new_items
        await pl.save()
    except PyMongoError:
        playlist = _demo_find_playlist(user_id, playlist_id)
        if not playlist:
            raise HTTPException(status_code=404, detail="Playlist not found")
        new_items = [item for item in playlist["items"] if item["id"] != item_id]
        if len(new_items) == len(playlist["items"]):
            raise HTTPException(status_code=404, detail="Item not found")
        playlist["items"] = new_items


@router.delete("/playlists/{playlist_id}", status_code=204)
async def delete_playlist(playlist_id: str, user: User = Depends(get_current_user)):
    user_id = str(user.id)

    if LISTS_DEMO_MODE:
        playlists = _playlist_bucket(user_id)
        for index, playlist in enumerate(playlists):
            if playlist["id"] == playlist_id:
                playlists.pop(index)
                return
        raise HTTPException(status_code=404, detail="Playlist not found")

    try:
        pl = await Playlist.get(playlist_id)
        if not pl or pl.user_id != user_id:
            raise HTTPException(status_code=404, detail="Playlist not found")
        await pl.delete()
    except PyMongoError:
        playlists = _playlist_bucket(user_id)
        for index, playlist in enumerate(playlists):
            if playlist["id"] == playlist_id:
                playlists.pop(index)
                return
        raise HTTPException(status_code=404, detail="Playlist not found")
