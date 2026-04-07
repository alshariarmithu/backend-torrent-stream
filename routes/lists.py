from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException

from auth import get_current_user
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


def torrent_dict(t: TorrentItem) -> dict:
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


async def get_or_create_torrent(payload: TorrentPayload) -> TorrentItem:
    if payload.hash:
        existing = await TorrentItem.find_one(TorrentItem.hash == payload.hash)
        if existing:
            return existing

    item = TorrentItem(**payload.model_dump())
    await item.insert()
    return item


async def materialize_rows(rows: list[WatchlistItem | WishlistItem | WatchLaterItem]) -> list[dict]:
    result: list[dict] = []
    for row in rows:
        torrent = await TorrentItem.get(row.torrent_id)
        if torrent is None:
            continue
        entry = {
            "list_id": str(row.id),
            "added_at": row.added_at,
            "torrent": torrent_dict(torrent),
        }
        if hasattr(row, "watched"):
            entry["watched"] = getattr(row, "watched", False)
        result.append(entry)
    return result


@router.get("/watchlist")
async def get_watchlist(user: User = Depends(get_current_user)):
    rows = await WatchlistItem.find(WatchlistItem.user_id == str(user.id)).to_list()
    return await materialize_rows(rows)


@router.post("/watchlist", status_code=201)
async def add_watchlist(payload: TorrentPayload, user: User = Depends(get_current_user)):
    torrent = await get_or_create_torrent(payload)
    existing = await WatchlistItem.find_one(
        WatchlistItem.user_id == str(user.id),
        WatchlistItem.torrent_id == str(torrent.id),
    )
    if existing:
        raise HTTPException(status_code=400, detail="Already in watchlist")
    await WatchlistItem(user_id=str(user.id), torrent_id=str(torrent.id)).insert()
    return {"message": "Added to watchlist"}


@router.patch("/watchlist/{item_id}/watched")
async def mark_watched(
    item_id: str,
    watched: bool = True,
    user: User = Depends(get_current_user),
):
    item = await WatchlistItem.get(item_id)
    if not item or item.user_id != str(user.id):
        raise HTTPException(status_code=404, detail="Not found")
    item.watched = watched
    await item.save()
    return {"message": "Updated"}


@router.delete("/watchlist/{item_id}", status_code=204)
async def remove_watchlist(item_id: str, user: User = Depends(get_current_user)):
    item = await WatchlistItem.get(item_id)
    if not item or item.user_id != str(user.id):
        raise HTTPException(status_code=404, detail="Not found")
    await item.delete()


@router.get("/wishlist")
async def get_wishlist(user: User = Depends(get_current_user)):
    rows = await WishlistItem.find(WishlistItem.user_id == str(user.id)).to_list()
    return await materialize_rows(rows)


@router.post("/wishlist", status_code=201)
async def add_wishlist(payload: TorrentPayload, user: User = Depends(get_current_user)):
    torrent = await get_or_create_torrent(payload)
    existing = await WishlistItem.find_one(
        WishlistItem.user_id == str(user.id),
        WishlistItem.torrent_id == str(torrent.id),
    )
    if existing:
        raise HTTPException(status_code=400, detail="Already in wishlist")
    await WishlistItem(user_id=str(user.id), torrent_id=str(torrent.id)).insert()
    return {"message": "Added to wishlist"}


@router.delete("/wishlist/{item_id}", status_code=204)
async def remove_wishlist(item_id: str, user: User = Depends(get_current_user)):
    item = await WishlistItem.get(item_id)
    if not item or item.user_id != str(user.id):
        raise HTTPException(status_code=404, detail="Not found")
    await item.delete()


@router.get("/watchlater")
async def get_watchlater(user: User = Depends(get_current_user)):
    rows = await WatchLaterItem.find(WatchLaterItem.user_id == str(user.id)).to_list()
    return await materialize_rows(rows)


@router.post("/watchlater", status_code=201)
async def add_watchlater(payload: TorrentPayload, user: User = Depends(get_current_user)):
    torrent = await get_or_create_torrent(payload)
    existing = await WatchLaterItem.find_one(
        WatchLaterItem.user_id == str(user.id),
        WatchLaterItem.torrent_id == str(torrent.id),
    )
    if existing:
        raise HTTPException(status_code=400, detail="Already in watch later")
    await WatchLaterItem(user_id=str(user.id), torrent_id=str(torrent.id)).insert()
    return {"message": "Added to watch later"}


@router.delete("/watchlater/{item_id}", status_code=204)
async def remove_watchlater(item_id: str, user: User = Depends(get_current_user)):
    item = await WatchLaterItem.get(item_id)
    if not item or item.user_id != str(user.id):
        raise HTTPException(status_code=404, detail="Not found")
    await item.delete()


@router.get("/playlists")
async def get_playlists(user: User = Depends(get_current_user)):
    playlists = await Playlist.find(Playlist.user_id == str(user.id)).to_list()
    return [
        {
            "id": str(p.id),
            "name": p.name,
            "description": p.description,
            "created_at": p.created_at,
            "item_count": len(p.items),
        }
        for p in playlists
    ]


@router.post("/playlists", status_code=201)
async def create_playlist(body: PlaylistCreate, user: User = Depends(get_current_user)):
    pl = Playlist(user_id=str(user.id), name=body.name, description=body.description or "")
    await pl.insert()
    return {"id": str(pl.id), "name": pl.name, "description": pl.description, "created_at": pl.created_at, "item_count": 0}


@router.get("/playlists/{playlist_id}")
async def get_playlist(playlist_id: str, user: User = Depends(get_current_user)):
    pl = await Playlist.get(playlist_id)
    if not pl or pl.user_id != str(user.id):
        raise HTTPException(status_code=404, detail="Playlist not found")

    items = sorted(pl.items, key=lambda x: x.position)
    materialized = []
    for item in items:
        torrent = await TorrentItem.get(item.torrent_id)
        if torrent is None:
            continue
        materialized.append({
            "item_id": item.id,
            "position": item.position,
            "torrent": torrent_dict(torrent),
        })

    return {
        "id": str(pl.id),
        "name": pl.name,
        "description": pl.description,
        "created_at": pl.created_at,
        "items": materialized,
    }


@router.post("/playlists/{playlist_id}/items", status_code=201)
async def add_playlist_item(
    playlist_id: str,
    body: PlaylistItemAdd,
    user: User = Depends(get_current_user),
):
    pl = await Playlist.get(playlist_id)
    if not pl or pl.user_id != str(user.id):
        raise HTTPException(status_code=404, detail="Playlist not found")

    torrent = await get_or_create_torrent(body.torrent)
    pl.items.append(
        PlaylistEntry(
            torrent_id=str(torrent.id),
            position=body.position or len(pl.items),
            added_at=datetime.utcnow(),
        )
    )
    await pl.save()
    return {"message": "Added to playlist"}


@router.delete("/playlists/{playlist_id}/items/{item_id}", status_code=204)
async def remove_playlist_item(
    playlist_id: str,
    item_id: str,
    user: User = Depends(get_current_user),
):
    pl = await Playlist.get(playlist_id)
    if not pl or pl.user_id != str(user.id):
        raise HTTPException(status_code=404, detail="Playlist not found")

    new_items = [item for item in pl.items if item.id != item_id]
    if len(new_items) == len(pl.items):
        raise HTTPException(status_code=404, detail="Item not found")
    pl.items = new_items
    await pl.save()


@router.delete("/playlists/{playlist_id}", status_code=204)
async def delete_playlist(playlist_id: str, user: User = Depends(get_current_user)):
    pl = await Playlist.get(playlist_id)
    if not pl or pl.user_id != str(user.id):
        raise HTTPException(status_code=404, detail="Playlist not found")
    await pl.delete()
