"""
User list management:
  Watchlist / Wishlist / Watch Later / Playlists
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

from database import (
    get_db, User, TorrentItem,
    WatchlistItem, WishlistItem, WatchLaterItem,
    Playlist, PlaylistItem,
)
from auth import get_current_user

router = APIRouter()


# ─── Shared Schemas ──────────────────────────────────────────────────────────

class TorrentPayload(BaseModel):
    name: str
    size: Optional[str] = ""
    seeders: Optional[str] = "0"
    leechers: Optional[str] = "0"
    magnet: Optional[str] = ""
    hash: Optional[str] = ""
    poster: Optional[str] = ""
    category: Optional[str] = ""
    site: Optional[str] = ""
    url: Optional[str] = ""


class TorrentOut(BaseModel):
    id: int
    name: str
    size: str
    seeders: str
    leechers: str
    magnet: str
    hash: str
    poster: str
    category: str
    site: str
    url: str
    added_at: datetime

    class Config:
        from_attributes = True


def get_or_create_torrent(db: Session, payload: TorrentPayload) -> TorrentItem:
    """Reuse existing torrent record by hash, or create a new one."""
    if payload.hash:
        existing = db.query(TorrentItem).filter(TorrentItem.hash == payload.hash).first()
        if existing:
            return existing
    item = TorrentItem(**payload.dict())
    db.add(item)
    db.flush()
    return item


def torrent_out(t: TorrentItem) -> dict:
    return {
        "id": t.id, "name": t.name, "size": t.size or "",
        "seeders": t.seeders or "0", "leechers": t.leechers or "0",
        "magnet": t.magnet or "", "hash": t.hash or "",
        "poster": t.poster or "", "category": t.category or "",
        "site": t.site or "", "url": t.url or "",
        "added_at": t.added_at,
    }


# ─── Watchlist ───────────────────────────────────────────────────────────────

@router.get("/watchlist")
def get_watchlist(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    items = db.query(WatchlistItem).filter(WatchlistItem.user_id == user.id).all()
    return [{"list_id": i.id, "watched": i.watched, "added_at": i.added_at,
             "torrent": torrent_out(i.torrent)} for i in items if i.torrent]


@router.post("/watchlist", status_code=201)
def add_watchlist(payload: TorrentPayload,
                  user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    torrent = get_or_create_torrent(db, payload)
    if db.query(WatchlistItem).filter_by(user_id=user.id, torrent_id=torrent.id).first():
        raise HTTPException(400, "Already in watchlist")
    db.add(WatchlistItem(user_id=user.id, torrent_id=torrent.id))
    db.commit()
    return {"message": "Added to watchlist"}


@router.patch("/watchlist/{item_id}/watched")
def mark_watched(item_id: int, watched: bool = True,
                 user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    item = db.query(WatchlistItem).filter_by(id=item_id, user_id=user.id).first()
    if not item:
        raise HTTPException(404, "Not found")
    item.watched = watched
    db.commit()
    return {"message": "Updated"}


@router.delete("/watchlist/{item_id}", status_code=204)
def remove_watchlist(item_id: int,
                     user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    item = db.query(WatchlistItem).filter_by(id=item_id, user_id=user.id).first()
    if not item:
        raise HTTPException(404, "Not found")
    db.delete(item)
    db.commit()


# ─── Wishlist ────────────────────────────────────────────────────────────────

@router.get("/wishlist")
def get_wishlist(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    items = db.query(WishlistItem).filter(WishlistItem.user_id == user.id).all()
    return [{"list_id": i.id, "added_at": i.added_at,
             "torrent": torrent_out(i.torrent)} for i in items if i.torrent]


@router.post("/wishlist", status_code=201)
def add_wishlist(payload: TorrentPayload,
                 user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    torrent = get_or_create_torrent(db, payload)
    if db.query(WishlistItem).filter_by(user_id=user.id, torrent_id=torrent.id).first():
        raise HTTPException(400, "Already in wishlist")
    db.add(WishlistItem(user_id=user.id, torrent_id=torrent.id))
    db.commit()
    return {"message": "Added to wishlist"}


@router.delete("/wishlist/{item_id}", status_code=204)
def remove_wishlist(item_id: int,
                    user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    item = db.query(WishlistItem).filter_by(id=item_id, user_id=user.id).first()
    if not item:
        raise HTTPException(404, "Not found")
    db.delete(item)
    db.commit()


# ─── Watch Later ─────────────────────────────────────────────────────────────

@router.get("/watchlater")
def get_watchlater(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    items = db.query(WatchLaterItem).filter(WatchLaterItem.user_id == user.id).all()
    return [{"list_id": i.id, "added_at": i.added_at,
             "torrent": torrent_out(i.torrent)} for i in items if i.torrent]


@router.post("/watchlater", status_code=201)
def add_watchlater(payload: TorrentPayload,
                   user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    torrent = get_or_create_torrent(db, payload)
    if db.query(WatchLaterItem).filter_by(user_id=user.id, torrent_id=torrent.id).first():
        raise HTTPException(400, "Already in watch later")
    db.add(WatchLaterItem(user_id=user.id, torrent_id=torrent.id))
    db.commit()
    return {"message": "Added to watch later"}


@router.delete("/watchlater/{item_id}", status_code=204)
def remove_watchlater(item_id: int,
                      user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    item = db.query(WatchLaterItem).filter_by(id=item_id, user_id=user.id).first()
    if not item:
        raise HTTPException(404, "Not found")
    db.delete(item)
    db.commit()


# ─── Playlists ───────────────────────────────────────────────────────────────

class PlaylistCreate(BaseModel):
    name: str
    description: Optional[str] = ""

class PlaylistItemAdd(BaseModel):
    torrent: TorrentPayload
    position: Optional[int] = 0


@router.get("/playlists")
def get_playlists(user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    playlists = db.query(Playlist).filter(Playlist.user_id == user.id).all()
    return [{
        "id": p.id, "name": p.name, "description": p.description,
        "created_at": p.created_at, "item_count": len(p.items),
    } for p in playlists]


@router.post("/playlists", status_code=201)
def create_playlist(body: PlaylistCreate,
                    user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    pl = Playlist(user_id=user.id, name=body.name, description=body.description)
    db.add(pl)
    db.commit()
    db.refresh(pl)
    return {"id": pl.id, "name": pl.name, "description": pl.description}


@router.get("/playlists/{playlist_id}")
def get_playlist(playlist_id: int,
                 user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    pl = db.query(Playlist).filter_by(id=playlist_id, user_id=user.id).first()
    if not pl:
        raise HTTPException(404, "Playlist not found")
    items = sorted(pl.items, key=lambda x: x.position)
    return {
        "id": pl.id, "name": pl.name, "description": pl.description,
        "created_at": pl.created_at,
        "items": [{"item_id": i.id, "position": i.position,
                   "torrent": torrent_out(i.torrent)} for i in items if i.torrent],
    }


@router.post("/playlists/{playlist_id}/items", status_code=201)
def add_playlist_item(playlist_id: int, body: PlaylistItemAdd,
                      user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    pl = db.query(Playlist).filter_by(id=playlist_id, user_id=user.id).first()
    if not pl:
        raise HTTPException(404, "Playlist not found")
    torrent = get_or_create_torrent(db, body.torrent)
    db.add(PlaylistItem(playlist_id=pl.id, torrent_id=torrent.id, position=body.position))
    db.commit()
    return {"message": "Added to playlist"}


@router.delete("/playlists/{playlist_id}/items/{item_id}", status_code=204)
def remove_playlist_item(playlist_id: int, item_id: int,
                         user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    pl = db.query(Playlist).filter_by(id=playlist_id, user_id=user.id).first()
    if not pl:
        raise HTTPException(404, "Playlist not found")
    item = db.query(PlaylistItem).filter_by(id=item_id, playlist_id=pl.id).first()
    if not item:
        raise HTTPException(404, "Item not found")
    db.delete(item)
    db.commit()


@router.delete("/playlists/{playlist_id}", status_code=204)
def delete_playlist(playlist_id: int,
                    user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    pl = db.query(Playlist).filter_by(id=playlist_id, user_id=user.id).first()
    if not pl:
        raise HTTPException(404, "Playlist not found")
    db.delete(pl)
    db.commit()
