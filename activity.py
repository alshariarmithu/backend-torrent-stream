from __future__ import annotations

from datetime import datetime

from pymongo.errors import PyMongoError

from models import TorrentItem, TorrentPayload, UserRecentTorrent


def _payload_kwargs(payload: TorrentPayload) -> dict:
    return {
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
    }


async def touch_torrent(payload: TorrentPayload, hit_increment: int = 0) -> TorrentItem | None:
    now = datetime.utcnow()
    try:
        existing = None
        if payload.hash:
            existing = await TorrentItem.find_one(TorrentItem.hash == payload.hash)
        if existing:
            existing.name = payload.name
            existing.size = payload.size or ""
            existing.seeders = payload.seeders or "0"
            existing.leechers = payload.leechers or "0"
            existing.magnet = payload.magnet or ""
            existing.poster = payload.poster or ""
            existing.category = payload.category or ""
            existing.site = payload.site or ""
            existing.url = payload.url or ""
            if hit_increment:
                existing.hit_count += hit_increment
                existing.last_hit_at = now
            await existing.save()
            return existing

        item = TorrentItem(
            **_payload_kwargs(payload),
            hit_count=max(0, hit_increment),
            last_hit_at=now if hit_increment else None,
        )
        await item.insert()
        return item
    except PyMongoError:
        return None


async def record_recent_torrent(user_id: str, torrent_id: str, source: str = "search") -> None:
    now = datetime.utcnow()
    try:
        existing = await UserRecentTorrent.find_one(
            UserRecentTorrent.user_id == user_id,
            UserRecentTorrent.torrent_id == torrent_id,
        )
        if existing:
            existing.hit_count += 1
            existing.last_seen_at = now
            existing.source = source
            await existing.save()
            return

        recent = UserRecentTorrent(
            user_id=user_id,
            torrent_id=torrent_id,
            source=source,
            hit_count=1,
            last_seen_at=now,
        )
        await recent.insert()
    except PyMongoError:
        return
