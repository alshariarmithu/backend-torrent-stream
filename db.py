import os
from contextlib import asynccontextmanager
from pymongo import AsyncMongoClient
from beanie import init_beanie

from models import (
    User,
    TorrentItem,
    WatchlistItem,
    WishlistItem,
    WatchLaterItem,
    Playlist,
)

MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")
MONGODB_DB = os.getenv("MONGODB_DB", "torrentstream")

client: AsyncMongoClient | None = None


async def init_db() -> None:
    global client
    client = AsyncMongoClient(MONGODB_URL)
    db = client[MONGODB_DB]
    await init_beanie(
        database=db,
        document_models=[
            User,
            TorrentItem,
            WatchlistItem,
            WishlistItem,
            WatchLaterItem,
            Playlist,
        ],
    )


async def close_db() -> None:
    global client
    if client is not None:
        client.close()
        client = None


@asynccontextmanager
async def lifespan_context(_app):
    await init_db()
    try:
        yield
    finally:
        await close_db()
