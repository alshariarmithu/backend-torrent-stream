import os
from contextlib import asynccontextmanager
from motor.motor_asyncio import AsyncIOMotorClient
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

client: AsyncIOMotorClient | None = None


async def init_db() -> None:
    global client
    client = AsyncIOMotorClient(MONGODB_URL)
    await init_beanie(
        database=client[MONGODB_DB],
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
    yield
    await close_db()
