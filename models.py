from __future__ import annotations

from datetime import datetime
from typing import Optional

from beanie import Document, Indexed
from pydantic import BaseModel, EmailStr, Field


class User(Document):
    email: Indexed(EmailStr, unique=True)  # type: ignore[valid-type]
    hashed_password: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "db"


class TorrentItem(Document):
    name: str
    size: str = ""
    seeders: str = "0"
    leechers: str = "0"
    magnet: str = ""
    hash: Indexed(str, unique=True) = ""  # type: ignore[valid-type]
    poster: str = ""
    category: str = ""
    site: str = ""
    url: str = ""
    added_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "torrent_items"


class WatchlistItem(Document):
    user_id: str
    torrent_id: str
    watched: bool = False
    added_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "watchlist"
        indexes = [[("user_id", 1), ("torrent_id", 1)]]


class WishlistItem(Document):
    user_id: str
    torrent_id: str
    added_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "wishlist"
        indexes = [[("user_id", 1), ("torrent_id", 1)]]


class WatchLaterItem(Document):
    user_id: str
    torrent_id: str
    added_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "watchlater"
        indexes = [[("user_id", 1), ("torrent_id", 1)]]


class PlaylistEntry(BaseModel):
    id: str = Field(default_factory=lambda: __import__("uuid").uuid4().hex)
    torrent_id: str
    position: int = 0
    added_at: datetime = Field(default_factory=datetime.utcnow)


class Playlist(Document):
    user_id: str
    name: str
    description: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    items: list[PlaylistEntry] = Field(default_factory=list)

    class Settings:
        name = "playlists"
        indexes = [[("user_id", 1), ("name", 1)]]


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


class PlaylistCreate(BaseModel):
    name: str
    description: Optional[str] = ""


class PlaylistItemAdd(BaseModel):
    torrent: TorrentPayload
    position: Optional[int] = 0


class CommunityPost(Document):
    user_id: str
    author_email: EmailStr
    caption: str = ""
    torrent: TorrentPayload
    score: int = 0
    upvote_count: int = 0
    downvote_count: int = 0
    comment_count: int = 0
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "community_posts"


class CommunityComment(Document):
    post_id: str
    user_id: str
    author_email: EmailStr
    content: str
    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "community_comments"
        indexes = [[("post_id", 1), ("created_at", 1)]]


class CommunityPostVote(Document):
    post_id: str
    user_id: str
    value: int
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Settings:
        name = "community_post_votes"
        indexes = [
            [("post_id", 1), ("user_id", 1)],
        ]


class CommunityPostCreate(BaseModel):
    caption: Optional[str] = ""
    torrent: TorrentPayload


class CommunityCommentCreate(BaseModel):
    content: str


class CommunityVoteUpdate(BaseModel):
    value: int
