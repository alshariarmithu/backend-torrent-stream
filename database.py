"""
Database setup using SQLAlchemy + SQLite
"""
from sqlalchemy import (
    create_engine, Column, Integer, String, DateTime,
    ForeignKey, Text, Boolean
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

DATABASE_URL = "sqlite:///./torrentstream.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ─── Models ──────────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    watchlist = relationship("WatchlistItem", back_populates="user", cascade="all, delete")
    wishlist  = relationship("WishlistItem",  back_populates="user", cascade="all, delete")
    watchlater= relationship("WatchLaterItem",back_populates="user", cascade="all, delete")
    playlists = relationship("Playlist",      back_populates="user", cascade="all, delete")


class TorrentItem(Base):
    """Shared torrent metadata – referenced by list items."""
    __tablename__ = "torrent_items"
    id       = Column(Integer, primary_key=True, index=True)
    name     = Column(String,  nullable=False)
    size     = Column(String)
    seeders  = Column(String)
    leechers = Column(String)
    magnet   = Column(Text)
    hash     = Column(String, index=True)
    poster   = Column(String)
    category = Column(String)
    site     = Column(String)
    url      = Column(String)
    added_at = Column(DateTime, default=datetime.utcnow)


class WatchlistItem(Base):
    __tablename__ = "watchlist"
    id       = Column(Integer, primary_key=True, index=True)
    user_id  = Column(Integer, ForeignKey("users.id"), nullable=False)
    torrent_id = Column(Integer, ForeignKey("torrent_items.id"))
    watched  = Column(Boolean, default=False)
    added_at = Column(DateTime, default=datetime.utcnow)
    user     = relationship("User", back_populates="watchlist")
    torrent  = relationship("TorrentItem")


class WishlistItem(Base):
    __tablename__ = "wishlist"
    id       = Column(Integer, primary_key=True, index=True)
    user_id  = Column(Integer, ForeignKey("users.id"), nullable=False)
    torrent_id = Column(Integer, ForeignKey("torrent_items.id"))
    added_at = Column(DateTime, default=datetime.utcnow)
    user     = relationship("User", back_populates="wishlist")
    torrent  = relationship("TorrentItem")


class WatchLaterItem(Base):
    __tablename__ = "watchlater"
    id       = Column(Integer, primary_key=True, index=True)
    user_id  = Column(Integer, ForeignKey("users.id"), nullable=False)
    torrent_id = Column(Integer, ForeignKey("torrent_items.id"))
    added_at = Column(DateTime, default=datetime.utcnow)
    user     = relationship("User", back_populates="watchlater")
    torrent  = relationship("TorrentItem")


class Playlist(Base):
    __tablename__ = "playlists"
    id       = Column(Integer, primary_key=True, index=True)
    user_id  = Column(Integer, ForeignKey("users.id"), nullable=False)
    name     = Column(String, nullable=False)
    description = Column(String, default="")
    created_at  = Column(DateTime, default=datetime.utcnow)
    user     = relationship("User", back_populates="playlists")
    items    = relationship("PlaylistItem", back_populates="playlist", cascade="all, delete")


class PlaylistItem(Base):
    __tablename__ = "playlist_items"
    id          = Column(Integer, primary_key=True, index=True)
    playlist_id = Column(Integer, ForeignKey("playlists.id"), nullable=False)
    torrent_id  = Column(Integer, ForeignKey("torrent_items.id"))
    position    = Column(Integer, default=0)
    added_at    = Column(DateTime, default=datetime.utcnow)
    playlist    = relationship("Playlist", back_populates="items")
    torrent     = relationship("TorrentItem")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def init_db():
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
