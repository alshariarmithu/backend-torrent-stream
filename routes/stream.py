"""
Torrent Streaming via libtorrent
Supports HTTP Range requests so AVPlayer / iOS VideoPlayer can seek.

Install libtorrent:
  macOS:  brew install libtorrent-rasterbar && pip install python-libtorrent
  Ubuntu: apt-get install python3-libtorrent
  Docker: use image with libtorrent pre-installed (e.g. wernight/qbittorrent)

iOS integration:
  1. POST /stream/start        { magnet, hash }  → starts download
  2. GET  /stream/status/{hash}                  → poll until progress > 1%
  3. GET  /stream/info/{hash}                    → get filename + content-type
  4. Construct stream URL: GET /stream/{hash}?magnet=<encoded>
     Pass this URL directly to AVPlayer / VideoPlayer
"""
from fastapi import APIRouter, Depends, HTTPException, Header, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import os, time, threading, mimetypes
from pathlib import Path
from typing import Optional

from auth import get_current_user
from database import User

router = APIRouter()

DOWNLOAD_DIR = Path(os.getenv("TORRENT_DOWNLOAD_DIR", "/tmp/torrentstream"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".webm", ".ts", ".mpg", ".mpeg"}

# In-memory session store: hash → libtorrent handle
_sessions: dict = {}
_lock = threading.Lock()


# ─── libtorrent helpers ───────────────────────────────────────────────────────

def _get_lt():
    """
    Lazy-init shared libtorrent session.
    Returns (session, lt_module) or raises HTTP 503.
    """
    try:
        import libtorrent as lt
    except ImportError:
        return None, None

    if not hasattr(_get_lt, "_ses"):
        ses = lt.session()
        ses.listen_on(6881, 6891)
        # Compatible settings API (works on libtorrent 1.x and 2.x)
        try:
            settings = ses.get_settings()
            settings["active_downloads"] = 10
            settings["active_limit"] = 15
            ses.apply_settings(settings)
        except Exception:
            pass
        _get_lt._ses = ses
        _get_lt._lt  = lt

    return _get_lt._ses, _get_lt._lt


def _add_or_get(magnet: str, torrent_hash: str):
    """Add magnet to libtorrent session and return handle (idempotent)."""
    ses, lt = _get_lt()
    if ses is None:
        return None

    with _lock:
        if torrent_hash in _sessions:
            return _sessions[torrent_hash]

        save_path = str(DOWNLOAD_DIR / torrent_hash)
        os.makedirs(save_path, exist_ok=True)

        try:
            # libtorrent 2.x
            params = lt.parse_magnet_uri(magnet)
            params.save_path = save_path
        except AttributeError:
            # libtorrent 1.x fallback
            params = {
                "url": magnet,
                "save_path": save_path,
                "storage_mode": lt.storage_mode_t.storage_mode_sparse,
            }

        handle = ses.add_torrent(params)
        handle.set_sequential_download(True)
        # priority 7 = highest
        handle.set_priority(7)
        _sessions[torrent_hash] = handle
        return handle


def _wait_metadata(handle, timeout: int = 60) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if handle.has_metadata():
            return True
        time.sleep(0.5)
    return False


def _best_video(handle) -> Optional[Path]:
    """Return path to the largest video file inside the torrent."""
    if not handle.has_metadata():
        return None
    ti        = handle.get_torrent_info()
    files     = ti.files()
    save_path = Path(handle.save_path())
    best, best_size = None, 0
    for i in range(files.num_files()):
        path = files.file_path(i)
        size = files.file_size(i)
        if Path(path).suffix.lower() in VIDEO_EXTS and size > best_size:
            best, best_size = save_path / path, size
    return best


# ─── Request schemas ─────────────────────────────────────────────────────────

class StartRequest(BaseModel):
    magnet: str
    hash: str


# ─── Routes ──────────────────────────────────────────────────────────────────

@router.post("/start")
def start_torrent(body: StartRequest, _user: User = Depends(get_current_user)):
    """Start (or resume) downloading a torrent."""
    ses, _ = _get_lt()
    if ses is None:
        raise HTTPException(503,
            "libtorrent not installed. "
            "macOS: brew install libtorrent-rasterbar && pip install python-libtorrent | "
            "Ubuntu: apt-get install python3-libtorrent"
        )
    handle = _add_or_get(body.magnet, body.hash)
    if handle is None:
        raise HTTPException(500, "Failed to start torrent")
    return {"status": "started", "hash": body.hash}


@router.get("/status/{torrent_hash}")
def torrent_status(torrent_hash: str, _user: User = Depends(get_current_user)):
    """
    Poll download progress.
    iOS: poll until progress > 0.5 before opening AVPlayer.
    """
    ses, _ = _get_lt()
    if ses is None:
        raise HTTPException(503, "libtorrent not installed")

    with _lock:
        handle = _sessions.get(torrent_hash)
    if not handle:
        raise HTTPException(404, "Torrent not started – POST /stream/start first")

    s = handle.status()
    return {
        "hash":          torrent_hash,
        "progress":      round(s.progress * 100, 2),   # 0-100
        "download_rate": s.download_rate,               # bytes/s
        "upload_rate":   s.upload_rate,
        "num_peers":     s.num_peers,
        "state":         str(s.state),
        "has_metadata":  handle.has_metadata(),
        "paused":        s.paused,
    }


@router.get("/info/{torrent_hash}")
def torrent_info(torrent_hash: str, _user: User = Depends(get_current_user)):
    """
    Returns filename and content-type of the video file.
    iOS: call this after metadata is ready to pre-configure AVPlayer.
    """
    with _lock:
        handle = _sessions.get(torrent_hash)
    if not handle:
        raise HTTPException(404, "Torrent not started")
    if not _wait_metadata(handle, timeout=60):
        raise HTTPException(504, "Timed out waiting for metadata")

    video = _best_video(handle)
    if not video:
        raise HTTPException(404, "No video file found in torrent")

    content_type = mimetypes.guess_type(str(video))[0] or "video/mp4"
    ti = handle.get_torrent_info()
    return {
        "filename":     video.name,
        "content_type": content_type,
        "total_size":   ti.total_size(),
        "hash":         torrent_hash,
    }


@router.get("/{torrent_hash}")
async def stream_torrent(
    torrent_hash: str,
    magnet: Optional[str] = None,
    range: Optional[str] = Header(None),
    _user: User = Depends(get_current_user),
):
    """
    Stream the video file via HTTP with Range support (AVPlayer / seek-compatible).

    Usage from iOS:
      1. Encode your JWT as a header — BUT AVPlayer does not support custom headers.
         Solution: pass token as query param  ?token=<jwt>  and verify it here.
         OR: use a short-lived signed URL generated by /stream/sign/{hash}.

    For now the endpoint uses Bearer auth via the standard Depends.
    See /stream/sign/{hash} for a AVPlayer-compatible signed URL approach.
    """
    ses, _ = _get_lt()
    if ses is None:
        raise HTTPException(503, "libtorrent not installed on the server")

    # Auto-start if magnet provided and not yet running
    if magnet:
        with _lock:
            already = torrent_hash in _sessions
        if not already:
            _add_or_get(magnet, torrent_hash)

    with _lock:
        handle = _sessions.get(torrent_hash)
    if not handle:
        raise HTTPException(404, "Torrent not started. POST /stream/start first.")

    # Wait for metadata (up to 60 s)
    if not _wait_metadata(handle, timeout=60):
        raise HTTPException(504, "Timed out waiting for torrent metadata")

    video_path = _best_video(handle)
    if not video_path:
        raise HTTPException(404, "No video file found in torrent")

    # Wait until file exists on disk (libtorrent creates the file when first pieces arrive)
    for _ in range(30):
        if video_path.exists() and video_path.stat().st_size > 0:
            break
        time.sleep(1)
    else:
        raise HTTPException(404, f"Video file not yet on disk: {video_path.name}")

    file_size    = video_path.stat().st_size
    content_type = mimetypes.guess_type(str(video_path))[0] or "video/mp4"
    chunk_size   = 1024 * 1024  # 1 MB chunks

    # ── Range parsing ──────────────────────────────────────────────────────
    start, end = 0, file_size - 1
    is_range   = False

    if range:
        try:
            range_val = range.replace("bytes=", "")
            s, e = range_val.split("-")
            start    = int(s)
            end      = int(e) if e.strip() else file_size - 1
            is_range = True
        except Exception:
            pass  # ignore malformed range, serve from 0

    # Clamp
    start = max(0, min(start, file_size - 1))
    end   = max(start, min(end, file_size - 1))
    length = end - start + 1

    def iterfile():
        with open(video_path, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                data = f.read(min(chunk_size, remaining))
                if not data:
                    break
                yield data
                remaining -= len(data)

    headers = {
        "Content-Range":       f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges":       "bytes",
        "Content-Length":      str(length),
        "Content-Disposition": f'inline; filename="{video_path.name}"',
        "Cache-Control":       "no-cache",
    }

    return StreamingResponse(
        iterfile(),
        status_code=206 if is_range else 200,
        headers=headers,
        media_type=content_type,
    )


@router.delete("/{torrent_hash}")
def remove_torrent(
    torrent_hash: str,
    delete_files: bool = False,
    _user: User = Depends(get_current_user),
):
    """Stop torrent and optionally delete downloaded files."""
    ses, lt = _get_lt()
    if ses is None:
        raise HTTPException(503, "libtorrent not installed")

    with _lock:
        handle = _sessions.pop(torrent_hash, None)

    if handle and lt:
        try:
            # Compatible with both libtorrent 1.x and 2.x
            flag = getattr(lt, "remove_flags_t", None)
            if flag and delete_files:
                ses.remove_torrent(handle, flag.delete_files)
            else:
                ses.remove_torrent(handle, 1 if delete_files else 0)
        except Exception:
            ses.remove_torrent(handle)

    return {"message": "Removed", "hash": torrent_hash, "files_deleted": delete_files}