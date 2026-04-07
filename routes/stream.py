"""
Torrent Streaming via libtorrent
Supports HTTP Range requests so AVPlayer can seek.

Requirements:
  macOS:  brew install libtorrent-rasterbar && pip install python-libtorrent
  Ubuntu: apt-get install python3-libtorrent
  Docker: use image with libtorrent pre-installed
"""
from fastapi import APIRouter, Depends, HTTPException, Header, Request
from fastapi.responses import StreamingResponse, JSONResponse
import os, time, threading, mimetypes
from pathlib import Path
from typing import Optional

from auth import get_current_user
from database import User

router = APIRouter()

DOWNLOAD_DIR = Path(os.getenv("TORRENT_DOWNLOAD_DIR", "/tmp/torrentstream"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# In-memory session store: hash → libtorrent handle
_sessions: dict = {}
_lock = threading.Lock()

# ─── libtorrent helpers ───────────────────────────────────────────────────────

def _get_session():
    """Lazy-init a single shared libtorrent session."""
    try:
        import libtorrent as lt
    except ImportError:
        return None, None
    if not hasattr(_get_session, "_ses"):
        ses = lt.session()
        ses.listen_on(6881, 6891)
        settings = ses.get_settings()
        settings['active_downloads'] = 10
        ses.apply_settings(settings)
        _get_session._ses = ses
        _get_session._lt  = lt
    return _get_session._ses, _get_session._lt


def _add_or_get_torrent(magnet: str, torrent_hash: str):
    """Add a magnet link to libtorrent and return the handle."""
    ses, lt = _get_session()
    if ses is None:
        return None

    with _lock:
        if torrent_hash in _sessions:
            return _sessions[torrent_hash]

        save_path = str(DOWNLOAD_DIR / torrent_hash)
        os.makedirs(save_path, exist_ok=True)

        params = lt.parse_magnet_uri(magnet)
        params.save_path = save_path
        params.storage_mode = lt.storage_mode_t.storage_mode_sparse

        handle = ses.add_torrent(params)
        handle.set_sequential_download(True)
        handle.set_priority(7)
        _sessions[torrent_hash] = handle
        return handle


def _wait_for_metadata(handle, timeout: int = 60) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if handle.has_metadata():
            return True
        time.sleep(0.5)
    return False


def _largest_video_file(handle) -> Optional[Path]:
    """Return the path to the largest video-like file in the torrent."""
    VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".webm", ".ts"}
    ti = handle.get_torrent_info()
    best = None
    best_size = 0
    save_path = Path(handle.save_path())

    for i in range(ti.num_files()):
        f = ti.files().file_path(i)
        size = ti.files().file_size(i)
        ext = Path(f).suffix.lower()
        if ext in VIDEO_EXTS and size > best_size:
            best = save_path / f
            best_size = size
    return best


# ─── Routes ──────────────────────────────────────────────────────────────────

@router.post("/start")
def start_torrent(
    magnet: str,
    torrent_hash: str,
    _user: User = Depends(get_current_user),
):
    """Start downloading a torrent in the background."""
    ses, lt = _get_session()
    if ses is None:
        raise HTTPException(503, "libtorrent not installed on server")

    handle = _add_or_get_torrent(magnet, torrent_hash)
    if handle is None:
        raise HTTPException(500, "Failed to start torrent")

    return {"status": "started", "hash": torrent_hash}


@router.get("/status/{torrent_hash}")
def torrent_status(torrent_hash: str, _user: User = Depends(get_current_user)):
    """Returns download progress and state."""
    ses, lt = _get_session()
    if ses is None:
        raise HTTPException(503, "libtorrent not installed")

    with _lock:
        handle = _sessions.get(torrent_hash)
    if not handle:
        raise HTTPException(404, "Torrent not started")

    s = handle.status()
    return {
        "hash": torrent_hash,
        "progress": round(s.progress * 100, 2),
        "download_rate": s.download_rate,
        "upload_rate": s.upload_rate,
        "num_peers": s.num_peers,
        "state": str(s.state),
        "has_metadata": handle.has_metadata(),
        "paused": s.paused,
    }


@router.get("/{torrent_hash}")
async def stream_torrent(
    torrent_hash: str,
    magnet: Optional[str] = None,
    request: Request = None,
    range: Optional[str] = Header(None),
    _user: User = Depends(get_current_user),
):
    """
    Stream a torrent file via HTTP.
    Pass ?magnet=<encoded_magnet> on first request to start the torrent.
    Supports HTTP Range headers for seeking (AVPlayer compatible).
    """
    ses, lt = _get_session()
    if ses is None:
        raise HTTPException(503,
            "libtorrent is not installed on the server. "
            "Install it with: pip install python-libtorrent (macOS: brew install libtorrent-rasterbar first)"
        )

    # Start torrent if needed
    if magnet and torrent_hash not in _sessions:
        _add_or_get_torrent(magnet, torrent_hash)

    with _lock:
        handle = _sessions.get(torrent_hash)
    if not handle:
        raise HTTPException(404, "Torrent not started. POST /stream/start first with magnet link.")

    # Wait for metadata (up to 60s)
    if not _wait_for_metadata(handle, timeout=60):
        raise HTTPException(504, "Timed out waiting for torrent metadata")

    video_path = _largest_video_file(handle)
    if not video_path:
        raise HTTPException(404, "No video file found in torrent")

    # Wait until the file exists and has some data
    waited = 0
    while waited < 30:
        if video_path.exists() and video_path.stat().st_size > 0:
            break
        time.sleep(1)
        waited += 1

    if not video_path.exists():
        raise HTTPException(404, f"Video file not yet available: {video_path.name}")

    file_size = video_path.stat().st_size
    content_type = mimetypes.guess_type(str(video_path))[0] or "video/mp4"

    # ── Range handling ──
    start = 0
    end   = file_size - 1
    chunk = 1024 * 1024  # 1 MB

    if range:
        try:
            range_val = range.replace("bytes=", "")
            s, e = range_val.split("-")
            start = int(s)
            end   = int(e) if e else file_size - 1
        except Exception:
            pass

    length = end - start + 1

    def iterfile():
        with open(video_path, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                read_size = min(chunk, remaining)
                data = f.read(read_size)
                if not data:
                    break
                yield data
                remaining -= len(data)

    status_code = 206 if range else 200
    headers = {
        "Content-Range":  f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges":  "bytes",
        "Content-Length": str(length),
        "Content-Disposition": f'inline; filename="{video_path.name}"',
    }

    return StreamingResponse(
        iterfile(),
        status_code=status_code,
        headers=headers,
        media_type=content_type,
    )


@router.delete("/{torrent_hash}")
def remove_torrent(torrent_hash: str, delete_files: bool = False,
                   _user: User = Depends(get_current_user)):
    ses, lt = _get_session()
    if ses is None:
        raise HTTPException(503, "libtorrent not installed")

    with _lock:
        handle = _sessions.pop(torrent_hash, None)

    if handle:
        option = lt.options_t.delete_files if delete_files else lt.options_t(0)
        ses.remove_torrent(handle, option)

    return {"message": "Removed", "hash": torrent_hash}
