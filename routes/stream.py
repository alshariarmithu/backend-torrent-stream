"""
Torrent Streaming via libtorrent
Supports HTTP Range requests so AVPlayer / iOS VideoPlayer can seek.

Install libtorrent:
  macOS:  brew install libtorrent-rasterbar && pip install python-libtorrent
  Ubuntu: apt-get install python3-libtorrent
  Docker: use image with libtorrent pre-installed (e.g. wernight/qbittorrent)

iOS integration (new streamlined flow):
  NEW: GET /stream/magnet/{hash}?magnet=<encoded>
       → auto-starts torrent + returns streaming URL
       → iOS app passes URL directly to AVPlayer
  
  Legacy polling-based flow:
  1. POST /stream/start        { magnet, hash }  → starts download
  2. GET  /stream/status/{hash}                  → poll until progress > 1%
  3. GET  /stream/info/{hash}                    → get filename + content-type
  4. Construct stream URL: GET /stream/{hash}?magnet=<encoded>
     Pass this URL directly to AVPlayer / VideoPlayer
"""
from fastapi import APIRouter, Depends, HTTPException, Header, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import logging
import os, time, threading, mimetypes, asyncio, json
from pathlib import Path
from typing import Optional
from urllib.parse import quote
from auth import decode_token, get_current_user
from models import User

router = APIRouter()
logger = logging.getLogger(__name__)

DOWNLOAD_DIR = Path(os.getenv("TORRENT_DOWNLOAD_DIR", "/tmp/torrentstream"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".webm", ".ts", ".mpg", ".mpeg"}
AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".wav", ".flac", ".ogg", ".oga", ".opus", ".weba"}
MEDIA_EXTS = VIDEO_EXTS | AUDIO_EXTS
SUBTITLE_EXTS = {".srt", ".vtt", ".ass", ".ssa", ".sub", ".idx"}
PREFERRED_VIDEO_EXTS = (".mp4", ".m4v", ".mov", ".webm", ".mkv", ".avi", ".ts", ".mpg", ".mpeg")
PREFERRED_AUDIO_EXTS = (".m4a", ".mp3", ".aac", ".opus", ".ogg", ".oga", ".wav", ".flac", ".weba")


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


PREPARE_TIMEOUT_SEC = _env_int("STREAM_PREPARE_TIMEOUT", 25)
METADATA_TIMEOUT_SEC = _env_int("STREAM_METADATA_TIMEOUT", 180)
FILE_WAIT_TIMEOUT_SEC = _env_int("STREAM_FILE_WAIT_TIMEOUT", 120)
STREAM_CHUNK_SIZE = _env_int("STREAM_CHUNK_SIZE", 32 * 1024)
STREAM_READY_MIN_BYTES = _env_int("STREAM_READY_MIN_BYTES", 64 * 1024)

# In-memory session store: hash → libtorrent handle
_sessions: dict = {}
_lock = threading.Lock()


async def _authorize_stream_request(token: Optional[str], authorization: Optional[str]) -> User:
    """Support AVPlayer token query auth and standard Bearer auth."""
    raw_token = token

    if not raw_token and authorization:
        parts = authorization.split(" ", 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            raw_token = parts[1].strip()

    if not raw_token:
        raise HTTPException(status_code=401, detail="Missing auth token")

    payload = decode_token(raw_token)
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    user = await User.get(user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")

    return user


# ─── libtorrent helpers ───────────────────────────────────────────────────────

def _get_lt():
    """
    Lazy-init shared libtorrent session.
    Returns (session, lt_module) or raises HTTP 503.
    """
    try:
        import libtorrent as lt  # type: ignore[import-not-found]
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


def _wait_metadata(handle, timeout: int = 60, label: str = "") -> bool:
    deadline = time.time() + timeout
    last_log = 0.0
    while time.time() < deadline:
        if handle.has_metadata():
            if label:
                logger.info("%s metadata ready", label)
            return True
        now = time.time()
        if label and now - last_log >= 5:
            remaining = max(0, int(deadline - now))
            logger.info("%s waiting for metadata (%ss remaining)", label, remaining)
            last_log = now
        time.sleep(0.5)
    if label:
        logger.warning("%s metadata wait timed out after %ss", label, timeout)
    return False


def _wait_file_ready(media_path: Path, timeout: int = 60, min_bytes: int = 1, label: str = "") -> int:
    deadline = time.time() + timeout
    last_log = 0.0

    while time.time() < deadline:
        try:
            size = media_path.stat().st_size
        except FileNotFoundError:
            size = 0

        if size >= min_bytes:
            if label:
                logger.info("%s file ready bytes=%s", label, size)
            return size

        now = time.time()
        if label and now - last_log >= 5:
            remaining = max(0, int(deadline - now))
            logger.info("%s waiting for file bytes (%s/%s, %ss remaining)", label, size, min_bytes, remaining)
            last_log = now

        time.sleep(0.5)

    if label:
        logger.warning("%s file wait timed out after %ss (min_bytes=%s)", label, timeout, min_bytes)
    return 0


def _best_video(handle) -> Optional[Path]:
    """Return path to the largest playable media file inside the torrent."""
    info = _best_video_info(handle)
    if not info:
        return None
    return Path(info["full_path"])


def _best_video_info(handle) -> Optional[dict]:
    """Return metadata about the best playable media file in the torrent."""
    if not handle.has_metadata():
        return None

    ti = handle.get_torrent_info()
    files = ti.files()
    save_path = Path(handle.save_path())
    best: Optional[dict] = None

    for i in range(files.num_files()):
        path = files.file_path(i)
        size = files.file_size(i)
        ext = Path(path).suffix.lower()
        if ext not in MEDIA_EXTS:
            continue

        kind = "video" if ext in VIDEO_EXTS else "audio"
        preferred_exts = PREFERRED_VIDEO_EXTS if kind == "video" else PREFERRED_AUDIO_EXTS
        ext_rank = preferred_exts.index(ext) if ext in preferred_exts else len(preferred_exts)
        content_type = mimetypes.guess_type(str(path))[0] or ("video/mp4" if kind == "video" else "audio/mpeg")

        candidate = {
            "index": i,
            "name": Path(path).name,
            "path": str(path),
            "size": int(size),
            "full_path": str(save_path / path),
            "ext": ext,
            "kind": kind,
            "ext_rank": ext_rank,
            "content_type": content_type,
        }

        if (
            best is None
            or candidate["ext_rank"] < best["ext_rank"]
            or (
                candidate["ext_rank"] == best["ext_rank"]
                and candidate["size"] > best["size"]
            )
        ):
            best = candidate
    return best


def _subtitle_tracks(handle) -> list[dict]:
    """List subtitle files available in torrent metadata."""
    if not handle.has_metadata():
        return []

    ti = handle.get_torrent_info()
    files = ti.files()
    save_path = Path(handle.save_path())
    tracks: list[dict] = []

    for i in range(files.num_files()):
        path = files.file_path(i)
        ext = Path(path).suffix.lower()
        if ext not in SUBTITLE_EXTS:
            continue

        tracks.append(
            {
                "index": i,
                "name": Path(path).name,
                "path": str(path),
                "size": int(files.file_size(i)),
                "ext": ext.lstrip("."),
                "downloaded": (save_path / path).exists(),
            }
        )

    return tracks


def _prioritize_files(handle, video_index: Optional[int], subtitle_indexes: list[int]) -> None:
    """Prioritize selected video and subtitle files for faster startup."""
    if not handle.has_metadata():
        return

    try:
        ti = handle.get_torrent_info()
        file_count = ti.files().num_files()
    except Exception:
        return

    priorities = [0] * file_count
    if video_index is not None and 0 <= video_index < file_count:
        priorities[video_index] = 7
    for idx in subtitle_indexes:
        if 0 <= idx < file_count and priorities[idx] < 5:
            priorities[idx] = 5

    try:
        handle.prioritize_files(priorities)
        return
    except Exception:
        pass

    for idx, priority in enumerate(priorities):
        if priority <= 0:
            continue
        try:
            handle.file_priority(idx, priority)
        except Exception:
            continue


def _http_base_from_websocket(websocket: WebSocket) -> str:
    """Build external HTTP base URL for a websocket client request."""
    forwarded_proto = websocket.headers.get("x-forwarded-proto")
    forwarded_host = websocket.headers.get("x-forwarded-host")
    if forwarded_proto and forwarded_host:
        proto = forwarded_proto.split(",")[0].strip()
        host = forwarded_host.split(",")[0].strip()
        return f"{proto}://{host}"

    ws_scheme = websocket.url.scheme
    http_scheme = "https" if ws_scheme == "wss" else "http"
    host = websocket.url.hostname or "localhost"
    port = websocket.url.port
    if port and not ((http_scheme == "http" and port == 80) or (http_scheme == "https" and port == 443)):
        return f"{http_scheme}://{host}:{port}"
    return f"{http_scheme}://{host}"


# ─── Request schemas ─────────────────────────────────────────────────────────

class StartRequest(BaseModel):
    magnet: str
    hash: str


# ─── Routes ──────────────────────────────────────────────────────────────────

@router.post("/start")
def start_torrent(body: StartRequest, _user: User = Depends(get_current_user)):
    """Start (or resume) downloading a torrent."""
    logger.info("stream start requested hash=%s magnet_len=%s", body.hash, len(body.magnet) if body.magnet else 0)
    ses, _ = _get_lt()
    if ses is None:
        raise HTTPException(503,
            "libtorrent not installed. "
            "macOS: brew install libtorrent-rasterbar && pip install python-libtorrent | "
            "Ubuntu: apt-get install python3-libtorrent"
        )
    handle = _add_or_get(body.magnet, body.hash)
    if handle is None:
        logger.error("stream start failed hash=%s", body.hash)
        raise HTTPException(500, "Failed to start torrent")
    logger.info("stream start completed hash=%s", body.hash)
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
    logger.info(
        "status requested hash=%s progress=%.2f peers=%s state=%s metadata=%s",
        torrent_hash,
        s.progress * 100,
        s.num_peers,
        s.state,
        handle.has_metadata(),
    )
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
    Returns filename and content-type of the best playable media file.
    iOS and browser clients can use this after metadata is ready to pre-configure playback.
    """
    with _lock:
        handle = _sessions.get(torrent_hash)
    if not handle:
        raise HTTPException(404, "Torrent not started")
    logger.info("info requested hash=%s waiting for metadata", torrent_hash)
    if not _wait_metadata(handle, timeout=METADATA_TIMEOUT_SEC, label=f"info hash={torrent_hash}"):
        raise HTTPException(504, "Timed out waiting for metadata")

    media = _best_video(handle)
    if not media:
        logger.warning("info no media found hash=%s", torrent_hash)
        raise HTTPException(404, "No media file found in torrent")

    content_type = mimetypes.guess_type(str(media))[0] or "video/mp4"
    ti = handle.get_torrent_info()
    logger.info(
        "info ready hash=%s filename=%s content_type=%s total_size=%s",
        torrent_hash,
        media.name,
        content_type,
        ti.total_size(),
    )
    return {
        "filename":     media.name,
        "content_type": content_type,
        "total_size":   ti.total_size(),
        "hash":         torrent_hash,
    }


@router.get("/{torrent_hash}")
async def stream_torrent(
    request: Request,
    torrent_hash: str,
    magnet: Optional[str] = None,
    token: Optional[str] = None,
    range_header: Optional[str] = Header(None, alias="Range"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
):
    """
    Stream the selected media file via HTTP with Range support (AVPlayer / browser seek-compatible).

    Usage from iOS:
      1. Encode your JWT as a header — BUT AVPlayer does not support custom headers.
         Solution: pass token as query param  ?token=<jwt>  and verify it here.
         OR: use a short-lived signed URL generated by /stream/sign/{hash}.

    For now the endpoint uses Bearer auth via the standard Depends.
    See /stream/sign/{hash} for a AVPlayer-compatible signed URL approach.
    """
    await _authorize_stream_request(token=token, authorization=authorization)

    client_host = request.client.host if request.client else "unknown"
    logger.info(
        "http stream requested hash=%s client=%s magnet=%s range=%s",
        torrent_hash,
        client_host,
        bool(magnet),
        range_header or "none",
    )
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
    if not _wait_metadata(handle, timeout=METADATA_TIMEOUT_SEC, label=f"http stream hash={torrent_hash}"):
        logger.warning("http stream metadata timeout hash=%s", torrent_hash)
        raise HTTPException(504, "Timed out waiting for torrent metadata")

    media = _best_video_info(handle)
    if not media:
        logger.warning("http stream no media found hash=%s", torrent_hash)
        raise HTTPException(404, "No media file found in torrent")

    subtitle_tracks = _subtitle_tracks(handle)
    _prioritize_files(handle, media.get("index"), [track["index"] for track in subtitle_tracks])
    media_path = Path(media["full_path"])
    logger.info(
        "http stream selected hash=%s file=%s kind=%s content_type=%s subtitles=%s",
        torrent_hash,
        media["name"],
        media["kind"],
        media["content_type"],
        len(subtitle_tracks),
    )

    ready_bytes = _wait_file_ready(
        media_path,
        timeout=FILE_WAIT_TIMEOUT_SEC,
        min_bytes=STREAM_READY_MIN_BYTES,
        label=f"http stream hash={torrent_hash}",
    )
    if ready_bytes <= 0:
        logger.warning("http stream file not ready hash=%s file=%s", torrent_hash, media_path.name)
        raise HTTPException(504, f"Media file not yet ready: {media_path.name}")

    file_size    = media_path.stat().st_size
    content_type = mimetypes.guess_type(str(media_path))[0] or ("video/mp4" if media["kind"] == "video" else "audio/mpeg")
    chunk_size   = max(16 * 1024, STREAM_CHUNK_SIZE)
    logger.info(
        "http stream chunk size hash=%s chunk_size=%s",
        torrent_hash,
        chunk_size,
    )

    # ── Range parsing ──────────────────────────────────────────────────────
    start, end = 0, file_size - 1
    is_range   = False

    if range_header:
        try:
            range_val = range_header.replace("bytes=", "")
            s, e = range_val.split("-")
            start    = int(s)
            end      = int(e) if e.strip() else file_size - 1
            is_range = True
        except Exception:
            logger.warning("http stream malformed range ignored hash=%s range=%s", torrent_hash, range_header)
            pass  # ignore malformed range, serve from 0

    # Clamp
    start = max(0, min(start, file_size - 1))
    end   = max(start, min(end, file_size - 1))
    length = end - start + 1

    def iterfile():
        with open(media_path, "rb") as f:
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
        "Content-Disposition": f'inline; filename="{media_path.name}"',
        "Cache-Control":       "no-cache",
    }

    return StreamingResponse(
        iterfile(),
        status_code=206 if is_range else 200,
        headers=headers,
        media_type=content_type,
    )


@router.get("/magnet/{torrent_hash}")
def stream_magnet(
    request: Request,
    torrent_hash: str,
    magnet: Optional[str] = None,
    _user: User = Depends(get_current_user),
):
    """
    Streamlined flow: auto-start torrent + return streaming URL.
    
    iOS Usage:
      GET /stream/magnet/{hash}?magnet=<encoded_magnet_link>
      Response: { "stream_url": "https://..." }
      
    The returned URL can be passed directly to AVPlayer without polling.
    The first request may block briefly while libtorrent initializes.
    """
    client_host = request.client.host if request.client else "unknown"
    logger.info(
        "stream magnet requested hash=%s client=%s magnet=%s",
        torrent_hash,
        client_host,
        bool(magnet),
    )
    ses, _ = _get_lt()
    if ses is None:
        raise HTTPException(
            503,
            "libtorrent not installed. "
            "macOS: brew install libtorrent-rasterbar && pip install python-libtorrent"
        )

    # Auto-start torrent if magnet provided and not yet running
    if magnet:
        with _lock:
            if torrent_hash not in _sessions:
                logger.info("stream magnet auto-starting hash=%s", torrent_hash)
                _add_or_get(magnet, torrent_hash)
    else:
        with _lock:
            if torrent_hash not in _sessions:
                logger.warning("stream magnet missing and no session hash=%s", torrent_hash)
                raise HTTPException(
                    400,
                    "Magnet link required: ?magnet=<encoded_link>"
                )

    with _lock:
        handle = _sessions.get(torrent_hash)
    if not handle:
        logger.error("stream magnet handle missing hash=%s", torrent_hash)
        raise HTTPException(404, "Torrent not started")

    # Heavy preparation on server: wait metadata, choose best media, scan subtitles.
    prepared = _wait_metadata(handle, timeout=PREPARE_TIMEOUT_SEC, label=f"stream magnet hash={torrent_hash}")
    media = _best_video_info(handle) if prepared else None
    subtitle_tracks = _subtitle_tracks(handle) if prepared else []
    if media:
        _prioritize_files(handle, media.get("index"), [track["index"] for track in subtitle_tracks])

    base_url = str(request.base_url).rstrip("/")
    stream_path = f"/stream/{torrent_hash}"
    content_type = media["content_type"] if media else None
    if media:
        logger.info(
            "stream magnet ready hash=%s media=%s kind=%s content_type=%s subtitles=%s stream_url=%s",
            torrent_hash,
            media["name"],
            media["kind"],
            content_type,
            len(subtitle_tracks),
            f"{base_url}{stream_path}",
        )
    else:
        logger.warning("stream magnet preparing hash=%s prepared=%s", torrent_hash, prepared)

    return {
        "status": "ready" if prepared and media else "preparing",
        "prepared": bool(prepared and media),
        "hash": torrent_hash,
        "stream_path": stream_path,
        "stream_url": f"{base_url}{stream_path}",
        "content_type": content_type,
        "magnet": magnet,
        "selected_video": {
            "name": media["name"],
            "path": media["path"],
            "size": media["size"],
            "kind": media["kind"],
            "content_type": content_type,
        } if media else None,
        "selected_media": {
            "name": media["name"],
            "path": media["path"],
            "size": media["size"],
            "kind": media["kind"],
            "content_type": content_type,
        } if media else None,
        "subtitles_available": len(subtitle_tracks) > 0,
        "subtitle_tracks": subtitle_tracks,
        "message": (
            "Torrent prepared. Stream link ready for playback."
            if prepared and media
            else "Torrent session started. Metadata is still loading; stream may take longer to start."
        )
    }


async def _stream_wait_ws_impl(websocket: WebSocket, torrent_hash: str):
    """Websocket progress channel for torrent preparation and stream readiness."""
    token = websocket.query_params.get("token")
    magnet = websocket.query_params.get("magnet")
    client_host = websocket.client.host if websocket.client else "unknown"
    logger.info("stream ws requested hash=%s client=%s token=%s magnet=%s", torrent_hash, client_host, bool(token), bool(magnet))

    if not token:
        logger.warning("stream ws missing token hash=%s client=%s", torrent_hash, client_host)
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Missing auth token")
        return

    try:
        payload = decode_token(token)
        user_id = payload.get("sub")
        user = await User.get(user_id) if user_id else None
        if not user:
            logger.warning("stream ws user not found hash=%s client=%s", torrent_hash, client_host)
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="User not found")
            return
    except Exception:
        logger.warning("stream ws invalid token hash=%s client=%s", torrent_hash, client_host)
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token")
        return

    await websocket.accept()

    ses, _ = _get_lt()
    if ses is None:
        logger.error("stream ws libtorrent missing hash=%s", torrent_hash)
        await websocket.send_json({
            "type": "error",
            "message": "libtorrent not installed on the server",
        })
        await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
        return

    with _lock:
        handle = _sessions.get(torrent_hash)

    if not handle:
        if not magnet:
            logger.info("stream ws requesting magnet hash=%s", torrent_hash)
            await websocket.send_json({
                "type": "need_magnet",
                "hash": torrent_hash,
                "message": "Send {\"magnet\":\"...\"} as first websocket message",
            })
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=20)
                data = json.loads(raw)
                magnet = data.get("magnet") if isinstance(data, dict) else None
            except Exception:
                magnet = None

        if magnet:
            logger.info("stream ws auto-starting hash=%s", torrent_hash)
            handle = _add_or_get(magnet, torrent_hash)

    if not handle:
        logger.warning("stream ws no handle hash=%s", torrent_hash)
        await websocket.send_json({
            "type": "error",
            "message": "Magnet link required or torrent not started",
        })
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.send_json({
        "type": "started",
        "hash": torrent_hash,
        "message": "Torrent session started. Fetching nodes and metadata…",
    })
    logger.info("stream ws started hash=%s", torrent_hash)

    started_at = time.time()

    try:
        while True:
            state = handle.status()
            has_metadata = handle.has_metadata()

            await websocket.send_json({
                "type": "status",
                "hash": torrent_hash,
                "progress": float(state.progress),
                "download_rate": int(state.download_rate),
                "upload_rate": int(state.upload_rate),
                "num_peers": int(state.num_peers),
                "state": str(state.state),
                "has_metadata": has_metadata,
            })

            if has_metadata:
                media = _best_video_info(handle)
                if media:
                    subtitle_tracks = _subtitle_tracks(handle)
                    _prioritize_files(handle, media.get("index"), [track["index"] for track in subtitle_tracks])

                    media_path = Path(media["full_path"])
                    current_bytes = media_path.stat().st_size if media_path.exists() else 0

                    if current_bytes < STREAM_READY_MIN_BYTES:
                        await websocket.send_json({
                            "type": "status",
                            "hash": torrent_hash,
                            "progress": float(state.progress),
                            "download_rate": int(state.download_rate),
                            "upload_rate": int(state.upload_rate),
                            "num_peers": int(state.num_peers),
                            "state": str(state.state),
                            "has_metadata": has_metadata,
                            "file_bytes": current_bytes,
                            "ready_bytes": STREAM_READY_MIN_BYTES,
                            "file_ready": False,
                            "selected_media": {
                                "name": media["name"],
                                "path": media["path"],
                                "size": media["size"],
                                "kind": media["kind"],
                                "content_type": media["content_type"],
                            },
                        })
                        logger.info(
                            "stream ws buffering hash=%s media=%s kind=%s bytes=%s/%s",
                            torrent_hash,
                            media["name"],
                            media["kind"],
                            current_bytes,
                            STREAM_READY_MIN_BYTES,
                        )
                        await asyncio.sleep(1)
                        continue

                    base_url = _http_base_from_websocket(websocket)
                    stream_url = f"{base_url}/stream/{torrent_hash}?token={quote(token, safe='')}"
                    content_type = media["content_type"]

                    await websocket.send_json({
                        "type": "ready",
                        "hash": torrent_hash,
                        "stream_url": stream_url,
                        "content_type": content_type,
                        "selected_video": {
                            "name": media["name"],
                            "path": media["path"],
                            "size": media["size"],
                            "kind": media["kind"],
                            "content_type": content_type,
                        },
                        "selected_media": {
                            "name": media["name"],
                            "path": media["path"],
                            "size": media["size"],
                            "kind": media["kind"],
                            "content_type": content_type,
                        },
                        "subtitles_available": len(subtitle_tracks) > 0,
                        "subtitle_tracks": subtitle_tracks,
                        "message": "Stream ready. Start playback now.",
                    })
                    logger.info(
                        "stream ws ready hash=%s media=%s kind=%s content_type=%s subtitles=%s stream_url=%s bytes=%s",
                        torrent_hash,
                        media["name"],
                        media["kind"],
                        content_type,
                        len(subtitle_tracks),
                        stream_url,
                        current_bytes,
                    )
                    await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
                    return

            if time.time() - started_at > METADATA_TIMEOUT_SEC:
                logger.warning("stream ws metadata timeout hash=%s", torrent_hash)
                await websocket.send_json({
                    "type": "error",
                    "message": "Timed out waiting for torrent metadata",
                })
                await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                return

            await asyncio.sleep(1)
    except WebSocketDisconnect:
        return


@router.websocket("/ws/{torrent_hash}")
async def stream_wait_ws_path(websocket: WebSocket, torrent_hash: str):
    await _stream_wait_ws_impl(websocket, torrent_hash)


@router.websocket("/ws")
async def stream_wait_ws_query(websocket: WebSocket):
    torrent_hash = websocket.query_params.get("hash")
    if not torrent_hash:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Missing hash")
        return
    await _stream_wait_ws_impl(websocket, torrent_hash)


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