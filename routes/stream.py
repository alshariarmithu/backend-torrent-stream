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
import os, time, threading, mimetypes, asyncio, json
from pathlib import Path
from typing import Optional
from urllib.parse import quote
from auth import decode_token, get_current_user
from models import User

router = APIRouter()

DOWNLOAD_DIR = Path(os.getenv("TORRENT_DOWNLOAD_DIR", "/tmp/torrentstream"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".webm", ".ts", ".mpg", ".mpeg"}
SUBTITLE_EXTS = {".srt", ".vtt", ".ass", ".ssa", ".sub", ".idx"}
PREFERRED_VIDEO_EXTS = (".mp4", ".m4v", ".mov", ".webm", ".mkv", ".avi", ".ts", ".mpg", ".mpeg")


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


def _wait_metadata(handle, timeout: int = 60) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if handle.has_metadata():
            return True
        time.sleep(0.5)
    return False


def _best_video(handle) -> Optional[Path]:
    """Return path to the largest video file inside the torrent."""
    info = _best_video_info(handle)
    if not info:
        return None
    return Path(info["full_path"])


def _best_video_info(handle) -> Optional[dict]:
    """Return metadata about the largest video file in torrent."""
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
        if ext not in VIDEO_EXTS:
            continue

        ext_rank = PREFERRED_VIDEO_EXTS.index(ext) if ext in PREFERRED_VIDEO_EXTS else len(PREFERRED_VIDEO_EXTS)
        content_type = mimetypes.guess_type(str(path))[0] or "video/mp4"

        candidate = {
            "index": i,
            "name": Path(path).name,
            "path": str(path),
            "size": int(size),
            "full_path": str(save_path / path),
            "ext": ext,
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
    if not _wait_metadata(handle, timeout=METADATA_TIMEOUT_SEC):
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
    token: Optional[str] = None,
    range: Optional[str] = Header(None),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
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
    await _authorize_stream_request(token=token, authorization=authorization)

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
    if not _wait_metadata(handle, timeout=METADATA_TIMEOUT_SEC):
        raise HTTPException(504, "Timed out waiting for torrent metadata")

    video = _best_video_info(handle)
    if not video:
        raise HTTPException(404, "No video file found in torrent")

    subtitle_tracks = _subtitle_tracks(handle)
    _prioritize_files(handle, video.get("index"), [track["index"] for track in subtitle_tracks])
    video_path = Path(video["full_path"])

    # Wait until file exists on disk (libtorrent creates the file when first pieces arrive)
    for _ in range(FILE_WAIT_TIMEOUT_SEC):
        if video_path.exists():
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
                _add_or_get(magnet, torrent_hash)
    else:
        with _lock:
            if torrent_hash not in _sessions:
                raise HTTPException(
                    400,
                    "Magnet link required: ?magnet=<encoded_link>"
                )

    with _lock:
        handle = _sessions.get(torrent_hash)
    if not handle:
        raise HTTPException(404, "Torrent not started")

    # Heavy preparation on server: wait metadata, choose best video, scan subtitles.
    prepared = _wait_metadata(handle, timeout=PREPARE_TIMEOUT_SEC)
    video = _best_video_info(handle) if prepared else None
    subtitle_tracks = _subtitle_tracks(handle) if prepared else []
    if video:
        _prioritize_files(handle, video.get("index"), [track["index"] for track in subtitle_tracks])

    base_url = str(request.base_url).rstrip("/")
    stream_path = f"/stream/{torrent_hash}"
    content_type = video["content_type"] if video else None

    return {
        "status": "ready" if prepared and video else "preparing",
        "prepared": bool(prepared and video),
        "hash": torrent_hash,
        "stream_path": stream_path,
        "stream_url": f"{base_url}{stream_path}",
        "content_type": content_type,
        "magnet": magnet,
        "selected_video": {
            "name": video["name"],
            "path": video["path"],
            "size": video["size"],
            "content_type": content_type,
        } if video else None,
        "subtitles_available": len(subtitle_tracks) > 0,
        "subtitle_tracks": subtitle_tracks,
        "message": (
            "Torrent prepared. Stream link ready for AVPlayer."
            if prepared and video
            else "Torrent session started. Metadata is still loading; stream may take longer to start."
        )
    }


async def _stream_wait_ws_impl(websocket: WebSocket, torrent_hash: str):
    """Websocket progress channel for torrent preparation and stream readiness."""
    token = websocket.query_params.get("token")
    magnet = websocket.query_params.get("magnet")

    if not token:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Missing auth token")
        return

    try:
        payload = decode_token(token)
        user_id = payload.get("sub")
        user = await User.get(user_id) if user_id else None
        if not user:
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="User not found")
            return
    except Exception:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="Invalid token")
        return

    await websocket.accept()

    ses, _ = _get_lt()
    if ses is None:
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
            handle = _add_or_get(magnet, torrent_hash)

    if not handle:
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
                video = _best_video_info(handle)
                if video:
                    subtitle_tracks = _subtitle_tracks(handle)
                    _prioritize_files(handle, video.get("index"), [track["index"] for track in subtitle_tracks])

                    base_url = _http_base_from_websocket(websocket)
                    stream_url = f"{base_url}/stream/{torrent_hash}?token={quote(token, safe='')}"
                    content_type = video["content_type"]

                    await websocket.send_json({
                        "type": "ready",
                        "hash": torrent_hash,
                        "stream_url": stream_url,
                        "content_type": content_type,
                        "selected_video": {
                            "name": video["name"],
                            "path": video["path"],
                            "size": video["size"],
                            "content_type": content_type,
                        },
                        "subtitles_available": len(subtitle_tracks) > 0,
                        "subtitle_tracks": subtitle_tracks,
                        "message": "Stream ready. Start playback now.",
                    })
                    await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
                    return

            if time.time() - started_at > METADATA_TIMEOUT_SEC:
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