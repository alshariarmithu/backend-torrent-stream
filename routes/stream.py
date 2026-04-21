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
from fastapi.responses import PlainTextResponse, StreamingResponse
from pydantic import BaseModel
import logging
import os, time, threading, mimetypes, asyncio, json, shutil, subprocess, gc
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
BROWSER_DIRECT_VIDEO_EXTS = {".mp4", ".m4v", ".mov", ".webm", ".ogv"}
BROWSER_DIRECT_AUDIO_EXTS = {".mp3", ".m4a", ".aac", ".wav", ".ogg", ".oga", ".opus", ".weba"}


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
STREAM_READY_MIN_BYTES = _env_int("STREAM_READY_MIN_BYTES", 8 * 1024 * 1024)
HLS_READY_MIN_BYTES = _env_int("HLS_READY_MIN_BYTES", 24 * 1024 * 1024)
HLS_READY_TIMEOUT_SEC = _env_int("HLS_READY_TIMEOUT", 120)
HLS_SEGMENT_TIME_SEC = _env_int("HLS_SEGMENT_TIME", 6)
HLS_LIST_SIZE = _env_int("HLS_LIST_SIZE", 8)
HLS_PROBE_SIZE_BYTES = _env_int("HLS_PROBE_SIZE_BYTES", 64 * 1024 * 1024)
HLS_ANALYZE_DURATION_US = _env_int("HLS_ANALYZE_DURATION_US", 30_000_000)
HLS_AUDIO_PROBE_MIN_BYTES = _env_int("HLS_AUDIO_PROBE_MIN_BYTES", 64 * 1024 * 1024)
STREAM_PROGRESS_WAIT_TIMEOUT_SEC = _env_int("STREAM_PROGRESS_WAIT_TIMEOUT", 30)
STREAM_PROGRESS_POLL_INTERVAL_MS = _env_int("STREAM_PROGRESS_POLL_INTERVAL_MS", 250)
STREAM_RANGE_READY_MIN_BYTES = _env_int("STREAM_RANGE_READY_MIN_BYTES", 512 * 1024)
STREAM_RANGE_PRIORITY_WINDOW_BYTES = _env_int("STREAM_RANGE_PRIORITY_WINDOW_BYTES", 16 * 1024 * 1024)
STREAM_RANGE_PREFETCH_WINDOW_BYTES = _env_int("STREAM_RANGE_PREFETCH_WINDOW_BYTES", 8 * 1024 * 1024)
STREAM_RANGE_BACK_BUFFER_BYTES = _env_int("STREAM_RANGE_BACK_BUFFER_BYTES", 2 * 1024 * 1024)
STREAM_IDLE_TIMEOUT_SEC = _env_int("STREAM_IDLE_TIMEOUT", 15)
STREAM_PREPARING_IDLE_TIMEOUT_SEC = _env_int("STREAM_PREPARING_IDLE_TIMEOUT", 10)
STREAM_REAPER_INTERVAL_SEC = _env_int("STREAM_REAPER_INTERVAL", 3)
HLS_SESSION_IDLE_TIMEOUT_SEC = _env_int("HLS_SESSION_IDLE_TIMEOUT", 15)

# In-memory session store: hash → libtorrent handle
_sessions: dict = {}
_session_activity: dict = {}
_hls_sessions: dict = {}
_lock = threading.Lock()
_hls_lock = threading.Lock()

HLS_ROOT_DIR = DOWNLOAD_DIR / "_hls"
HLS_ROOT_DIR.mkdir(parents=True, exist_ok=True)


def _collect_garbage(reason: str) -> None:
    try:
        freed = gc.collect()
        logger.info("stream gc collected=%s reason=%s", freed, reason)
    except Exception:
        logger.exception("stream gc failed reason=%s", reason)


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
        _start_session_reaper()

    return _get_lt._ses, _get_lt._lt


def _session_timeout_for_handle(handle) -> int:
    try:
        has_metadata = bool(handle and handle.has_metadata())
    except Exception:
        has_metadata = False
    return STREAM_IDLE_TIMEOUT_SEC if has_metadata else STREAM_PREPARING_IDLE_TIMEOUT_SEC


def _touch_session_activity(torrent_hash: str) -> None:
    with _lock:
        meta = _session_activity.setdefault(
            torrent_hash,
            {
                "active_clients": 0,
                "last_activity": time.time(),
                "created_at": time.time(),
            },
        )
        meta["last_activity"] = time.time()


def _acquire_session_client(torrent_hash: str, reason: str) -> None:
    with _lock:
        meta = _session_activity.setdefault(
            torrent_hash,
            {
                "active_clients": 0,
                "last_activity": time.time(),
                "created_at": time.time(),
            },
        )
        meta["active_clients"] += 1
        meta["last_activity"] = time.time()
        active_clients = meta["active_clients"]
    logger.info("stream client acquired hash=%s reason=%s active_clients=%s", torrent_hash, reason, active_clients)


def _release_session_client(torrent_hash: str, reason: str) -> None:
    with _lock:
        meta = _session_activity.setdefault(
            torrent_hash,
            {
                "active_clients": 0,
                "last_activity": time.time(),
                "created_at": time.time(),
            },
        )
        meta["active_clients"] = max(0, int(meta.get("active_clients", 0)) - 1)
        meta["last_activity"] = time.time()
        active_clients = meta["active_clients"]
    logger.info("stream client released hash=%s reason=%s active_clients=%s", torrent_hash, reason, active_clients)


def _pop_session_state(torrent_hash: str) -> tuple[Optional[object], Optional[dict], Optional[dict]]:
    with _lock:
        handle = _sessions.pop(torrent_hash, None)
        meta = _session_activity.pop(torrent_hash, None)
    with _hls_lock:
        hls_session = _hls_sessions.pop(torrent_hash, None)
    return handle, meta, hls_session


def _remove_torrent_session(torrent_hash: str, *, delete_files: bool, reason: str) -> bool:
    ses = getattr(_get_lt, "_ses", None)
    lt = getattr(_get_lt, "_lt", None)
    handle, _meta, hls_session = _pop_session_state(torrent_hash)
    if not handle:
        if hls_session:
            _stop_hls_session(hls_session)
        return False

    logger.info("stream removing hash=%s delete_files=%s reason=%s", torrent_hash, delete_files, reason)

    if ses is not None and lt is not None:
        try:
            flag = getattr(lt, "remove_flags_t", None)
            if flag and delete_files:
                ses.remove_torrent(handle, flag.delete_files)
            else:
                ses.remove_torrent(handle, 1 if delete_files else 0)
        except Exception:
            try:
                ses.remove_torrent(handle)
            except Exception:
                logger.exception("stream remove_torrent failed hash=%s reason=%s", torrent_hash, reason)

    _stop_hls_session(hls_session)
    _collect_garbage(f"removed torrent session {torrent_hash}: {reason}")
    return True


def _touch_hls_session(torrent_hash: str) -> None:
    with _hls_lock:
        session = _hls_sessions.get(torrent_hash)
        if session:
            session["last_access"] = time.time()


def _reap_inactive_hls_sessions() -> None:
    now = time.time()
    stale_hashes: list[str] = []

    with _hls_lock:
        for torrent_hash, session in list(_hls_sessions.items()):
            with _lock:
                meta = _session_activity.get(torrent_hash) or {}
                active_clients = int(meta.get("active_clients", 0))
            if active_clients > 0:
                continue
            last_access = float(session.get("last_access", now))
            if now - last_access >= HLS_SESSION_IDLE_TIMEOUT_SEC:
                stale_hashes.append(torrent_hash)

    for torrent_hash in stale_hashes:
        with _hls_lock:
            session = _hls_sessions.pop(torrent_hash, None)
        if session:
            logger.info("stream stopping stale hls session hash=%s idle_timeout=%ss", torrent_hash, HLS_SESSION_IDLE_TIMEOUT_SEC)
            _stop_hls_session(session)
            _collect_garbage(f"stopped stale hls session {torrent_hash}")


def _reap_inactive_sessions() -> None:
    now = time.time()
    stale_hashes: list[tuple[str, int]] = []

    with _lock:
        for torrent_hash, handle in list(_sessions.items()):
            meta = _session_activity.setdefault(
                torrent_hash,
                {
                    "active_clients": 0,
                    "last_activity": now,
                    "created_at": now,
                },
            )
            if int(meta.get("active_clients", 0)) > 0:
                continue
            idle_for = now - float(meta.get("last_activity", now))
            timeout_sec = _session_timeout_for_handle(handle)
            if idle_for >= timeout_sec:
                stale_hashes.append((torrent_hash, int(idle_for)))

    for torrent_hash, idle_for in stale_hashes:
        _remove_torrent_session(
            torrent_hash,
            delete_files=False,
            reason=f"idle timeout after {idle_for}s without active clients",
        )


def _session_reaper_loop() -> None:
    while True:
        try:
            _reap_inactive_sessions()
            _reap_inactive_hls_sessions()
        except Exception:
            logger.exception("stream reaper failed")
        time.sleep(max(2, STREAM_REAPER_INTERVAL_SEC))


def _start_session_reaper() -> None:
    if hasattr(_start_session_reaper, "_thread"):
        return
    thread = threading.Thread(target=_session_reaper_loop, name="torrent-stream-reaper", daemon=True)
    thread.start()
    _start_session_reaper._thread = thread


def _add_or_get(magnet: str, torrent_hash: str):
    """Add magnet to libtorrent session and return handle (idempotent)."""
    ses, lt = _get_lt()
    if ses is None:
        return None

    with _lock:
        if torrent_hash in _sessions:
            meta = _session_activity.setdefault(
                torrent_hash,
                {
                    "active_clients": 0,
                    "last_activity": time.time(),
                    "created_at": time.time(),
                },
            )
            meta["last_activity"] = time.time()
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
        try:
            handle.set_sequential_download(False)
        except Exception:
            pass
        try:
            # Keep overall torrent priority modest; the active playback window is raised separately.
            handle.set_priority(1)
        except Exception:
            pass
        _sessions[torrent_hash] = handle
        _session_activity[torrent_hash] = {
            "active_clients": 0,
            "last_activity": time.time(),
            "created_at": time.time(),
        }
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


def _file_piece_context(handle, media: dict) -> Optional[dict]:
    file_index = media.get("index")
    file_size = int(media.get("size") or 0)
    if file_index is None or file_size <= 0 or not handle.has_metadata():
        return None

    try:
        ti = handle.get_torrent_info()
        first_map = ti.map_file(file_index, 0, 1)
        if file_size == 1:
            last_map = first_map
        else:
            last_map = ti.map_file(file_index, file_size - 1, 1)
    except Exception:
        return None

    piece_length = int(ti.piece_length())
    first_piece = int(first_map.piece)
    last_piece = int(last_map.piece)
    file_absolute_start = first_piece * piece_length + int(first_map.start)

    return {
        "ti": ti,
        "file_index": file_index,
        "file_size": file_size,
        "piece_length": piece_length,
        "first_piece": first_piece,
        "last_piece": last_piece,
        "file_absolute_start": file_absolute_start,
    }


def _contiguous_bytes_from_offset(handle, media: dict, start_offset: int = 0) -> int:
    """
    Return bytes available contiguously starting at the requested file offset.

    This is stricter than file_progress(): it only counts pieces that are present without gaps
    from the specific byte range the client wants to read.
    """
    context = _file_piece_context(handle, media)
    if not context:
        return 0

    file_size = int(context["file_size"])
    if start_offset < 0 or start_offset >= file_size:
        return 0

    ti = context["ti"]
    piece_length = int(context["piece_length"])
    last_piece = int(context["last_piece"])
    file_absolute_start = int(context["file_absolute_start"])

    try:
        start_map = ti.map_file(int(context["file_index"]), start_offset, 1)
    except Exception:
        return 0

    start_piece = int(start_map.piece)
    contiguous = 0

    for piece in range(start_piece, last_piece + 1):
        try:
            if not handle.have_piece(piece):
                break
            piece_size = int(ti.piece_size(piece))
        except Exception:
            break

        piece_start = piece * piece_length
        piece_file_offset_start = max(0, piece_start - file_absolute_start)
        piece_file_offset_end = min(file_size, piece_start + piece_size - file_absolute_start)
        overlap_start = max(start_offset, piece_file_offset_start)
        overlap_end = piece_file_offset_end
        overlap = max(0, overlap_end - overlap_start)

        if overlap <= 0:
            continue
        contiguous += overlap

    return min(contiguous, file_size - start_offset)


def _prioritize_media_range(
    handle,
    torrent_hash: str,
    media: dict,
    start_offset: int,
    window_bytes: int = STREAM_RANGE_PRIORITY_WINDOW_BYTES,
    prefetch_window_bytes: int = STREAM_RANGE_PREFETCH_WINDOW_BYTES,
) -> None:
    context = _file_piece_context(handle, media)
    if not context:
        return

    file_size = int(context["file_size"])
    if start_offset < 0 or start_offset >= file_size:
        return

    retained_start_offset = max(0, start_offset - max(0, STREAM_RANGE_BACK_BUFFER_BYTES))
    end_offset = min(file_size - 1, retained_start_offset + max(1, window_bytes) - 1)
    ti = context["ti"]
    file_index = int(context["file_index"])
    first_piece = int(context["first_piece"])
    last_piece = int(context["last_piece"])

    try:
        start_piece = int(ti.map_file(file_index, retained_start_offset, 1).piece)
        end_piece = int(ti.map_file(file_index, end_offset, 1).piece)
        prefetch_end_offset = min(file_size - 1, end_offset + max(0, prefetch_window_bytes))
        prefetch_end_piece = int(ti.map_file(file_index, prefetch_end_offset, 1).piece)
    except Exception:
        return

    target_priorities: dict[int, int] = {}
    for piece in range(start_piece, end_piece + 1):
        target_priorities[piece] = 7
    for piece in range(end_piece + 1, prefetch_end_piece + 1):
        target_priorities.setdefault(piece, 6)

    with _lock:
        meta = _session_activity.setdefault(
            torrent_hash,
            {
                "active_clients": 0,
                "last_activity": time.time(),
                "created_at": time.time(),
            },
        )
        previous_priorities = dict(meta.get("piece_priorities") or {})
        previous_media_index = meta.get("priority_media_index")
        initialized = bool(meta.get("piece_window_initialized"))

    if previous_media_index != file_index and previous_priorities:
        for piece in previous_priorities:
            try:
                handle.piece_priority(piece, 0)
            except Exception:
                continue
        previous_priorities = {}
        initialized = False

    if not initialized:
        for piece in range(first_piece, last_piece + 1):
            if piece in target_priorities:
                continue
            try:
                if handle.piece_priority(piece) != 0:
                    handle.piece_priority(piece, 0)
            except Exception:
                continue

    for piece in previous_priorities:
        if piece in target_priorities:
            continue
        try:
            if handle.piece_priority(piece) != 0:
                handle.piece_priority(piece, 0)
        except Exception:
            continue

    for piece, priority in target_priorities.items():
        if previous_priorities.get(piece) == priority:
            continue
        try:
            handle.piece_priority(piece, priority)
        except Exception:
            continue

    with _lock:
        meta = _session_activity.setdefault(
            torrent_hash,
            {
                "active_clients": 0,
                "last_activity": time.time(),
                "created_at": time.time(),
            },
        )
        meta["piece_priorities"] = target_priorities
        meta["priority_media_index"] = file_index
        meta["piece_window_initialized"] = True


def _media_available_bytes(handle, media: dict, start_offset: int = 0) -> int:
    contiguous = _contiguous_bytes_from_offset(handle, media, start_offset)
    if contiguous > 0:
        return contiguous
    return 0


def _prioritize_hls_frontier(handle, torrent_hash: str, media: dict) -> int:
    contiguous_prefix = _media_available_bytes(handle, media, start_offset=0)
    frontier_offset = max(0, contiguous_prefix - max(STREAM_RANGE_BACK_BUFFER_BYTES, STREAM_CHUNK_SIZE))
    _prioritize_media_range(
        handle,
        torrent_hash,
        media,
        frontier_offset,
        window_bytes=max(HLS_READY_MIN_BYTES, STREAM_RANGE_PRIORITY_WINDOW_BYTES),
        prefetch_window_bytes=max(STREAM_RANGE_PREFETCH_WINDOW_BYTES, HLS_SEGMENT_TIME_SEC * 2 * 1024 * 1024),
    )
    return contiguous_prefix


def _wait_file_ready(
    handle,
    media: dict,
    timeout: int = 60,
    min_bytes: int = 1,
    start_offset: int = 0,
    label: str = "",
) -> int:
    deadline = time.time() + timeout
    last_log = 0.0

    while time.time() < deadline:
        size = _media_available_bytes(handle, media, start_offset=start_offset)

        if size >= min_bytes:
            if label:
                logger.info("%s file ready bytes=%s offset=%s", label, size, start_offset)
            return size

        now = time.time()
        if label and now - last_log >= 5:
            remaining = max(0, int(deadline - now))
            logger.info(
                "%s waiting for file bytes (%s/%s from offset=%s, %ss remaining)",
                label,
                size,
                min_bytes,
                start_offset,
                remaining,
            )
            last_log = now

        time.sleep(0.5)

    if label:
        logger.warning("%s file wait timed out after %ss (min_bytes=%s)", label, timeout, min_bytes)
    return 0


def _ffmpeg_binary(name: str) -> Optional[str]:
    return shutil.which(name)


def _hls_tools_available() -> bool:
    return bool(_ffmpeg_binary("ffmpeg") and _ffmpeg_binary("ffprobe"))


def _probe_media_ready(media_path: Path) -> tuple[bool, dict | str]:
    ffprobe_bin = _ffmpeg_binary("ffprobe")
    if not ffprobe_bin:
        return False, "ffprobe not installed"

    try:
        result = subprocess.run(
            [
                ffprobe_bin,
                "-v",
                "error",
                "-probesize",
                str(HLS_PROBE_SIZE_BYTES),
                "-analyzeduration",
                str(HLS_ANALYZE_DURATION_US),
                "-show_streams",
                "-show_entries",
                "format=duration",
                "-of",
                "json",
                str(media_path),
            ],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except Exception as exc:
        return False, str(exc)

    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "ffprobe failed").strip()
        return False, detail[:300]

    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        return False, "ffprobe returned invalid JSON"

    streams = payload.get("streams") or []
    if not streams:
        return False, "ffprobe found no media streams"

    video_streams = [stream for stream in streams if stream.get("codec_type") == "video"]
    audio_streams = [stream for stream in streams if stream.get("codec_type") == "audio"]

    return True, {
        "streams": streams,
        "video_streams": video_streams,
        "audio_streams": audio_streams,
        "has_video": bool(video_streams),
        "has_audio": bool(audio_streams),
    }


def _hls_session_dir(torrent_hash: str) -> Path:
    return HLS_ROOT_DIR / torrent_hash


def _cleanup_hls_dir(session_dir: Path) -> None:
    if not session_dir.exists():
        return
    for child in session_dir.iterdir():
        if child.is_file():
            child.unlink(missing_ok=True)


def _stop_hls_session(session: Optional[dict]) -> None:
    if not session:
        return

    process = session.get("process")
    if process and process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except Exception:
            process.kill()

    log_handle = session.get("log_handle")
    if log_handle:
        try:
            log_handle.close()
        except Exception:
            pass

    session_dir = session.get("session_dir")
    if session_dir:
        try:
            shutil.rmtree(session_dir, ignore_errors=True)
        except Exception:
            logger.exception("failed to remove hls session dir %s", session_dir)


def _ensure_hls_session(torrent_hash: str, media: dict, available_bytes: int) -> tuple[Optional[dict], Optional[str]]:
    ffmpeg_bin = _ffmpeg_binary("ffmpeg")
    if not ffmpeg_bin:
        return None, "ffmpeg not installed"

    media_path = Path(media["full_path"])
    probe_ok, probe_result = _probe_media_ready(media_path)
    if not probe_ok:
        return None, str(probe_result or "media probe not ready")

    probe_info = probe_result if isinstance(probe_result, dict) else {}
    if not probe_info.get("has_video"):
        return None, "video stream not discoverable yet"

    if media.get("kind") == "video" and not probe_info.get("has_audio") and available_bytes < HLS_AUDIO_PROBE_MIN_BYTES:
        return None, "audio track not discoverable yet"

    with _hls_lock:
        session = _hls_sessions.get(torrent_hash)
        if session:
            same_media = session.get("media_path") == str(media_path)
            process = session.get("process")
            if same_media and process and process.poll() is None and Path(session["playlist_path"]).exists():
                session["last_access"] = time.time()
                return session, None
            _stop_hls_session(session)

        session_dir = _hls_session_dir(torrent_hash)
        session_dir.mkdir(parents=True, exist_ok=True)
        _cleanup_hls_dir(session_dir)

        playlist_path = session_dir / "stream.m3u8"
        log_path = session_dir / "ffmpeg.log"
        log_handle = open(log_path, "w", encoding="utf-8")

        command = [
            ffmpeg_bin,
            "-hide_banner",
            "-loglevel",
            "warning",
            "-nostdin",
            "-probesize",
            str(HLS_PROBE_SIZE_BYTES),
            "-analyzeduration",
            str(HLS_ANALYZE_DURATION_US),
            "-i",
            str(media_path),
            "-map",
            "0:v:0?",
            "-map",
            "0:a:0?",
            "-sn",
            "-dn",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-pix_fmt",
            "yuv420p",
            "-profile:v",
            "main",
            "-level:v",
            "4.0",
            "-movflags",
            "+faststart",
            "-g",
            "48",
            "-keyint_min",
            "48",
            "-sc_threshold",
            "0",
            "-c:a",
            "aac",
            "-ac",
            "2",
            "-ar",
            "48000",
            "-b:a",
            "128k",
            "-max_muxing_queue_size",
            "2048",
            "-f",
            "hls",
            "-hls_time",
            str(HLS_SEGMENT_TIME_SEC),
            "-hls_list_size",
            str(HLS_LIST_SIZE),
            "-hls_segment_type",
            "fmp4",
            "-hls_fmp4_init_filename",
            "init.mp4",
            "-hls_flags",
            "append_list+delete_segments+independent_segments+temp_file",
            "-hls_segment_filename",
            str(session_dir / "segment_%05d.m4s"),
            str(playlist_path),
        ]

        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=log_handle,
            text=True,
        )

        session = {
            "torrent_hash": torrent_hash,
            "media_path": str(media_path),
            "playlist_path": str(playlist_path),
            "session_dir": str(session_dir),
            "log_path": str(log_path),
            "log_handle": log_handle,
            "process": process,
            "content_type": "application/vnd.apple.mpegurl",
            "last_access": time.time(),
        }
        _hls_sessions[torrent_hash] = session

    deadline = time.time() + HLS_READY_TIMEOUT_SEC
    while time.time() < deadline:
        process = session["process"]
        playlist = Path(session["playlist_path"])
        if playlist.exists():
            try:
                content = playlist.read_text(encoding="utf-8")
            except Exception:
                content = ""
            if "#EXTINF" in content:
                return session, None

        if process.poll() is not None:
            break

        time.sleep(0.5)

    error_detail = ""
    try:
        error_detail = Path(session["log_path"]).read_text(encoding="utf-8")[-500:]
    except Exception:
        error_detail = ""

    with _hls_lock:
        current = _hls_sessions.pop(torrent_hash, None)
        _stop_hls_session(current)

    detail = error_detail.strip() or "ffmpeg exited before the HLS playlist became ready"
    return None, detail


def _playlist_with_token(playlist_path: Path, torrent_hash: str, token: str) -> str:
    content = playlist_path.read_text(encoding="utf-8")
    rewritten: list[str] = []
    for line in content.splitlines():
        if not line or line.startswith("#"):
            rewritten.append(line)
            continue
        rewritten.append(f"/stream/hls/{torrent_hash}/{line}?token={quote(token, safe='')}")
    return "\n".join(rewritten) + "\n"


def _best_video(handle) -> Optional[Path]:
    """Return path to the largest playable media file inside the torrent."""
    info = _best_video_info(handle)
    if not info:
        return None
    return Path(info["full_path"])


def _prefer_direct_browser_playback(media: Optional[dict]) -> bool:
    if not media:
        return False
    ext = str(media.get("ext") or "").lower()
    kind = media.get("kind")
    if kind == "video":
        return ext in BROWSER_DIRECT_VIDEO_EXTS
    if kind == "audio":
        return ext in BROWSER_DIRECT_AUDIO_EXTS
    return False


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
    """Keep only the selected media/subtitles eligible for download; piece windows drive urgency."""
    if not handle.has_metadata():
        return

    try:
        ti = handle.get_torrent_info()
        file_count = ti.files().num_files()
    except Exception:
        return

    priorities = [0] * file_count
    if video_index is not None and 0 <= video_index < file_count:
        priorities[video_index] = 1
    for idx in subtitle_indexes:
        if 0 <= idx < file_count and priorities[idx] < 1:
            priorities[idx] = 1

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
    _touch_session_activity(body.hash)
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
    _touch_session_activity(torrent_hash)
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
    _touch_session_activity(torrent_hash)
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

    lease_acquired = False
    response_started = False
    _acquire_session_client(torrent_hash, "http-stream")
    lease_acquired = True

    try:
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

        total_size   = int(media["size"])
        content_type = mimetypes.guess_type(str(media_path))[0] or ("video/mp4" if media["kind"] == "video" else "audio/mpeg")
        chunk_size   = max(16 * 1024, STREAM_CHUNK_SIZE)

        # ── Range parsing ──────────────────────────────────────────────────────
        start, requested_end = 0, total_size - 1
        is_range = False

        if range_header:
            try:
                range_val = range_header.replace("bytes=", "")
                s, e = range_val.split("-")
                start = int(s)
                requested_end = int(e) if e.strip() else total_size - 1
                is_range = True
            except Exception:
                logger.warning("http stream malformed range ignored hash=%s range=%s", torrent_hash, range_header)
                pass

        start = max(0, min(start, total_size - 1))
        end = max(start, min(requested_end, total_size - 1))
        length = end - start + 1
        _prioritize_media_range(handle, torrent_hash, media, start)

        ready_min_bytes = STREAM_RANGE_READY_MIN_BYTES if is_range else STREAM_READY_MIN_BYTES
        ready_bytes = _wait_file_ready(
            handle,
            media,
            timeout=FILE_WAIT_TIMEOUT_SEC,
            min_bytes=min(ready_min_bytes, length),
            start_offset=start,
            label=f"http stream hash={torrent_hash}",
        )
        if ready_bytes <= 0:
            logger.warning(
                "http stream range not ready hash=%s file=%s start=%s length=%s",
                torrent_hash,
                media_path.name,
                start,
                length,
            )
            raise HTTPException(504, f"Requested media range not yet ready: {media_path.name}")

        logger.info(
            "http stream chunk size hash=%s chunk_size=%s start=%s ready_bytes=%s total_size=%s",
            torrent_hash,
            chunk_size,
            start,
            ready_bytes,
            total_size,
        )

        async def iterfile():
            try:
                with open(media_path, "rb") as f:
                    current_offset = start
                    remaining = length
                    last_progress_at = time.time()
                    while remaining > 0:
                        if await request.is_disconnected():
                            logger.info("http stream client disconnected hash=%s file=%s", torrent_hash, media_path.name)
                            break

                        available_bytes = _media_available_bytes(handle, media, start_offset=current_offset)
                        if available_bytes <= 0:
                            _prioritize_media_range(handle, torrent_hash, media, current_offset)
                            if time.time() - last_progress_at >= STREAM_PROGRESS_WAIT_TIMEOUT_SEC:
                                logger.warning(
                                    "http stream stalled hash=%s file=%s offset=%s available=%s timeout=%s",
                                    torrent_hash,
                                    media_path.name,
                                    current_offset,
                                    available_bytes,
                                    STREAM_PROGRESS_WAIT_TIMEOUT_SEC,
                                )
                                break
                            await asyncio.sleep(max(0.05, STREAM_PROGRESS_POLL_INTERVAL_MS / 1000))
                            continue

                        readable = min(chunk_size, remaining, available_bytes)
                        f.seek(current_offset)
                        data = f.read(readable)
                        if not data:
                            if time.time() - last_progress_at >= STREAM_PROGRESS_WAIT_TIMEOUT_SEC:
                                logger.warning(
                                    "http stream empty read stall hash=%s file=%s offset=%s available=%s",
                                    torrent_hash,
                                    media_path.name,
                                    current_offset,
                                    available_bytes,
                                )
                                break
                            await asyncio.sleep(max(0.05, STREAM_PROGRESS_POLL_INTERVAL_MS / 1000))
                            continue

                        last_progress_at = time.time()
                        _touch_session_activity(torrent_hash)
                        yield data
                        current_offset += len(data)
                        remaining -= len(data)
            finally:
                _release_session_client(torrent_hash, "http-stream")

        headers = {
            "Accept-Ranges":       "bytes",
            "Content-Length":      str(length),
            "Content-Disposition": f'inline; filename="{media_path.name}"',
            "Cache-Control":       "no-cache",
            "X-Initial-Available-Bytes": str(ready_bytes),
            "X-Total-Bytes":       str(total_size),
        }

        if is_range:
            headers["Content-Range"] = f"bytes {start}-{end}/{total_size}"

        response_started = True
        status_code = 206 if is_range else 200
        return StreamingResponse(
            iterfile(),
            status_code=status_code,
            headers=headers,
            media_type=content_type,
        )
    finally:
        if lease_acquired and not response_started:
            _release_session_client(torrent_hash, "http-stream")


@router.get("/hls/{torrent_hash}/stream.m3u8")
async def stream_hls_playlist(
    torrent_hash: str,
    token: Optional[str] = None,
    magnet: Optional[str] = None,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
):
    await _authorize_stream_request(token=token, authorization=authorization)

    ses, _ = _get_lt()
    if ses is None:
        raise HTTPException(503, "libtorrent not installed on the server")
    if not _hls_tools_available():
        raise HTTPException(503, "ffmpeg/ffprobe are required for HLS playback")

    if magnet:
        with _lock:
            already = torrent_hash in _sessions
        if not already:
            _add_or_get(magnet, torrent_hash)

    with _lock:
        handle = _sessions.get(torrent_hash)
    if not handle:
        raise HTTPException(404, "Torrent not started. POST /stream/start first.")

    _acquire_session_client(torrent_hash, "hls-playlist")
    try:
        if not _wait_metadata(handle, timeout=METADATA_TIMEOUT_SEC, label=f"hls stream hash={torrent_hash}"):
            raise HTTPException(504, "Timed out waiting for torrent metadata")

        media = _best_video_info(handle)
        if not media:
            raise HTTPException(404, "No media file found in torrent")
        if media["kind"] != "video":
            raise HTTPException(400, "HLS playback is only available for video files")

        subtitle_tracks = _subtitle_tracks(handle)
        _prioritize_files(handle, media.get("index"), [track["index"] for track in subtitle_tracks])
        _prioritize_hls_frontier(handle, torrent_hash, media)

        ready_bytes = _wait_file_ready(
            handle,
            media,
            timeout=HLS_READY_TIMEOUT_SEC,
            min_bytes=HLS_READY_MIN_BYTES,
            label=f"hls buffer hash={torrent_hash}",
        )
        if ready_bytes <= 0:
            raise HTTPException(504, "Video bytes are not ready for HLS transcoding yet")

        session, error = _ensure_hls_session(torrent_hash, media, ready_bytes)
        if not session:
            raise HTTPException(503, f"HLS preparation still warming up: {error}")

        playlist = Path(session["playlist_path"])
        if not playlist.exists():
            raise HTTPException(503, "HLS playlist is not ready yet")

        _touch_session_activity(torrent_hash)
        _touch_hls_session(torrent_hash)
        return PlainTextResponse(
            _playlist_with_token(playlist, torrent_hash, token or ""),
            media_type="application/vnd.apple.mpegurl",
            headers={"Cache-Control": "no-cache"},
        )
    finally:
        _release_session_client(torrent_hash, "hls-playlist")


@router.get("/hls/{torrent_hash}/{asset_name}")
async def stream_hls_asset(
    torrent_hash: str,
    asset_name: str,
    token: Optional[str] = None,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
):
    await _authorize_stream_request(token=token, authorization=authorization)

    if "/" in asset_name or ".." in asset_name:
        raise HTTPException(400, "Invalid HLS asset path")

    session_dir = _hls_session_dir(torrent_hash)
    asset_path = session_dir / asset_name
    if not asset_path.exists() or not asset_path.is_file():
        raise HTTPException(404, "HLS asset not found")

    media_type = mimetypes.guess_type(str(asset_path))[0]
    if not media_type:
        if asset_path.suffix.lower() in {".m4s", ".mp4"}:
            media_type = "video/mp4"
        elif asset_path.suffix.lower() == ".ts":
            media_type = "video/mp2t"
        else:
            media_type = "application/octet-stream"
    _acquire_session_client(torrent_hash, "hls-asset")
    _touch_hls_session(torrent_hash)

    with _lock:
        handle = _sessions.get(torrent_hash)
    if handle and handle.has_metadata():
        media = _best_video_info(handle)
        if media and media["kind"] == "video":
            _prioritize_hls_frontier(handle, torrent_hash, media)

    async def iter_asset():
        try:
            with open(asset_path, "rb") as f:
                while True:
                    data = f.read(64 * 1024)
                    if not data:
                        break
                    _touch_session_activity(torrent_hash)
                    yield data
        finally:
            _release_session_client(torrent_hash, "hls-asset")

    return StreamingResponse(
        iter_asset(),
        media_type=media_type,
        headers={"Cache-Control": "no-cache"},
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
    _touch_session_activity(torrent_hash)

    # Heavy preparation on server: wait metadata, choose best media, scan subtitles.
    prepared = _wait_metadata(handle, timeout=PREPARE_TIMEOUT_SEC, label=f"stream magnet hash={torrent_hash}")
    media = _best_video_info(handle) if prepared else None
    subtitle_tracks = _subtitle_tracks(handle) if prepared else []
    if media:
        _prioritize_files(handle, media.get("index"), [track["index"] for track in subtitle_tracks])
        if media["kind"] == "video" and _hls_tools_available():
            _prioritize_hls_frontier(handle, torrent_hash, media)
        else:
            _prioritize_media_range(handle, torrent_hash, media, 0)
        _touch_session_activity(torrent_hash)

    base_url = str(request.base_url).rstrip("/")
    stream_path = f"/stream/{torrent_hash}"
    hls_path = f"/stream/hls/{torrent_hash}/stream.m3u8"
    content_type = media["content_type"] if media else None
    if media:
        logger.info(
            "stream magnet ready hash=%s media=%s kind=%s content_type=%s subtitles=%s stream_url=%s hls_url=%s",
            torrent_hash,
            media["name"],
            media["kind"],
            content_type,
            len(subtitle_tracks),
            f"{base_url}{stream_path}",
            f"{base_url}{hls_path}",
        )
    else:
        logger.warning("stream magnet preparing hash=%s prepared=%s", torrent_hash, prepared)

    return {
        "status": "ready" if prepared and media else "preparing",
        "prepared": bool(prepared and media),
        "hash": torrent_hash,
        "stream_path": stream_path,
        "stream_url": f"{base_url}{stream_path}",
        "hls_path": hls_path,
        "hls_url": f"{base_url}{hls_path}" if media and media["kind"] == "video" and _hls_tools_available() else None,
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
            "Torrent prepared. HLS playback will be used when available."
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
    last_hls_attempt = 0.0
    _acquire_session_client(torrent_hash, "ws")

    try:
        while True:
            state = handle.status()
            has_metadata = handle.has_metadata()
            _touch_session_activity(torrent_hash)

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

                    current_bytes = _media_available_bytes(handle, media)
                    prefer_direct = _prefer_direct_browser_playback(media)
                    should_prepare_hls = media["kind"] == "video" and _hls_tools_available() and not prefer_direct
                    if should_prepare_hls:
                        current_bytes = _prioritize_hls_frontier(handle, torrent_hash, media)
                    else:
                        _prioritize_media_range(handle, torrent_hash, media, 0)
                    ready_target = HLS_READY_MIN_BYTES if should_prepare_hls else STREAM_READY_MIN_BYTES

                    if current_bytes < ready_target:
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
                            "ready_bytes": ready_target,
                            "file_ready": False,
                            "playback_mode": "hls" if should_prepare_hls else "direct",
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
                            ready_target,
                        )
                        await asyncio.sleep(1)
                        continue

                    base_url = _http_base_from_websocket(websocket)
                    stream_url = f"{base_url}/stream/{torrent_hash}?token={quote(token, safe='')}"
                    content_type = media["content_type"]
                    hls_url = (
                        f"{base_url}/stream/hls/{torrent_hash}/stream.m3u8?token={quote(token, safe='')}"
                        if media["kind"] == "video" and _hls_tools_available()
                        else None
                    )
                    if should_prepare_hls:
                        hls_ready = False
                        hls_error = None
                        if time.time() - last_hls_attempt >= 2:
                            last_hls_attempt = time.time()
                            session, hls_error = _ensure_hls_session(torrent_hash, media, current_bytes)
                            if session:
                                hls_ready = True
                        if not hls_ready:
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
                                "ready_bytes": ready_target,
                                "file_ready": True,
                                "playback_mode": "hls",
                                "selected_media": {
                                    "name": media["name"],
                                    "path": media["path"],
                                    "size": media["size"],
                                    "kind": media["kind"],
                                    "content_type": media["content_type"],
                                },
                                "message": f"Buffered enough for HLS. Waiting for transcoder… {hls_error or ''}".strip(),
                            })
                            await asyncio.sleep(1)
                            continue

                    await websocket.send_json({
                        "type": "ready",
                        "hash": torrent_hash,
                        "stream_url": stream_url,
                        "hls_url": hls_url,
                        "playback_mode": "hls" if hls_url else "direct",
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
                        "stream ws ready hash=%s media=%s kind=%s content_type=%s subtitles=%s stream_url=%s hls_url=%s bytes=%s",
                        torrent_hash,
                        media["name"],
                        media["kind"],
                        content_type,
                        len(subtitle_tracks),
                        stream_url,
                        hls_url,
                        current_bytes,
                    )
                    await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
                    return

            if time.time() - started_at > max(METADATA_TIMEOUT_SEC, HLS_READY_TIMEOUT_SEC):
                logger.warning("stream ws metadata timeout hash=%s", torrent_hash)
                await websocket.send_json({
                    "type": "error",
                    "message": "Timed out waiting for torrent metadata or HLS preparation",
                })
                await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
                return

            await asyncio.sleep(1)
    except WebSocketDisconnect:
        return
    finally:
        _release_session_client(torrent_hash, "ws")


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
    ses, _ = _get_lt()
    if ses is None:
        raise HTTPException(503, "libtorrent not installed")
    _remove_torrent_session(
        torrent_hash,
        delete_files=delete_files,
        reason="manual delete endpoint",
    )

    return {"message": "Removed", "hash": torrent_hash, "files_deleted": delete_files}
