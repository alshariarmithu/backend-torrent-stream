from fastapi import HTTPException

from models import TorrentPayload

DEFAULT_BADWORD_KEYWORDS = [
    "porn",
    "xxx",
    "nsfw",
    "nude",
    "nudity",
    "sexcam",
    "camgirl",
    "escort",
    "brazzers",
    "hentai",
    "onlyfans",
    "fansly",
]

DEFAULT_BLOCKED_SITE_KEYWORDS = [
    "pornhub",
    "xvideos",
    "xnxx",
    "xhamster",
    "redtube",
    "youporn",
    "spankbang",
    "brazzers",
    "onlyfans",
    "fansly",
    "manyvids",
]

ALL_BLOCKED_KEYWORDS = sorted(set(DEFAULT_BADWORD_KEYWORDS + DEFAULT_BLOCKED_SITE_KEYWORDS))


def _normalize(value: str | None) -> str:
    return (value or "").strip().lower()


def blocked_reason(*values: str | None) -> str | None:
    for value in values:
        normalized = _normalize(value)
        if not normalized:
            continue
        for keyword in ALL_BLOCKED_KEYWORDS:
            if keyword in normalized:
                return keyword
    return None


def filter_torrent_items(items: list[dict]) -> list[dict]:
    return [
        item
        for item in items
        if blocked_reason(item.get("name"), item.get("site"), item.get("url")) is None
    ]


def assert_allowed_text(value: str | None, field_name: str) -> None:
    if blocked_reason(value) is not None:
        raise HTTPException(status_code=400, detail=f"{field_name} contains blocked content")


def assert_allowed_torrent(payload: TorrentPayload) -> None:
    if blocked_reason(payload.name, payload.site, payload.url) is not None:
        raise HTTPException(
            status_code=400,
            detail="Torrent content is blocked by content policy",
        )
