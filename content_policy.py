from pathlib import Path

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


def _load_source_blocked_keywords() -> list[str]:
    # Source: https://github.com/rrgeorge-pdcontributions/NSFW-Words-List/blob/master/nsfw_list.txt
    keywords_path = Path(__file__).with_name("nsfw_list.txt")
    try:
        return [
            line.strip().lower()
            for line in keywords_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except OSError:
        return []


SOURCE_BLOCKED_KEYWORDS = _load_source_blocked_keywords()

ALL_BLOCKED_KEYWORDS = sorted(
    set(DEFAULT_BADWORD_KEYWORDS + DEFAULT_BLOCKED_SITE_KEYWORDS + SOURCE_BLOCKED_KEYWORDS)
)


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
