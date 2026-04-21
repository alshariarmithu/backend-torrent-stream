import os
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pymongo.errors import PyMongoError

from auth import get_current_user
from models import (
    CommunityComment,
    CommunityCommentCreate,
    CommunityPost,
    CommunityPostCreate,
    CommunityPostVote,
    CommunityVoteUpdate,
    User,
)

router = APIRouter()

COMMUNITY_DEMO_MODE = os.getenv("COMMUNITY_DEMO_MODE", "true").lower() not in {"0", "false", "no"}

DEMO_POSTS: dict[str, dict] = {}
DEMO_COMMENTS: dict[str, list[dict]] = {}
DEMO_VOTES: dict[tuple[str, str], dict] = {}


def _now() -> datetime:
    return datetime.utcnow()


def _new_id() -> str:
    return uuid4().hex


def _as_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _torrent_dict(torrent) -> dict:
    return {
        "name": torrent.name,
        "size": torrent.size or "",
        "seeders": torrent.seeders or "0",
        "leechers": torrent.leechers or "0",
        "magnet": torrent.magnet or "",
        "hash": torrent.hash or "",
        "poster": torrent.poster or "",
        "category": torrent.category or "",
        "site": torrent.site or "",
        "url": torrent.url or "",
    }


def _user_vote_from_demo(post_id: str, user_id: str) -> int:
    vote = DEMO_VOTES.get((post_id, user_id))
    return int(vote["value"]) if vote else 0


def _serialize_post(post: CommunityPost | dict, user_id: str, user_vote: int | None = None) -> dict:
    if isinstance(post, dict):
        resolved_vote = _user_vote_from_demo(post["id"], user_id) if user_vote is None else int(user_vote)
        return {
            "id": post["id"],
            "author_email": post["author_email"],
            "caption": post["caption"],
            "torrent": dict(post["torrent"]),
            "score": int(post["score"]),
            "upvote_count": int(post["upvote_count"]),
            "downvote_count": int(post["downvote_count"]),
            "comment_count": int(post["comment_count"]),
            "user_vote": resolved_vote,
            "created_at": _as_iso(post["created_at"]),
        }

    resolved_vote = int(user_vote or 0)
    return {
        "id": str(post.id),
        "author_email": str(post.author_email),
        "caption": post.caption or "",
        "torrent": _torrent_dict(post.torrent),
        "score": int(post.score),
        "upvote_count": int(post.upvote_count),
        "downvote_count": int(post.downvote_count),
        "comment_count": int(post.comment_count),
        "user_vote": resolved_vote,
        "created_at": _as_iso(post.created_at),
    }


def _serialize_comment(comment: CommunityComment | dict) -> dict:
    if isinstance(comment, dict):
        return {
            "id": comment["id"],
            "author_email": comment["author_email"],
            "content": comment["content"],
            "created_at": _as_iso(comment["created_at"]),
        }

    return {
        "id": str(comment.id),
        "author_email": str(comment.author_email),
        "content": comment.content,
        "created_at": _as_iso(comment.created_at),
    }


def _vote_state(post: CommunityPost | dict, user_id: str, user_vote: int | None = None) -> dict:
    serialized = _serialize_post(post, user_id, user_vote=user_vote)
    return {
        "score": serialized["score"],
        "upvote_count": serialized["upvote_count"],
        "downvote_count": serialized["downvote_count"],
        "comment_count": serialized["comment_count"],
        "user_vote": serialized["user_vote"],
    }


def _sort_posts(posts: list[CommunityPost | dict]) -> list[CommunityPost | dict]:
    return sorted(
        posts,
        key=lambda post: (
            int(post["score"]) if isinstance(post, dict) else int(post.score),
            post["created_at"] if isinstance(post, dict) else post.created_at,
        ),
        reverse=True,
    )


def _demo_post_from_body(body: CommunityPostCreate, user: User) -> dict:
    post_id = _new_id()
    post = {
        "id": post_id,
        "user_id": str(user.id),
        "author_email": str(user.email),
        "caption": (body.caption or "").strip(),
        "torrent": _torrent_dict(body.torrent),
        "score": 0,
        "upvote_count": 0,
        "downvote_count": 0,
        "comment_count": 0,
        "created_at": _now(),
    }
    DEMO_POSTS[post_id] = post
    DEMO_COMMENTS.setdefault(post_id, [])
    return post


def _demo_get_post(post_id: str) -> dict | None:
    return DEMO_POSTS.get(post_id)


def _demo_comments_for_post(post_id: str) -> list[dict]:
    return sorted(DEMO_COMMENTS.get(post_id, []), key=lambda comment: comment["created_at"])


def _validate_post_payload(body: CommunityPostCreate) -> None:
    if not body.torrent.name.strip():
        raise HTTPException(status_code=400, detail="torrent.name is required")


def _validate_comment_payload(body: CommunityCommentCreate) -> str:
    content = body.content.strip()
    if not content:
        raise HTTPException(status_code=400, detail="Comment content cannot be empty")
    return content


def _validate_vote_value(body: CommunityVoteUpdate) -> int:
    if body.value not in {-1, 0, 1}:
        raise HTTPException(status_code=400, detail="Vote value must be -1, 0, or 1")
    return body.value


def _apply_vote_counts(post: dict, previous: int, new_value: int) -> None:
    if previous == 1:
        post["upvote_count"] = max(0, int(post["upvote_count"]) - 1)
        post["score"] = int(post["score"]) - 1
    elif previous == -1:
        post["downvote_count"] = max(0, int(post["downvote_count"]) - 1)
        post["score"] = int(post["score"]) + 1

    if new_value == 1:
        post["upvote_count"] = int(post["upvote_count"]) + 1
        post["score"] = int(post["score"]) + 1
    elif new_value == -1:
        post["downvote_count"] = int(post["downvote_count"]) + 1
        post["score"] = int(post["score"]) - 1


async def _get_post(post_id: str) -> CommunityPost | dict | None:
    if COMMUNITY_DEMO_MODE:
        return _demo_get_post(post_id)

    try:
        post = await CommunityPost.get(post_id)
        if post:
            return post
    except PyMongoError:
        pass
    return _demo_get_post(post_id)


async def _get_db_user_vote(post_id: str, user_id: str) -> int:
    try:
        vote = await CommunityPostVote.find_one(
            CommunityPostVote.post_id == post_id,
            CommunityPostVote.user_id == user_id,
        )
        return int(vote.value) if vote else 0
    except PyMongoError:
        return _user_vote_from_demo(post_id, user_id)


@router.get("/posts")
async def get_posts(user: User = Depends(get_current_user)):
    user_id = str(user.id)

    if COMMUNITY_DEMO_MODE:
        posts: list[CommunityPost | dict] = list(DEMO_POSTS.values())
    else:
        try:
            posts = await CommunityPost.find_all().to_list()
        except PyMongoError:
            posts = list(DEMO_POSTS.values())
    serialized_posts = []
    for post in _sort_posts(posts):
        post_id = post["id"] if isinstance(post, dict) else str(post.id)
        user_vote = _user_vote_from_demo(post_id, user_id) if isinstance(post, dict) else await _get_db_user_vote(post_id, user_id)
        serialized_posts.append(_serialize_post(post, user_id, user_vote=user_vote))
    return serialized_posts


@router.post("/posts", status_code=201)
async def create_post(body: CommunityPostCreate, user: User = Depends(get_current_user)):
    _validate_post_payload(body)

    if COMMUNITY_DEMO_MODE:
        post = _demo_post_from_body(body, user)
        return _serialize_post(post, str(user.id))

    try:
        post = CommunityPost(
            user_id=str(user.id),
            author_email=user.email,
            caption=(body.caption or "").strip(),
            torrent=body.torrent,
        )
        await post.insert()
        return _serialize_post(post, str(user.id))
    except PyMongoError:
        post = _demo_post_from_body(body, user)
        return _serialize_post(post, str(user.id))


@router.get("/posts/{post_id}")
async def get_post(post_id: str, user: User = Depends(get_current_user)):
    post = await _get_post(post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    user_id = str(user.id)
    user_vote = _user_vote_from_demo(post_id, user_id) if isinstance(post, dict) else await _get_db_user_vote(post_id, user_id)
    return _serialize_post(post, user_id, user_vote=user_vote)


@router.get("/posts/{post_id}/comments")
async def get_comments(post_id: str, user: User = Depends(get_current_user)):
    post = await _get_post(post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")

    if COMMUNITY_DEMO_MODE or isinstance(post, dict):
        comments: list[CommunityComment | dict] = _demo_comments_for_post(post_id)
    else:
        try:
            comments = await CommunityComment.find(CommunityComment.post_id == post_id).sort("created_at").to_list()
        except PyMongoError:
            comments = _demo_comments_for_post(post_id)

    return [_serialize_comment(comment) for comment in comments]


@router.post("/posts/{post_id}/comments", status_code=201)
async def add_comment(
    post_id: str,
    body: CommunityCommentCreate,
    user: User = Depends(get_current_user),
):
    content = _validate_comment_payload(body)
    post = await _get_post(post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")

    if COMMUNITY_DEMO_MODE or isinstance(post, dict):
        comment = {
            "id": _new_id(),
            "post_id": post_id,
            "user_id": str(user.id),
            "author_email": str(user.email),
            "content": content,
            "created_at": _now(),
        }
        DEMO_COMMENTS.setdefault(post_id, []).append(comment)
        post["comment_count"] = int(post["comment_count"]) + 1
        return _serialize_comment(comment)

    try:
        comment = CommunityComment(
            post_id=post_id,
            user_id=str(user.id),
            author_email=user.email,
            content=content,
        )
        await comment.insert()
        post.comment_count += 1
        await post.save()
        return _serialize_comment(comment)
    except PyMongoError:
        demo_post = DEMO_POSTS.setdefault(
            post_id,
            {
                "id": str(post.id),
                "user_id": post.user_id,
                "author_email": str(post.author_email),
                "caption": post.caption,
                "torrent": _torrent_dict(post.torrent),
                "score": post.score,
                "upvote_count": post.upvote_count,
                "downvote_count": post.downvote_count,
                "comment_count": post.comment_count,
                "created_at": post.created_at,
            },
        )
        comment = {
            "id": _new_id(),
            "post_id": post_id,
            "user_id": str(user.id),
            "author_email": str(user.email),
            "content": content,
            "created_at": _now(),
        }
        DEMO_COMMENTS.setdefault(post_id, []).append(comment)
        demo_post["comment_count"] = int(demo_post["comment_count"]) + 1
        return _serialize_comment(comment)


@router.put("/posts/{post_id}/vote")
async def vote_post(
    post_id: str,
    body: CommunityVoteUpdate,
    user: User = Depends(get_current_user),
):
    user_id = str(user.id)
    new_value = _validate_vote_value(body)
    post = await _get_post(post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")

    if COMMUNITY_DEMO_MODE or isinstance(post, dict):
        key = (post_id, user_id)
        existing_vote = DEMO_VOTES.get(key)
        previous = int(existing_vote["value"]) if existing_vote else 0
        _apply_vote_counts(post, previous, new_value)

        if new_value == 0:
            DEMO_VOTES.pop(key, None)
        else:
            now = _now()
            DEMO_VOTES[key] = {
                "post_id": post_id,
                "user_id": user_id,
                "value": new_value,
                "created_at": existing_vote["created_at"] if existing_vote else now,
                "updated_at": now,
            }
        return _vote_state(post, user_id, user_vote=new_value)

    try:
        existing_vote = await CommunityPostVote.find_one(
            CommunityPostVote.post_id == post_id,
            CommunityPostVote.user_id == user_id,
        )
        previous = int(existing_vote.value) if existing_vote else 0
        if previous != new_value:
            post.upvote_count = int(post.upvote_count)
            post.downvote_count = int(post.downvote_count)
            post.score = int(post.score)

            if previous == 1:
                post.upvote_count = max(0, post.upvote_count - 1)
                post.score -= 1
            elif previous == -1:
                post.downvote_count = max(0, post.downvote_count - 1)
                post.score += 1

            if new_value == 1:
                post.upvote_count += 1
                post.score += 1
            elif new_value == -1:
                post.downvote_count += 1
                post.score -= 1

            if new_value == 0:
                if existing_vote:
                    await existing_vote.delete()
            elif existing_vote:
                existing_vote.value = new_value
                existing_vote.updated_at = _now()
                await existing_vote.save()
            else:
                await CommunityPostVote(
                    post_id=post_id,
                    user_id=user_id,
                    value=new_value,
                ).insert()

            await post.save()
        final_vote = new_value if previous != new_value else previous
        return _vote_state(post, user_id, user_vote=final_vote)
    except PyMongoError:
        demo_post = DEMO_POSTS.setdefault(
            post_id,
            {
                "id": str(post.id),
                "user_id": post.user_id,
                "author_email": str(post.author_email),
                "caption": post.caption,
                "torrent": _torrent_dict(post.torrent),
                "score": post.score,
                "upvote_count": post.upvote_count,
                "downvote_count": post.downvote_count,
                "comment_count": post.comment_count,
                "created_at": post.created_at,
            },
        )
        key = (post_id, user_id)
        existing_vote = DEMO_VOTES.get(key)
        previous = int(existing_vote["value"]) if existing_vote else 0
        _apply_vote_counts(demo_post, previous, new_value)
        if new_value == 0:
            DEMO_VOTES.pop(key, None)
        else:
            now = _now()
            DEMO_VOTES[key] = {
                "post_id": post_id,
                "user_id": user_id,
                "value": new_value,
                "created_at": existing_vote["created_at"] if existing_vote else now,
                "updated_at": now,
            }
        return _vote_state(demo_post, user_id, user_vote=new_value)
