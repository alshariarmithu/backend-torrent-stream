from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from auth import get_current_user
from content_policy import assert_allowed_text, assert_allowed_torrent
from models import (
    CommunityComment,
    CommunityCommentCreate,
    CommunityPost,
    CommunityPostCreate,
    CommunityPostVote,
    CommunityVoteRequest,
    User,
)

router = APIRouter()

MAX_TAGS = 5
MAX_TAG_LENGTH = 24


def _normalize_tags(tags: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()

    for raw_tag in tags:
        tag = raw_tag.strip().lower()
        if not tag:
            continue
        if len(tag) > MAX_TAG_LENGTH:
            raise HTTPException(
                status_code=400,
                detail=f"Each tag must be at most {MAX_TAG_LENGTH} characters",
            )
        if tag not in seen:
            normalized.append(tag)
            seen.add(tag)

    if len(normalized) > MAX_TAGS:
        raise HTTPException(status_code=400, detail=f"At most {MAX_TAGS} tags are allowed")

    return normalized


def _post_response(post: CommunityPost, user_vote: int) -> dict:
    return {
        "id": str(post.id),
        "author_email": post.author_email,
        "caption": post.caption,
        "torrent": post.torrent.model_dump(),
        "tags": post.tags,
        "score": post.score,
        "upvote_count": post.upvote_count,
        "downvote_count": post.downvote_count,
        "comment_count": post.comment_count,
        "user_vote": user_vote,
        "created_at": post.created_at,
    }


def _comment_response(comment: CommunityComment) -> dict:
    return {
        "id": str(comment.id),
        "author_email": comment.author_email,
        "content": comment.content,
        "created_at": comment.created_at,
    }


async def _get_post_or_404(post_id: str) -> CommunityPost:
    post = await CommunityPost.get(post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Post not found")
    return post


async def _user_vote_value(post_id: str, user_id: str) -> int:
    vote = await CommunityPostVote.find_one(
        CommunityPostVote.post_id == post_id,
        CommunityPostVote.user_id == user_id,
    )
    return vote.value if vote else 0


@router.get("/posts")
async def get_posts(
    tag: str | None = Query(default=None),
    user: User = Depends(get_current_user),
):
    user_id = str(user.id)
    normalized_tag = tag.strip().lower() if tag else None
    post_query = (
        CommunityPost.find(CommunityPost.tags == normalized_tag)
        if normalized_tag
        else CommunityPost.find_all()
    )
    posts = await post_query.sort([("score", -1), ("created_at", -1)]).to_list()

    post_ids = {str(post.id) for post in posts}
    votes = await CommunityPostVote.find(CommunityPostVote.user_id == user_id).to_list() if posts else []
    vote_map = {vote.post_id: vote.value for vote in votes if vote.post_id in post_ids}

    return [_post_response(post, vote_map.get(str(post.id), 0)) for post in posts]


@router.post("/posts", status_code=201)
async def create_post(body: CommunityPostCreate, user: User = Depends(get_current_user)):
    if not body.torrent.name.strip():
        raise HTTPException(status_code=400, detail="torrent.name is required")
    assert_allowed_torrent(body.torrent)
    assert_allowed_text(body.caption, "caption")
    for tag in body.tags:
        assert_allowed_text(tag, "tag")

    post = CommunityPost(
        user_id=str(user.id),
        author_email=user.email,
        caption=body.caption.strip() if body.caption else "",
        torrent=body.torrent,
        tags=_normalize_tags(body.tags),
    )
    await post.insert()
    return _post_response(post, 0)


@router.get("/posts/{post_id}")
async def get_post(post_id: str, user: User = Depends(get_current_user)):
    post = await _get_post_or_404(post_id)
    user_vote = await _user_vote_value(post_id, str(user.id))
    return _post_response(post, user_vote)


@router.get("/posts/{post_id}/comments")
async def get_comments(post_id: str, user: User = Depends(get_current_user)):
    _ = user
    await _get_post_or_404(post_id)
    comments = await CommunityComment.find(
        CommunityComment.post_id == post_id
    ).sort([("created_at", 1)]).to_list()
    return [_comment_response(comment) for comment in comments]


@router.post("/posts/{post_id}/comments", status_code=201)
async def add_comment(
    post_id: str,
    body: CommunityCommentCreate,
    user: User = Depends(get_current_user),
):
    content = body.content.strip()
    if not content:
        raise HTTPException(status_code=400, detail="Comment content cannot be empty")
    assert_allowed_text(content, "comment")

    post = await _get_post_or_404(post_id)
    comment = CommunityComment(
        post_id=post_id,
        user_id=str(user.id),
        author_email=user.email,
        content=content,
    )
    await comment.insert()

    post.comment_count += 1
    await post.save()
    return _comment_response(comment)


@router.put("/posts/{post_id}/vote")
async def vote_post(
    post_id: str,
    body: CommunityVoteRequest,
    user: User = Depends(get_current_user),
):
    post = await _get_post_or_404(post_id)
    user_id = str(user.id)
    existing_vote = await CommunityPostVote.find_one(
        CommunityPostVote.post_id == post_id,
        CommunityPostVote.user_id == user_id,
    )

    previous_value = existing_vote.value if existing_vote else 0
    new_value = body.value

    if new_value == 0:
        if existing_vote:
            await existing_vote.delete()
    elif existing_vote:
        if existing_vote.value != new_value:
            existing_vote.value = new_value
            existing_vote.updated_at = datetime.utcnow()
            await existing_vote.save()
    else:
        vote = CommunityPostVote(
            post_id=post_id,
            user_id=user_id,
            value=new_value,
        )
        await vote.insert()

    delta = new_value - previous_value
    if previous_value == 1:
        post.upvote_count = max(0, post.upvote_count - 1)
    elif previous_value == -1:
        post.downvote_count = max(0, post.downvote_count - 1)

    if new_value == 1:
        post.upvote_count += 1
    elif new_value == -1:
        post.downvote_count += 1

    post.score += delta
    await post.save()

    return {
        "score": post.score,
        "upvote_count": post.upvote_count,
        "downvote_count": post.downvote_count,
        "comment_count": post.comment_count,
        "user_vote": new_value,
    }
