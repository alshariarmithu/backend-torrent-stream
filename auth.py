import os
from datetime import datetime, timedelta, timezone

from beanie.operators import Eq
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr

from models import User

SECRET_KEY = os.getenv("JWT_SECRET", "change-me-in-production-use-random-256bit")
ALGORITHM = "HS256"
TOKEN_EXPIRE_DAYS = int(os.getenv("TOKEN_EXPIRE_DAYS", "30"))

pwd_ctx = CryptContext(schemes=["argon2"], deprecated="auto")
security = HTTPBearer()
router = APIRouter()


class RegisterRequest(BaseModel):
    email: EmailStr
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user_id: str
    email: EmailStr


class MeResponse(BaseModel):
    id: str
    email: EmailStr
    created_at: datetime


def hash_password(password: str) -> str:
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    return pwd_ctx.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_ctx.verify(plain, hashed)


def create_token(user_id: str, email: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "exp": datetime.now(timezone.utc) + timedelta(days=TOKEN_EXPIRE_DAYS),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid or expired token") from exc


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> User:
    payload = decode_token(credentials.credentials)
    user = await User.get(payload["sub"])
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return user


@router.post("/register", response_model=TokenResponse)
async def register(req: RegisterRequest):
    existing = await User.find_one(Eq(User.email, req.email))
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")

    user = User(email=req.email, hashed_password=hash_password(req.password))
    await user.insert()
    return TokenResponse(
        access_token=create_token(str(user.id), user.email),
        user_id=str(user.id),
        email=user.email,
    )


@router.post("/login", response_model=TokenResponse)
async def login(req: LoginRequest):
    user = await User.find_one(Eq(User.email, req.email))
    if not user or not verify_password(req.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    return TokenResponse(
        access_token=create_token(str(user.id), user.email),
        user_id=str(user.id),
        email=user.email,
    )


@router.get("/me", response_model=MeResponse)
async def me(user: User = Depends(get_current_user)):
    return MeResponse(id=str(user.id), email=user.email, created_at=user.created_at)
