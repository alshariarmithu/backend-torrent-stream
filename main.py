"""
TorrentStream Server
FastAPI backend with JWT auth, user lists, and torrent streaming
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from database import init_db
from auth import router as auth_router
from routes.lists import router as lists_router
from routes.search import router as search_router
from routes.stream import router as stream_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="TorrentStream API",
    description="Backend for TorrentStream iOS App",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router,    prefix="/auth",   tags=["Auth"])
app.include_router(lists_router,   prefix="/lists",  tags=["Lists"])
app.include_router(search_router,  prefix="/search", tags=["Search"])
app.include_router(stream_router,  prefix="/stream", tags=["Stream"])


@app.get("/health")
async def health():
    return {"status": "ok", "service": "TorrentStream"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8081, reload=True)