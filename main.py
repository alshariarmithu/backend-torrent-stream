from dotenv import load_dotenv
import os
from importlib import import_module
load_dotenv()
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from auth import router as auth_router
from db import lifespan_context
from routes.lists import router as lists_router
from routes.search import router as search_router
from routes.stream import router as stream_router

try:
    community_router = import_module("routes.community").router
except Exception:
    community_router = None

app = FastAPI(
    title="TorrentStream API",
    description="Backend for TorrentStream iOS App (MongoDB + Beanie)",
    version="2.0.0",
    lifespan=lifespan_context,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router, prefix="/auth", tags=["Auth"])
app.include_router(lists_router, prefix="/lists", tags=["Lists"])
if community_router is not None:
    app.include_router(community_router, prefix="/community", tags=["Community"])
app.include_router(search_router, prefix="/search", tags=["Search"])
app.include_router(stream_router, prefix="/stream", tags=["Stream"])

# serve a basic static UI
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def ui():
    try:
        with open("static/index.html", "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>UI not found</h1>", status_code=404)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "TorrentStream", "database": "mongodb"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8081)), reload=True)
