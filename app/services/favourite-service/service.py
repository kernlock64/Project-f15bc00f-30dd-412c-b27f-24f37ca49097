from fastapi import FastAPI, Depends, Request, Response, Form, status
import httpx
from sqlalchemy import *

from httpx import Limits, AsyncClient, HTTPError

from db_init import get_async_session, engine
from model import FavouriteDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession

limits = Limits(
    max_connections=700,
    max_keepalive_connections=50,
)


@asynccontextmanager
async def lifespan(cl: FastAPI):
    client = httpx.AsyncClient(limits=limits, timeout=5)
    cl.state.http_client = client
    try:
        yield
    finally:
        await client.aclose()
        await engine.dispose()


async def get_httpx(request: Request) -> httpx.AsyncClient:
    return request.app.state.http_client


app = FastAPI(lifespan=lifespan)

async def get_db():
    factory = get_async_session()
    async with factory() as session:
        yield session



@app.get("/favourites/get-favourites")
async def get_favourites(
        request: Request,
        db: AsyncSession = Depends(get_db),
):
    user_id = request.headers["X-User-Id"]
    

@app.get("/favourites/is-favourite")
async def is_favourite(
        request: Request,
        db: AsyncSession = Depends(get_db),
        post_id: int = Form(...),
):
    pass


@app.post("/favourites/{post_id}")
async def favourite_manager(
        request: Request,
        db: AsyncSession = Depends(get_db),
):
    pass

@app.post("/favourites/deletion/")
async def favourite_deletion(
        request: Request,
        db: AsyncSession = Depends(get_db),
):
    try:
        await db.execute(delete(FavouriteDB).where(FavouriteDB.uid == str(request.headers["X-User-Id"])))
        await db.commit()
        return {
            "status": "True",
        }
    except HTTPError as e:
        return {
            "status": "False",
            "message": str(e),
        }

