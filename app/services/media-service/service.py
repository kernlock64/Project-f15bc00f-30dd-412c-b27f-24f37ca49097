from fastapi import FastAPI, Depends, Request, Form, UploadFile, File
import httpx
from sqlalchemy import *
import uuid

from httpx import Limits, AsyncClient, HTTPError

from db_init import get_async_session, engine
from model import MediaDB
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

@app.get("/media/{filename}")
def get_image(filename: str,
              db: AsyncSession = Depends(get_db),
              client: AsyncClient = Depends(get_httpx)):
    pass

@app.post("/media/add-image/{filename}")
async def add_image(filename: str,
                    file: UploadFile = File(...),
                    db: AsyncSession = Depends(get_db),
                    client: AsyncClient = Depends(get_httpx)):
    pass

@app.delete("/delete-image/{filename}")
def delete_image(filename: str,
                 db: AsyncSession = Depends(get_db),
                 client: AsyncClient = Depends(get_httpx)):
    pass
