from fastapi import FastAPI, Depends, Request, Form
import httpx
from sqlalchemy import *
import uuid

from httpx import Limits, AsyncClient, HTTPError

from db_init import get_async_session, engine
from model import MailDB
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

@app.post("/mail/on-password-change")
async def password_change(
        r_token: str = Form(...),
):
    return {
        "status": "True"
    }

@app.post("/mail/on-password-forget")
async def password_forget(
        r_token: str = Form(...),
):
    return {
        "status": "True"
    }

@app.post("/mail/on-suspicious-activity")
async def suspicious_activity():
    pass

@app.post("/mail/on-requesting-provider")
async def requesting_provider():
    pass

@app.post("/mail/on-acceptance-provider")
async def registration_provider():
    pass

@app.post("/mail/on-deletion")
async def registration_provider():
    pass