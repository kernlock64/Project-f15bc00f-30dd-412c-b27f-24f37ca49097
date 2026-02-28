import uuid

from fastapi import FastAPI, File, UploadFile, Depends, Request, Response, Form, status, BackgroundTasks
from fastapi.responses import JSONResponse
from starlette.responses import StreamingResponse
from starlette.datastructures import MutableHeaders
import httpx
import hashlib
import secrets
import jwt
from datetime import datetime, timedelta, timezone
from sqlalchemy import *
import os

from httpx import Limits, AsyncClient, HTTPError

from db_init import get_async_session, engine
from model import AuthDB, TokenDB
from email_validator import EmailNotValidError, validate_email
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession

limits = Limits(
    max_connections=1000,
    max_keepalive_connections=100,
)


@asynccontextmanager
async def lifespan(cl: FastAPI):
    client = httpx.AsyncClient(limits=limits, timeout=5)
    cl.state.http_client = client
    factory = get_async_session()
    app.state.db = factory
    try:
        yield
    finally:
        await client.aclose()
        await engine.dispose()


async def get_httpx(request: Request) -> httpx.AsyncClient:
    return request.app.state.http_client


app = FastAPI(lifespan=lifespan)


async def get_db():
    factory = app.state.db
    async with factory() as session:
        yield session

@app.get("/users/get-user")
async def get_user():
    pass

@app.post("/users/add-user")
async def add_user():
    pass

@app.post("/users/delete-user")
async def delete_user():
    pass

@app.post("/users/change-userdata")
async def change_userdata():
    pass

@app.post("/users/change-avatar")
async def change_avatar():
    pass
