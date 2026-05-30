from fastapi import FastAPI, Depends, Request, Form, UploadFile, File, status
from fastapi.responses import JSONResponse
import httpx
from sqlalchemy import *
import uuid
import json
import os
from datetime import *
import jwt
import io

from httpx import Limits, AsyncClient, HTTPError

from db_init import get_async_session, engine
from model import TicketsDB, PriceListsDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession

import asyncio
import aio_pika

limits = Limits(
    max_connections=1000,
    max_keepalive_connections=100,
)

MAX_AD_SIZE = 30 * 1024 * 1024


@asynccontextmanager
async def lifespan(cl: FastAPI):
    client = httpx.AsyncClient(limits=limits, timeout=5)
    cl.state.http_client = client
    try:
        yield
    finally:
        await client.aclose()


async def get_httpx() -> httpx.AsyncClient:
    return app.state.http_client


async def get_db():
    factory = app.state.db
    async with factory() as session:
        yield session


app = FastAPI(lifespan=lifespan)


async def send_request(
        dest: str,
        data: dict,
) -> httpx.Response:

    client = await get_httpx()

    dest_url = {
        "u": "http://auth-service:5000/api/v1/users/manip",
        "p": "http://auth-service:5000/api/v1/posts/manip",
        "a": "http://auth-service:5000/api/v1/manip",
    }
    if dest not in [
        "dp",
        "pp",
        "ap",
        "da",
        "pu",
        "va",
        "gu",
        "au",
    ]:
        return {
            "Invalid destination!"
        }

    payload = {
        "thm": "admin",
        "dest": dest,
        "exp": datetime.now(tz=timezone.utc) + timedelta(seconds=300)
    }
    prv_key = os.getenv("SECRET_KEY").replace("\\n", "\n")
    access_t = jwt.encode(payload, prv_key, algorithm="ES256")

    headers = {
        "admin_t": access_t,
    }

    data = {
        "metadata": data,
    }

    resp = await client.post(url=dest_url[dest[1]], headers=headers, json=data)

    return resp


@app.post("/api/v1/management/web-health/push")
async def push(
        file: UploadFile = File(...),
):
    from file_proceed import initial_proceed
    if not file.filename.endswith('.zip'):
        return JSONResponse(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            content={
                "status": "False",
                "message": "Требуется предоставить файл в расширении .zip!",
            }
        )

    if file.size > MAX_AD_SIZE:
        return JSONResponse(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            content={
                "status": "False",
                "message": "Файл превышает допусимый размер!",
            }
        )

    try:

        name = str(uuid.uuid4())

        data = await file.read()

        with open(f'temp/{name}.zip', 'wb') as f:
            f.write(data)

        data = await initial_proceed(f"{name}.zip")

        res = await send_request("ap", data)

        if res.json().get("status") != "True":
            return JSONResponse(
                "Something went wrong!",
            )

        os.remove(f'temp/{name}.zip')

        return {
            "status": "True",
        }

    except Exception as e:
        return {
            "status": "False",
            "message": str(e)
        }

@app.get("/api/v1/management/web-health/get-p")
async def get_post():
    pass

@app.get("/api/v1/management/web-health/get-ps")
async def get_posts():
    pass

@app.get("/api/v1/management/web-health/get-u")
async def get_user():
    pass


@app.get("/api/v1/management/web-health/get-us")
async def get_users():
    pass


@app.get("/api/v1/management/web-health/m/{destination}")
async def manipulate(
        destination: str,
):
    pass


@app.post("/api/v1/management/web-health/request")
async def request():
    pass


@app.post("/api/v1/management/web-health/request-c")
async def requests_close():
    pass
