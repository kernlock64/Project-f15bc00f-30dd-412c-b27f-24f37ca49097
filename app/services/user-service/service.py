import os
import uuid

from fastapi import FastAPI, File, UploadFile, Depends, Request, Form, Body
import httpx
from datetime import datetime, timezone
from sqlalchemy import *
from httpx import Limits, AsyncClient
from db_init import get_async_session, engine
from model import CustomersDB, ProdDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession

MEDIA_KEY = os.getenv("MEDIA_KEY")

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
async def get_user(
        request: Request,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    uid = request.headers.get("X-User-Id")
    role = request.headers.get("X-Role")

    res1 = await client.get(f"http://favourite-service:5005/favourites/get-favourites", headers=request.headers)

    if res1.json()["status"] != "True":
        return {
            "status": "False",
            "message": "Something went wrong!",
        }
    if role == "1":
        users = await db.execute(select(CustomersDB).where(CustomersDB.id == uid))
        user = users.scalar_one_or_none()
        return {
            "id": str(user.id),
            "username": str(user.username),
            "email": str(user.email),
            "image": str(user.image),
            "date_created": str(user.date_created),
            "full_name": str(user.full_name),
            "favourites": str(res1.json()["favourites"]),
        }
    if role == "2":

        res2 = await client.get(f"http://post-service:5003/posts/get-posts", headers=request.headers)
        if res2.json()["status"] != "True":
            return {
                "status": "False",
                "message": "Something went wrong!",
            }

        users = await db.execute(select(ProdDB).where(ProdDB.id == uid))
        user = users.scalar_one_or_none()
        return {
            "id": str(user.id),
            "name": str(user.name),
            "email": str(user.email),
            "image": str(user.image),
            "date_created": str(user.date_created),
            "full_name": str(user.full_name),
            "telephone": str(user.telephone),
            "description": str(user.description),
            "delivery_description": str(user.delivery_description),
            "favourites": str(res1.json()["favourites"]),
            "posts": str(res2.json()["posts"]),
        }
    else:
        return {
            "status": "False",
            "message": "Unknown error!",
        }


@app.post("/users/add-user")
async def add_user(
        db: AsyncSession = Depends(get_db),
        avatar: UploadFile = File(...),
        client: AsyncClient = Depends(get_httpx),
        username: str = Form(...),
        email: str = Form(...),
        role: str = Form(...),
        uid: str = Form(...),
        telephone: str = Form(...),
):
    async def read_file():
        while chunk := await avatar.read(64 * 1024):
            yield chunk

    filename = str(uuid.uuid4())

    req = client.build_request(
        method="POST",
        url=f"http://media-service:5001/media/add-image/{avatar.content_type.split("/")[1]}",
        headers={"X-Key": MEDIA_KEY, "X-Name": filename},
        content=read_file(),
    )

    resp = await client.send(req, stream=True)

    if resp.status_code != 200:
        return {
            "status": "False",
            "message": "Error adding image",
        }
    else:
        user = CustomersDB(id=uid, username=username, email=email, date_created=datetime.now(timezone.utc),
                           image=str(filename + avatar.content_type.split("/")[1])) if role == "1" else ProdDB(
            name=username, id=uid, email=email,
            date_created=datetime.now(
                timezone.utc),
            image=str(filename + avatar.content_type.split("/")[1]),
            telephone=telephone, )
        db.add(user)
        await db.commit()

        return {
            "status": "True",
        }


@app.post("/users/delete-user")
async def delete_user(
        request: Request,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    uid = request.headers.get("X-User-Id")
    role = request.headers.get("X-Role")

    res1 = await client.post(f"http://favourite-service:5005/favourites/deletion", headers=request.headers)

    if res1.json()["status"] != "True":
        return {
            "status": "False",
            "message": "Error deleting user",
        }
    else:
        if role == "2":
            res2 = await client.post(f"http://post-service:5003/posts/deletion", headers=request.headers)
            if res2.json()["status"] != "True":
                return {
                    "status": "False",
                    "message": "Error deleting user",
                }
        cls = CustomersDB if role == "1" else ProdDB
        users = await db.execute(select(cls).where(cls.id == uid))
        user = users.scalar_one_or_none()

        await client.post(f"http://media-service:5001/media/delete-image/{user.image}",
                          headers={"X-Key": MEDIA_KEY})

        await db.delete(user)
        await db.commit()
        return {
            "status": "True",
        }


@app.post("/users/change-userdata")
async def change_userdata(
        request: Request,
        db: AsyncSession = Depends(get_db),
        changed_data: dict = Body,
):
    uid = request.headers.get("X-User-Id")
    role = request.headers.get("X-Role")
    cls = CustomersDB if role == "1" else ProdDB

    users = await db.execute(select(cls).where(cls.id == uid))
    user = users.scalar_one_or_none()

    for key, value in changed_data.items():
        if not key in cls.__table__.columns:
            return {
                "status": "False",
                "message": "No such parameter!",
            }
        setattr(user, key, value)

    return {
        "status": "True",
    }


@app.post("/users/change-avatar")
async def change_avatar(
        request: Request,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
        avatar: UploadFile = File(),
):
    uid = request.headers.get("X-User-Id")
    role = request.headers.get("X-Role")
    cls = CustomersDB if role == "1" else ProdDB

    filename = await db.execute(select(cls.image).where(cls.id == uid))

    content = await avatar.read()

    files = {
        "avatar": (avatar.filename, content, avatar.content_type),
    }

    resp = await client.post(f"http://media-service:5001/media/change-image/{filename}", files=files,
                             headers={"X-Key": MEDIA_KEY})
    if resp.json()["status"] != "True":
        return {
            "status": "False",
            "message": "Error adding image",
        }

    return {
        "status": "True",
    }
