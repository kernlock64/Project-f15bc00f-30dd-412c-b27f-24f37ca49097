from fastapi import FastAPI, Depends, Request, Form, UploadFile, File, status
from fastapi.responses import JSONResponse
import httpx
from sqlalchemy import *
import uuid
import json
import os
from datetime import *
import jwt

from httpx import Limits, AsyncClient, HTTPError

from model import TicketsDB, PriceListsDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession

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
        "gp",
        "ap",
        "up",
        "sp",
        "da",
        "va",
        "uu",
        "gu",
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
async def get_post(
        pid: str
):
    res = await send_request("gp", {"pid": pid})

    if res.json().get("status") != "True":
        return JSONResponse(
            "Something went wrong!",
        )

    return res.json()


@app.get("/api/v1/management/web-health/get-ps")
async def get_posts(
        page: int = 1,
        limit: int = 10,
        fil: str = None,
        q: str = None,
):
    res = await send_request("sp", {"page": page, "limit": limit, "filter": fil, "query": q})

    if res.json().get("status") != "True":
        return JSONResponse(
            "Something went wrong!",
        )

    return res.json()


@app.get("/api/v1/management/web-health/get-u")
async def get_user(
        uid: str

):
    res = await send_request("gp", {"uid": uid})

    if res.json().get("status") != "True":
        return {
            "status": "False",
            "message": res.json()
        }

    return res.json()


@app.get("/api/v1/management/web-health/get-us")
async def get_users():
    pass


@app.get("/api/v1/management/web-health/m/{destination}")
async def manipulate(
        destination: str,
        data: dict,
):
    try:
        match destination:

            case "dp":
                payload = {
                    "pid": data["pid"],
                }

                res = await send_request("dp", payload)

                if res.json().get("status") != "True":
                    return {
                        "status": "False",
                        "message": res.json()
                    }

                return res.json()

            case "da":
                payload = {
                    "uid": data["uid"],
                }

                res = await send_request("da", payload)

                if res.json().get("status") != "True":
                    return {
                        "status": "False",
                        "message": res.json()
                    }

                return res.json()

            case "up":
                payload = {
                    "changed_data": data["changed_data"],
                }

                res = await send_request("up", payload)

                if res.json().get("status") != "True":
                    return {
                        "status": "False",
                        "message": res.json()
                    }

                return res.json()

            case "up":
                payload = {
                    "changed_data": data["changed_data"],
                }

                res = await send_request("up", payload)

                if res.json().get("status") != "True":
                    return {
                        "status": "False",
                        "message": res.json()
                    }

                return res.json()

    except KeyError:

        return JSONResponse(
            "Отсутствует один, или несколько обязательных аргументов!"
        )

    except:
        return JSONResponse(
            "Something went wrong!",
        )


@app.post("/api/v1/management/web-health/request")
async def request(
        data: str,
        db: AsyncSession = Depends(get_db),
):
    try:
        db.add(
            TicketsDB(id=str(uuid.uuid4()), author_id=str(data["author_id"]), ticket_status="В РАБОТЕ",
                      theme=str(data["theme"]),
                      text=str(data["text"]), date_created=datetime.now(tz=timezone.utc),)
        )

    except Exception:
        return JSONResponse(
            "Something went wrong!",
        )


@app.post("/api/v1/management/web-health/request-take")
async def request_take(
        data: str,
        db: AsyncSession = Depends(get_db),
):

    # Add nickname for admin identification to db (later)
    pass


@app.post("/api/v1/management/web-health/get-request")
async def get_requests(
        page: int = 1,
        limit: int = 10,
        db: AsyncSession = Depends(get_db),

):
    offset = limit * page
    count = db.scalar(select(func.count()).select_from(TicketsDB))

    res = await db.scalars(select(TicketsDB).offset(offset).limit(limit))

    return {
        "count": count,
        "data": [res.all()]
    }


@app.post("/api/v1/management/web-health/request-c")
async def requests_close(
        request_id: str,
        db: AsyncSession = Depends(get_db),

):

    tickets = await db.execute(select(TicketsDB).where(TicketsDB.id == request_id))
    ticket = tickets.scalar_one_or_none()
    await db.delete(ticket)
    await db.commit()

    return JSONResponse(
        "True"
    )