from contextlib import asynccontextmanager
from datetime import *
from fastapi.middleware.cors import CORSMiddleware

import httpx
import jwt
from fastapi import FastAPI, Depends, UploadFile, File, HTTPException, Body, Form
from httpx import Limits
from sqlalchemy import *
from sqlalchemy.ext.asyncio import AsyncSession
import json

from db_init import get_async_session, engine
from model import TicketsDB, PriceListsDB
from shared.exception_handler import *
from shared.logger_middleware import *
from shared.logger import *

limits = Limits(
    max_connections=1000,
    max_keepalive_connections=100,
)

MAX_AD_SIZE = 30 * 1024 * 1024


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
        app.state.task.cancel()


app = FastAPI(lifespan=lifespan)

'''ВРЕМЕННО'''

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

'''ВРЕМЕННО'''


async def get_httpx() -> httpx.AsyncClient:
    return app.state.http_client


async def get_db():
    factory = app.state.db
    async with factory() as session:
        yield session


app.add_exception_handler(Exception, universal_server_error_handler)
app.add_exception_handler(StarletteException, universal_server_http_error_handler)
app.middleware("http")(request_trace)

async def send_request(
        dest: str,
        data: dict,
        request: Request,
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
        raise HTTPException(
            status_code=400,
            detail="Invalid destination!",
        )

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

    logger.info(
        f"Admin request to {dest_url[dest[1]]} was created", extra={
            "ip": request.headers.get("X-Real-IP"),
            "request_id": request_id.get(),
        }
    )

    return resp, resp.status_code


@app.post("/api/v1/management/web-health/push")
async def add_price_list(
        request: Request,
        file: UploadFile = File(...),
        db: AsyncSession = Depends(get_db),
):
    from file_proceed import initial_proceed
    if not file.filename.endswith('.zip'):
        raise HTTPException(
            status_code=400,
            detail="Incorrect file extension (should be .zip)",
        )

    if file.size > MAX_AD_SIZE:
        raise HTTPException(
            status_code=400,
            detail="The file exceeds allowed size limit!",
        )

    name = str(uuid.uuid4())

    data = await file.read()

    with open(f'temp/{name}.zip', 'wb') as f:
        f.write(data)

    data = await initial_proceed(f"{name}.zip")

    try:
        if data["metadata"]["status"] != "True":
            os.remove(f"temp/{name}.zip")
            raise HTTPException(
                status_code=400,
                detail="Incorrect file upload! Check the documentation",
            )
        price_list_name = data["metadata"]["price-list-name"]

    except Exception as e:
        logger.error(
            "Error in proceeding data!", extra={
                "data": data,
                "ip": request.headers.get("X-Real-IP"),
                "error": e,
                "request_id": request_id.get(),
            }
        )
        os.remove(f"temp/{name}.zip")
        raise HTTPException(
            status_code=400,
            detail="Incorrect file upload! Check the documentation",
        )

    db.add(PriceListsDB(id=name, name=price_list_name))

    res, res_status = await send_request("ap", data, request)

    if res_status != 200:
        logger.error(
            "External server error has occurred!", extra={
                "status_code": res_status,
                "ip": request.headers.get("X-Real-IP"),
                "error": res.text[:500],
                "request_id": request_id.get(),
            }
        )
        raise HTTPException(
            status_code=res_status,
            detail=res.json().get("detail"),
        )

    await db.commit()

    os.remove(f"temp/{name}.zip")


@app.get("/api/v1/management/web-health/get-p")
async def get_post(
        request: Request,
        pid: str = Form(...)
):
    res, res_status = await send_request("gp", {"pid": pid}, request)

    if res_status != 200:
        logger.error(
            "External server error has occurred!", extra={
                "status_code": res_status,
                "ip": request.headers.get("X-Real-IP"),
                "error": res.text[:500],
                "request_id": request_id.get(),
            }
        )
        raise HTTPException(
            status_code=res_status,
            detail=res.json().get("detail"),
        )

    return res.json()


@app.get("/api/v1/management/web-health/get-ps")
async def get_posts(
        request: Request,
        page: int = 1,
        limit: int = 10,
        fil: str = None,
        q: str = None,
):
    res, res_status = await send_request("sp", {"page": page, "limit": limit, "filter": fil, "query": q}, request)

    if res_status != 200:
        logger.error(
            "External server error has occurred!", extra={
                "status_code": res_status,
                "ip": request.headers.get("X-Real-IP"),
                "error": res.text[:500],
                "request_id": request_id.get(),
            }
        )

        raise HTTPException(
            status_code=res_status,
            detail=res.json().get("detail"),
        )

    return res.json()


@app.get("/api/v1/management/web-health/get-u")
async def get_user(
        request: Request,
        uid: str = Form(...)

):
    res, res_status = await send_request("gu", {"uid": uid}, request)

    if res_status != 200:
        logger.error(
            "External server error has occurred!", extra={
                "status_code": res_status,
                "ip": request.headers.get("X-Real-IP"),
                "error": res.text[:500],
                "request_id": request_id.get(),
            }
        )
        raise HTTPException(
            status_code=res_status,
            detail=res.json().get("detail"),
        )

    return res.json()


@app.get("/api/v1/management/web-health/get-us")
async def get_users():
    pass


@app.post("/api/v1/management/web-health/m/{destination}")
async def manipulate(
        request: Request,
        destination: str,
        data: str = Form(...),
):
    try:
        match destination:

            case "dp":
                payload = {
                    "post_id": data,
                }

                res, res_status = await send_request("dp", payload, request)

                if res_status != 200:
                    logger.error(
                        "External server error has occurred!", extra={
                            "status_code": res_status,
                            "ip": request.headers.get("X-Real-IP"),
                            "error": res.text[:500],
                            "request_id": request_id.get(),
                        }
                    )
                    raise HTTPException(
                        status_code=res_status,
                        detail=res.json().get("detail"),
                    )

            case "da":
                payload = {
                    "user_id": data,
                }

                res, res_status = await send_request("da", payload, request)

                if res_status != 200:
                    logger.error(
                        "External server error has occurred!", extra={
                            "status_code": res_status,
                            "ip": request.headers.get("X-Real-IP"),
                            "error": res.text[:500],
                            "request_id": request_id.get(),
                        }
                    )
                    raise HTTPException(
                        status_code=res_status,
                        detail=res.json().get("detail"),
                    )

            case "up":
                dat = json.loads(data)
                payload = {
                    "changed_data": dat,
                    "post_id": dat["post_id"],
                }

                dat.pop("post_id")

                res, res_status = await send_request("up", payload, request)

                if res_status != 200:
                    logger.error(
                        "External server error has occurred!", extra={
                            "status_code": res_status,
                            "ip": request.headers.get("X-Real-IP"),
                            "error": res.text[:500],
                            "request_id": request_id.get(),
                        }
                    )
                    raise HTTPException(
                        status_code=res_status,
                        detail=res.json().get("detail"),
                    )

            case "uu":
                payload = {
                    "changed_data": json.loads(data),
                }

                res, res_status = await send_request("uu", payload, request)

                if res_status != 200:
                    logger.error(
                        "External server error has occurred!", extra={
                            "status_code": res_status,
                            "ip": request.headers.get("X-Real-IP"),
                            "error": res.text[:500],
                            "request_id": request_id.get(),
                        }
                    )
                    raise HTTPException(
                        status_code=res_status,
                        detail=res.json().get("detail"),
                    )

    except KeyError:

        return JSONResponse(
            "Отсутствует один, или несколько обязательных аргументов!"
        )


@app.post("/api/v1/management/web-health/request")
async def request_make(
        theme: str = Body(...),
        uid: str = Body(...),
        details: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):
    db.add(
        TicketsDB(id=str(uuid.uuid4()), author_id=uid, ticket_status="ACTIVE",
                  theme=theme,
                  details=details, date_created=datetime.now(tz=timezone.utc), )
    )

    await db.commit()


@app.post("/api/v1/management/web-health/request-take")
async def request_take(
        data: str = Form(...),
        db: AsyncSession = Depends(get_db),
):
    # Add nickname for admin identification to db (later)
    pass


@app.get("/api/v1/management/web-health/get-request")
async def requests_get(
        page: int = 1,
        limit: int = 10,
        db: AsyncSession = Depends(get_db),

):
    offset = limit * (page - 1)
    count = await db.scalar(select(func.count()).select_from(TicketsDB))

    res = await db.execute(select(TicketsDB).offset(offset).limit(limit))

    return {
        "count": count,
        "data": [res.scalars().all()]
    }


@app.post("/api/v1/management/web-health/request-c")
async def requests_close(
        req_id: str = Form(...),
        db: AsyncSession = Depends(get_db),

):
    tickets = await db.execute(select(TicketsDB).where(TicketsDB.id == req_id))
    ticket = tickets.scalar_one_or_none()

    if not ticket:
        raise HTTPException(status_code=404, detail="NO_SUCH_TICKET")

    await db.delete(ticket)
    await db.commit()


@app.post("/api/v1/management/web-health/user-manipulate")
async def user_manipulate(
        request: Request,
        req_id: str = Form(...),
        verdict: int = Form(...),
        db: AsyncSession = Depends(get_db),

):
    if not req_id:
        raise HTTPException(status_code=404, detail="NO_SUCH_REQUEST")

    tickets = await db.execute(select(TicketsDB).where(TicketsDB.id == request_id.get()))
    ticket = tickets.scalar_one_or_none()

    if not ticket:
        raise HTTPException(
            status_code=404,
            detail="No such ticket!",
        )

    if int(verdict) not in [0, 1]:
        raise HTTPException(
            status_code=400,
            detail="Invalid verdict!",
        )

    payload = {
        "user_id": ticket.author_id,
        "verdict": verdict,
    }

    if verdict == 1:
        logger.info(
            f"User {ticket.author_id} was verified by admin", extra={
                "ip": request.headers.get("X-Real-IP"),
                "request_id": request_id.get().get(),
            }
        )

    if verdict == 0:
        logger.info(
            f"User {ticket.author_id} was rejected by admin", extra={
                "ip": request.headers.get("X-Real-IP"),
                "request_id": request_id.get(),
            }
        )

    res, res_status = await send_request("va", payload, request)

    if res_status != 200:
        logger.error(
            "External server error has occurred!", extra={
                "status_code": res_status,
                "ip": request.headers.get("X-Real-IP"),
                "error": res.text[:500],
                "request_id": request_id.get(),
            }
        )
        raise HTTPException(
            status_code=res_status,
            detail=res.json().get("detail"),
        )


@app.post("/api/v1/management/web-health/delete")
async def delete_price_list(
        request: Request,
        price_list_name: str = Form(...),
        db: AsyncSession = Depends(get_db),

):
    if not price_list_name:
        raise HTTPException(
            status_code=404,
            detail="No such price list!",
        )

    res = await db.execute(select(exists(PriceListsDB).where(PriceListsDB.name == price_list_name)))
    if not res.scalar():
        raise HTTPException(
            status_code=404,
            detail="No such ticket!",
        )

    payload = {
        "price-list-name": str(price_list_name),
    }

    res, res_status = await send_request("dp", payload, request)

    if res_status != 200:
        logger.error(
            "External server error has occurred!", extra={
                "status_code": res_status,
                "ip": request.headers.get("X-Real-IP"),
                "error": res.text[:500],
                "request_id": request_id.get(),
            }
        )
        raise HTTPException(
            status_code=res_status,
            detail=res.json().get("detail"),
        )
