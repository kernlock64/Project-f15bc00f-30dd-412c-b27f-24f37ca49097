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


ROUTES = {

    "users": "http://user-service:5004",
    "posts": "http://post-service:5003",
    "favourites": "http://favourite-service:5005",
}


@app.middleware("http")
async def proxy(request: Request, call_next):
    if request.url.path in {"/registration", "/login", "/refresh", "/forgot-password"}:
        return await call_next(request)

    token = request.cookies.get("access_t")
    if not token:
        token = request.query_params.get("r_token")
    public_key = os.getenv("PUBLIC_KEY").replace("\\n", "\n")

    print(token)

    if not token:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "status": "False",
                "message": "Unauthorized!",
            }
        )

    try:
        payload = jwt.decode(token, public_key, algorithms="ES256")

    except jwt.InvalidSignatureError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "status": "False",
                "message": "Unauthorized!",
            }
        )

    except jwt.ExpiredSignatureError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "status": "False",
                "message": "Token has expired",
            }
        )

    eng = app.state.db
    if payload["thm"] == "access":
        async with eng() as db:
            version = await db.execute(select(AuthDB.token_version).where(AuthDB.uid == payload["uid"]))
            v = version.scalar_one_or_none()

            if not v:
                return {
                    "status": "False",
                    "message": "User is not exists!"
                }

        if v != payload["tov"]:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "status": "False",
                    "message": "Unauthorized!",
                }
            )

        headers = MutableHeaders(scope=request.scope)

        headers['X-User-Id'] = payload["uid"]
        headers['X-Role'] = str(payload["role"])

        if hasattr(request, "_headers"):
            delattr(request, "_headers")

        resp = await call_next(request)
        resp.headers["X-User-Id"] = payload["uid"]

        return resp

    elif payload["thm"] == "reset":
        async with eng() as db:
            users = await db.execute(select(AuthDB).where(AuthDB.email == payload["e"]))
            u = users.scalar_one_or_none()

        if u.hashed_password != payload["p"]:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "status": "False",
                    "message": "Password has been already changed!",
                }
            )

        headers = MutableHeaders(scope=request.scope)

        headers['X-User-Id'] = u.uid

        if hasattr(request, "_headers"):
            delattr(request, "_headers")

        resp = await call_next(request)
        resp.headers["X-User-Id"] = u.uid

        return resp

    return {
        "status": "False",
        "message": "Unexpected error!"
    }


async def check_data(password, email, username, session: AsyncSession):
    check_flag = False

    if len(password) < 8:
        return {"status": "False", "message": "Пароль должен быть как минимум 8 символов!"}

    for letter in password:
        if letter.isdigit():
            check_flag = True
            break
        if letter.isupper():
            check_flag = True
            break
    if not check_flag:
        return {"status": "False", "message": "Пароль должен содержать в себе минимум 1 заглавную букву, и одну цифру!"}

    result = await session.execute(select(exists().where(AuthDB.email == email)))
    if result.scalar():
        return {"status": "False", "message": "Пользователь уже существует!"}

    if 3 > len(username):
        return {"status": "False", "message": "Недопустимое имя пользователя! Минимальная длина имени 3 символа!"}

    if len(username) > 15:
        return {"status": "False", "message": "Недопустимое имя пользователя! Максимальная длина имени 15 символов!"}

    if ' ' in username or any([i in '!@#$%^&*()_+=-' for i in username]):
        return {"status": "False",
                "message": "Недопустимое имя пользователя! Имя пользователя не может содержать пробелы!"}

    try:
        validate_email(email, check_deliverability=True)


    except EmailNotValidError:
        return {"status": "False", "message": 'E-mail недействителен!'}

    return {"status": "True", }


@app.post("/forgot-password")
async def forgot_password(
        db: AsyncSession = Depends(get_db),
        email: str = Form(...),
        client: AsyncClient = Depends(get_httpx),
):
    try:
        res = await db.execute(select(AuthDB).where(AuthDB.email == email))
        user = res.scalar_one_or_none()

        if user:
            payload = {
                "e": user.email,
                "p": user.hashed_password,
                "thm": "reset",
                "exp": datetime.now(tz=timezone.utc) + timedelta(seconds=300)
            }
            prv_key = os.getenv("SECRET_KEY").replace("\\n", "\n")
            reset_t = jwt.encode(payload, prv_key, algorithm="ES256")

            # DEBUG-INFO
            print(reset_t)
            # DEBUG-INFO

            await client.post("http://mail-service:5002/mail/on-password-forget", data={"r_token": reset_t})

        return {
            "status": "True",
            "message": "Запрос на смену пароля был успешно создан!"
        }

    except HTTPError as err:
        return {
            "status": "False",
            "message": err
        }


@app.post("/logout")
async def logout(
        request: Request,
        db: AsyncSession = Depends(get_db),
):
    uid = request.headers.get('X-User-Id')

    await db.execute(
        update(AuthDB)
        .where(AuthDB.uid == uid)
        .values(token_version=AuthDB.token_version + 1))

    await db.execute(
        update(TokenDB)
        .where(TokenDB.uid == uid)
        .values(is_expired=True))

    await db.commit()

    response = JSONResponse(status_code=status.HTTP_200_OK, content={"status": "True"})
    response.delete_cookie(key="refreshment_t")
    response.delete_cookie(key="access_t")

    return response


@app.post("/registration")
async def registration(
        response: Response,
        file: UploadFile = File(...),
        session: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
        username: str = Form(...),
        role: int = Form(...),
        password: str = Form(...),
        email: str = Form(...),
        telephone: str = Form(None),
):
    try:
        if not telephone and role == 2:
            return {"status": "False",
                    "message": "Пожалуйста, предоставьте телефонный номер!"}

        hashed_password = hashlib.sha256(password.encode()).hexdigest()

        content = await file.read()

        files = {
            "avatar": (file.filename, content, file.content_type),
        }

        check = await check_data(password, email, username, session=session)

        if check["status"] == "False":
            return {"status": check["status"], "message": check["message"]}

        user = AuthDB(email=email, hashed_password=hashed_password, date=datetime.now(tz=timezone.utc),
                      uid=str(uuid.uuid4()),
                      role=role, status="ACTIVE", token_version=1, username=username) if role == 1 else AuthDB(
            email=email,
            hashed_password=hashed_password,
            date=datetime.now(
                tz=timezone.utc),
            uid=str(uuid.uuid4()),
            role=role, status="PENDING",
            token_version=1,
            username=username, )
        session.add(user)

        payload = {
            "role": role,
            "uid": user.uid,
            "jit": str(uuid.uuid4()),
            "exp": datetime.now(tz=timezone.utc) + timedelta(seconds=900),
            "tov": user.token_version,
            "thm": "access"
        }

        data = {
            "username": username,
            "email": email,
            "role": role,
            "uid": user.uid,
            "telephone": telephone,
        }

        templ_data = {
            "email_to": email,
            "username": username,
        }

        resp = await client.post("http://user-service:5004/users/add-user", data=data, files=files,
                                 headers={"Content-Type": "multipart/form-data"})
        if resp.status_code != 200:
            await session.rollback()
            return {"status": "False", "message": str(resp.json()["message"])}

        resp = await client.post("http://mail-service:5002/mail/on-registration", json=templ_data,
                                 headers={"Content-Type": "application/json"})
        if resp.status_code != 200:
            await session.rollback()
            return {"status": "False", "message": str(resp.json()["message"])}

        else:
            await session.commit()

            if user.role == 2:
                return {
                    "status": "True",
                    "message": "Ваша заявление принято на рассмотрение!"
                }

            prv_key = os.getenv("SECRET_KEY").replace("\\n", "\n")

            access_t = jwt.encode(payload, prv_key, algorithm="ES256")
            refreshment_t = str(secrets.token_urlsafe(64))
            token = TokenDB(uid=user.uid, token=refreshment_t,
                            expires_at=(datetime.now(tz=timezone.utc) + timedelta(days=30)), is_expired=False)
            session.add(token)
            await session.commit()

            response.set_cookie(key="refreshment_t", value=refreshment_t, httponly=True, secure=True)
            response.set_cookie(key="access_t", value=access_t, secure=True, httponly=True)

            return {"status": "True"}


    except HTTPError as err:
        return {
            "status": "False",
            "message": str(err),
        }


async def deletion_task(uid,
                        client: AsyncClient,
                        ):
    try:
        await client.post("http://user-service:5004/users/delete-user", headers={"X-User-Id": str(uid)})
        return {"status": "True"}
    except HTTPError as err:
        return {
            "status": "False",
            "message": str(err),
        }


@app.post("/delete-user")
async def delete(
        request: Request,
        background_tasks: BackgroundTasks,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    try:
        uid = request.headers.get("X-User-Id")

        background_tasks.add_task(deletion_task, uid, client)

        await db.execute(
            update(TokenDB)
            .where(TokenDB.uid == uid)
            .values(is_expired=True))

        users = await db.execute(select(AuthDB).where(AuthDB.uid == uid))
        user = users.scalar_one_or_none()
        await db.delete(user)
        await db.commit()

        response = JSONResponse(status_code=status.HTTP_200_OK, content={"status": "True"})
        response.delete_cookie(key="refreshment_t")
        response.delete_cookie(key="access_t")

        return response

    except HTTPError as e:
        return {
            "status": "False",
            "message": str(e),
        }


@app.post("/login")
async def login(
        response: Response,
        background_tasks: BackgroundTasks,
        session: AsyncSession = Depends(get_db),
        password: str = Form(...),
        email: str = Form(...),
        client: AsyncClient = Depends(get_httpx),
):
    try:

        hashed_password = hashlib.sha256(password.encode()).hexdigest()

        users = await session.execute(select(AuthDB).where(AuthDB.email == email))
        user = users.scalars().one_or_none()

        if not user:
            return {
                "status": "False",
                "message": "Неверный адрес электронной почты или пароль!"
            }

        if user.hashed_password != hashed_password:
            return {
                "status": "False",
                "message": "Неверный адрес электронной почты или пароль!",
            }

        if user.status != "ACTIVE":
            if user.status == "PENDING":
                return {
                    "status": "False",
                    "message": "Заявка на рассмотрении. Благодарим за ожидание, в скором времени с вами свяжутся"
                }
            if user.status == "REJECTED":
                background_tasks.add_task(deletion_task, str(user.uid), client)
                users = await session.execute(select(AuthDB).where(AuthDB.uid == str(user.uid)))
                user = users.scalar_one_or_none()
                user.token_version += 1
                await session.delete(user)
                await session.commit()
                return {
                    "status": "False",
                    "message": "К сожалению, ваша заявка была отклонена."
                }

        payload = {
            "role": user.role,
            "uid": user.uid,
            "jit": str(uuid.uuid4()),
            "exp": datetime.now(tz=timezone.utc) + timedelta(seconds=900),
            "tov": user.token_version,
            "thm": "access"
        }

        prv_key = os.getenv("SECRET_KEY").replace("\\n", "\n")

        access_t = jwt.encode(payload, prv_key, algorithm="ES256")
        refreshment_t = str(secrets.token_urlsafe(64))
        token = TokenDB(uid=user.uid, token=refreshment_t,
                        expires_at=(datetime.now(tz=timezone.utc) + timedelta(days=30)), is_expired=False)
        session.add(token)
        await session.commit()

        response.set_cookie(key="refreshment_t", value=refreshment_t, httponly=True, secure=True)
        response.set_cookie(key="access_t", value=access_t, secure=True, httponly=True)

        return {"status": "True"}


    except HTTPError as err:
        return {
            "status": "False",
            "message": str(err),
        }


@app.post("/change-password")
async def change_password(
        request: Request,
        session: AsyncSession = Depends(get_db),
        new_password: str = Form(...),
):
    try:
        headers = request.headers
        uid = headers.get("X-User-Id")

        users = await session.execute(select(AuthDB).where(AuthDB.uid == uid))
        user = users.scalars().one_or_none()

        if not user:
            return {
                "status": "False",
                "message": "Пользователя не существует!"
            }

        user.hashed_password = hashlib.sha256(new_password.encode()).hexdigest()
        await session.commit()

        return {
            "status": "True",
        }

    except HTTPError as err:
        return {
            "status": "False",
            "message": str(err),
        }


@app.post("/change-email")
async def change_email(
        request: Request,
        session: AsyncSession = Depends(get_db),
        new_email: str = Form(...),
):
    try:
        headers = request.headers
        uid = headers.get("X-User-Id")

        users = await session.execute(select(AuthDB).where(AuthDB.uid == uid))
        user = users.scalars().one_or_none()

        if not user:
            return {
                "status": "False",
                "message": "Пользователя не существует!"
            }

        user.email = new_email
        await session.commit()

        return {
            "status": "True",
        }

    except HTTPError as err:
        return {
            "status": "False",
            "message": str(err),
        }


@app.post("/refresh")
async def refresh(
        request: Request,
        response: Response,
        session: AsyncSession = Depends(get_db),
):
    try:

        refreshment_t = request.cookies.get("refreshment_t")

        tokens = await session.execute(select(TokenDB).where(TokenDB.token == refreshment_t))
        token = tokens.scalars().one_or_none()
        uid = token.uid

        if not token or not uid:
            return {
                "status": "False",
                "message": "Invalid data!"
            }

        if token.is_expired:
            e_tokens = await session.execute(select(TokenDB).where(TokenDB.uid == uid))
            for t in e_tokens.scalars():
                t.is_expired = True

            await session.commit()

            return {
                "status": "False",
                "message": "Suspicious activity!"
            }

        if token.expires_at < datetime.now(tz=timezone.utc):
            return {
                "status": "False",
                "message": "Token expired!"
            }

        else:

            token.is_expired = True
            await session.commit()

            users = await session.execute(select(AuthDB).where(AuthDB.uid == uid))
            user = users.scalars().one_or_none()

            payload = {
                "role": user.role,
                "uid": user.uid,
                "jit": str(uuid.uuid4()),
                "exp": datetime.now(tz=timezone.utc) + timedelta(seconds=900),
                "tov": user.token_version,
                "thm": "access"
            }

            prv_key = os.getenv("SECRET_KEY").replace("\\n", "\n")

            access_t = jwt.encode(payload, prv_key, algorithm="ES256")
            refreshment_t = str(secrets.token_urlsafe(64))
            n_token = TokenDB(uid=user.uid, token=refreshment_t,
                              expires_at=(datetime.now(tz=timezone.utc) + timedelta(days=30)), is_expired=False)
            session.add(n_token)
            await session.commit()

            response.set_cookie(key="refreshment_t", value=refreshment_t, httponly=True, secure=True)
            response.set_cookie(key="access_t", value=access_t, secure=True, httponly=True)

            return {"status": "True"}

    except HTTPError as err:
        return {
            "status": "False",
            "message": str(err),
        }


@app.api_route("/{path:path}", methods=["GET", "POST", "DELETE"])
async def proxy(request: Request, client: AsyncClient = Depends(get_httpx), ):
    path = request.url.path
    target_url = ROUTES.get(path.split("/")[1])

    if not target_url:
        return {
            "status": "False",
            "message": "Invalid URL!"
        }

    if request.url.query:
        final_url = f"{target_url + path}?{request.url.query}"
    else:
        final_url = target_url + path

    headers = dict(request.headers)
    headers.pop("host", None)

    req = client.build_request(
        method=request.method,
        url=final_url,
        headers=headers,
        content=request.stream(),
    )

    response = await client.send(req, stream=True)
    return StreamingResponse(
        content=response.aiter_raw(),
        status_code=response.status_code,
        headers=response.headers,
    )
