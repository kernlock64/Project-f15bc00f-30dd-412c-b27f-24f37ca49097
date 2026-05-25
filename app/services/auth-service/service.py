import uuid
import json
from fastapi import FastAPI, Depends, Request, Response, Form, status, BackgroundTasks
from fastapi.responses import JSONResponse
from phonenumbers import NumberParseException
from starlette.responses import StreamingResponse
from starlette.datastructures import MutableHeaders
import httpx
import hashlib
import secrets
import jwt
import phonenumbers
from datetime import datetime, timedelta, timezone
from sqlalchemy import *
import os
import aio_pika
import asyncio


class RabbitMQGateway:

    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(f"amqp://admin:{os.getenv("DB_PASSWORD")}@rabbitmq/")
        self.channel = await self.connection.channel()

    async def close(self):
        if self.connection:
            await self.connection.close()


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
    rabbit = RabbitMQGateway()
    await rabbit.connect()
    app.state.rabbit = rabbit

    user_actions = await rabbit.channel.declare_exchange(
        "v1.exchange.user_actions",
        aio_pika.ExchangeType.DIRECT,
        durable=True
    )

    iternal_user_validation = await rabbit.channel.declare_exchange(
        "iternal_user_validation",
        aio_pika.ExchangeType.DIRECT,
        durable=True
    )

    iternal_user_validation_queue = await rabbit.channel.declare_queue(
        "check_user_actions",
        durable=True
    )

    await iternal_user_validation_queue.bind(
        iternal_user_validation,
        routing_key="iternal_validation",
    )

    await rabbit.channel.declare_queue(
        "wait_for_user_confirmation",
        durable=True,
        arguments={
            "x-message-ttl": 25200000,
            "x-dead-letter-exchange": "iternal_user_validation",
            "x-dead-letter-routing-key": "iternal_validation"
        }
    )

    app.state.iternal_user_validation = iternal_user_validation
    app.state.exchange = user_actions
    app.state.default_exchange = rabbit.channel.default_exchange

    app.state.task = asyncio.create_task((main()))

    try:
        yield
    finally:
        await client.aclose()
        await engine.dispose()
        await rabbit.close()
        app.state.task.cancel()


app = FastAPI(lifespan=lifespan)


async def publish(data: dict, routing_key):
    exchange = app.state.exchange

    message_body = json.dumps(data).encode()
    message = aio_pika.Message(
        body=message_body,
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )

    if not routing_key:
        return {
            "status": "False",
            "message": "Please enter valid route!",
        }
    await exchange.publish(message, routing_key=routing_key)

    return {
        "status": "True",
    }


async def iternal_publish(uid):
    exchange = app.state.default_exchange
    routing_key = "wait_for_user_confirmation"

    message_body = json.dumps({"X-User-Id": uid}).encode()
    message = aio_pika.Message(
        body=message_body,
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )

    await exchange.publish(message, routing_key=routing_key)

    return {
        "status": "True",
    }


async def user_check(uid, eng):
    async with eng() as db:
        users = await db.execute(select(AuthDB).where(AuthDB.uid == uid))
        user = users.scalar_one_or_none()

        if user.status != "ACTIVE":
            await db.delete(user)
        await db.commit()


async def listen_check():
    try:
        async with app.state.iternal_user_validation.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    data = json.loads(message.body.decode())
                    uid = data["X-User-Id"]

                    await user_check(uid, app.state.db)
    except Exception as e:
        return {
            "status": "False",
            "message": str(e)
        }


async def main():
    await asyncio.gather(
        listen_check(),
    )


async def get_httpx() -> httpx.AsyncClient:
    return app.state.http_client


async def get_db():
    factory = app.state.db
    async with factory() as session:
        yield session


ROUTES = {

    "users": "http://user-service:5004",
    "posts": "http://post-service:5003",
    "favourites": "http://favourite-service:5005",
}

ROUTES_NA = {
    "posts": "http://post-service:5003",
}


@app.middleware("http")
async def proxy(request: Request, call_next):
    if request.url.path in {"/api/v1/registration", "/api/v1/login", "/api/v1/refresh", "/api/v1/forgot-password",
                            "/api/v1/user-confirm"}:
        return await call_next(request)

    token = request.cookies.get("access_t")

    if not token:
        token = request.query_params.get("r_token")

    if not token:
        token = request.headers.get("admin_t")

    public_key = os.getenv("PUBLIC_KEY").replace("\\n", "\n")

    if not token:
        headers = MutableHeaders(scope=request.scope)

        headers['X-User-Id'] = "-1"
        headers['X-Role'] = "-1"

        if hasattr(request, "_headers"):
            delattr(request, "_headers")
        resp = await call_next(request)
        return resp

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

        headers['X-User-Id'] = str(payload["uid"])
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

        if request.url.path != "/change-password":
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={
                    "status": "False",
                    "message": "Unauthorized!",
                }
            )

        headers = MutableHeaders(scope=request.scope)

        headers['X-User-Id'] = u.uid

        if hasattr(request, "_headers"):
            delattr(request, "_headers")

        resp = await call_next(request)
        resp.headers["X-User-Id"] = u.uid

        return resp

    elif payload["thm"] == "admin":

        headers = MutableHeaders(scope=request.scope)

        headers['X-User-Id'] = "0"
        headers['X-Role'] = "0"
        headers['X-Scope'] = payload["dest"]

        if hasattr(request, "_headers"):
            delattr(request, "_headers")

        resp = await call_next(request)
        return resp

    return {
        "status": "False",
        "message": "Unexpected error!"
    }


@app.post("/api/v1/user-confirm")
async def user_confirm(
        token: str,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    public_key = os.getenv("PUBLIC_KEY").replace("\\n", "\n")

    try:
        payload = jwt.decode(token, public_key, algorithms="ES256")

    except jwt.InvalidSignatureError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "status": "False",
                "message": "Ссылка недействительна или была просрочена!",
            }
        )

    except jwt.ExpiredSignatureError:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "status": "False",
                "message": "Ссылка недействительна или была просрочена!",
            }
        )

    uid = payload.get("i")
    users = await db.execute(select(AuthDB).where(AuthDB.uid == uid))
    user = users.scalar_one_or_none()
    telephone = payload.get("t")

    if user.status == "ACTIVE":
        return JSONResponse({
            "status": "False",
            "message": "Ссылка уже была активирована!"
        })

    role = payload.get("r")
    user.status = "ACTIVE" if role == 1 else "VERIFIED"
    await db.commit()

    await client.post("http://user-service:5004/api/v1/users/add-user", data={
        "username": user.username,
        "email": user.email,
        "role": user.role,
        "uid": user.uid,
        "telephone": telephone,
    })
    await client.post("http://mail-service:5002/api/v1/mail/on-registration", json={
        "username": user.username,
    },
                      headers={"Content-Type": "application/json"})

    return {"status": "True"}

@app.post("/api/v1/admin-accept")
async def user_confirm_a(
        request: Request,
        db: AsyncSession = Depends(get_db),
):

    public_key = os.getenv("PUBLIC_KEY").replace("\\n", "\n")

    try:
        payload = jwt.decode(request.headers.get("admin_t"), public_key, algorithms="ES256")

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
                "message": "Unauthorized!",
            }
        )

    if payload.get("dest") != "aa":
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={
                "status": "False",
                "message": "Unauthorized!",
            }
        )

    data = request.headers.get("data")
    uid = data["uid"]
    users = await db.execute(select(AuthDB).where(AuthDB.uid == uid))
    user = users.scalar_one_or_none()

    if user.role == 2:
        user.status = "ACTIVE"
    await db.commit()


    return {"status": "True"}


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


@app.post("/api/v1/forgot-password")
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

            templ_data = {
                "email_to": user.email,
                "username": user.username,
                "r_token": reset_t
            }

            await client.post("http://mail-service:5002/api/v1/mail/on-password-forget", json=templ_data,
                              headers={"Content-Type": "application/json"})

        return {
            "status": "True",
            "message": "Запрос на смену пароля был успешно создан!"
        }

    except HTTPError as err:
        return {
            "status": "False",
            "message": err
        }


@app.post("/api/v1/logout")
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


@app.post("/api/v1/registration")
async def registration(
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

        if telephone:
            try:
                parsed = phonenumbers.parse(telephone, "RU")

                if not phonenumbers.is_valid_number(parsed):
                    return {"status": "False",
                            "message": "Некорректный формат телефонного номера!"}

                telephone = phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)

            except NumberParseException:
                return {"status": "False",
                        "message": "Некорректный формат телефонного номера!"}

        hashed_password = hashlib.sha256(password.encode()).hexdigest()
        check = await check_data(password, email, username, session=session)

        if check["status"] == "False":
            return {"status": check["status"], "message": check["message"]}

        user = AuthDB(email=email, hashed_password=hashed_password, date=datetime.now(tz=timezone.utc),
                      uid=str(uuid.uuid4()),
                      role=role, status="PENDING", token_version=1, username=username) if role == 1 else AuthDB(
            email=email,
            hashed_password=hashed_password,
            date=datetime.now(
                tz=timezone.utc),
            uid=str(uuid.uuid4()),
            role=role, status="PENDING",
            token_version=1,
            username=username, )
        session.add(user)

        templ_data = {
            "email_to": email,
            "username": username,
        }

        await session.commit()

        message = {
            "status": "True",
            "message": "Пожалуйста подтвердите вашу электронную почту, перейдя по отправленной ссылке"
        }

        if user.role == 2:

            resp = await client.post("http://mail-service:5002/api/v1/mail/on-requesting-provider", json=templ_data,
                                     headers={"Content-Type": "application/json"})
            if resp.status_code != 200:
                await session.rollback()
                return {"status": "False", "message": str(resp.json()["message"])}

            message = {
                "status": "True",
                "message": "Ваша заявление принято на рассмотрение!"
            }

        payload = {
            "i": user.uid,
            "t": telephone,
            "exp": datetime.now(tz=timezone.utc) + timedelta(hours=7),
            "r": role
        }

        prv_key = os.getenv("SECRET_KEY").replace("\\n", "\n")

        token = jwt.encode(payload, prv_key, algorithm="ES256")

        template_data = {
            "token": token,
            "username": username,
            "email_to": email,
        }

        await client.post("http://mail-service:5002/api/v1/mail/on-confirmation-mail", json=template_data,
                          headers={"Content-Type": "application/json"})

        return message


    except HTTPError as err:
        return {
            "status": "False",
            "message": str(err),
        }


async def deletion_task(uid, role
                        ):
    try:
        await publish({"X-User-Id": str(uid), "X-Role": str(role)}, routing_key="v1.queue.deletion")
        return {"status": "True"}
    except HTTPError as err:
        return {
            "status": "False",
            "message": str(err),
        }


@app.post("/api/v1/delete-user")
async def delete(
        request: Request,
        background_tasks: BackgroundTasks,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    try:
        uid = request.headers.get("X-User-Id")
        role = request.headers.get("X-Role")

        background_tasks.add_task(deletion_task, uid, role)

        await db.execute(
            update(TokenDB)
            .where(TokenDB.uid == uid)
            .values(is_expired=True))

        users = await db.execute(select(AuthDB).where(AuthDB.uid == uid))
        user = users.scalar_one_or_none()
        await db.delete(user)
        await db.commit()

        templ_data = {
            "email_to": user.email,
            "username": user.username,
        }

        resp = await client.post("http://mail-service:5002/api/v1/mail/on-deletion", json=templ_data,
                                 headers={"Content-Type": "application/json"})
        if resp.status_code != 200:
            await db.rollback()
            return {"status": "False", "message": str(resp.json()["message"])}

        response = JSONResponse(status_code=status.HTTP_200_OK, content={"status": "True"})
        response.delete_cookie(key="refreshment_t")
        response.delete_cookie(key="access_t")

        return response

    except HTTPError as e:
        return {
            "status": "False",
            "message": str(e),
        }


@app.post("/api/v1/login")
async def login(
        response: Response,
        background_tasks: BackgroundTasks,
        session: AsyncSession = Depends(get_db),
        password: str = Form(...),
        email: str = Form(...),
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
                    "message": "На вашу почту была отправлена ссылка для подтверждения регистрации. Пожалуйста, проверьте папку 'Спам' или папку 'Входящие'"
                }
            if user.status == "VERIFIED":
                return {
                    "status": "False",
                    "message": "Заявка на рассмотрении. Благодарим за ожидание, в скором времени с вами свяжутся"
                }
            if user.status == "REJECTED":
                background_tasks.add_task(deletion_task, str(user.uid), str(user.role))
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


@app.post("/api/v1/change-password")
async def change_password(
        request: Request,
        session: AsyncSession = Depends(get_db),
        new_password: str = Form(...),
        client: AsyncClient = Depends(get_httpx),
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

        templ_data = {
            "email_to": user.email,
            "username": user.username,
        }

        resp = await client.post("http://mail-service:5002/api/v1/mail/on-password-change", json=templ_data,
                                 headers={"Content-Type": "application/json"})
        if resp.status_code != 200:
            await session.rollback()
            return {"status": "False", "message": str(resp.json()["message"])}

        return {
            "status": "True",
        }

    except HTTPError as err:
        return {
            "status": "False",
            "message": str(err),
        }


@app.post("/api/v1/change-email")
async def change_email(
        request: Request,
        session: AsyncSession = Depends(get_db),
        new_email: str = Form(...),
):
    try:
        headers = request.headers
        uid = headers.get("X-User-Id")
        role = headers.get("X-Role")

        users = await session.execute(select(AuthDB).where(AuthDB.uid == uid))
        user = users.scalars().one_or_none()

        if not user:
            return {
                "status": "False",
                "message": "Пользователя не существует!"
            }

        user.email = new_email

        await publish({"X-User-Id": str(uid), "X-Role": str(role), "X-Changed-Value": new_email},
                      routing_key="v1.queue.changes")

        await session.commit()

        return {
            "status": "True",
        }

    except HTTPError as err:
        return {
            "status": "False",
            "message": str(err),
        }


@app.post("/api/v1/refresh")
async def refresh(
        request: Request,
        response: Response,
        session: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    try:

        refreshment_t = request.cookies.get("refreshment_t")

        tokens = await session.execute(select(TokenDB).where(TokenDB.token == refreshment_t))
        token = tokens.scalars().one_or_none()

        uid = token.uid

        users = await session.execute(select(AuthDB).where(AuthDB.uid == uid))
        user = users.scalars().one_or_none()

        if not token or not uid:
            return {
                "status": "False",
                "message": "Invalid data!"
            }

        if token.is_expired:

            await session.execute(
                update(AuthDB)
                .where(AuthDB.uid == uid)
                .values(token_version=AuthDB.token_version + 1))

            await session.execute(
                update(TokenDB)
                .where(TokenDB.uid == uid)
                .values(is_expired=True))

            await session.commit()

            templ_data = {
                "email_to": user.email,
                "username": user.username,
            }

            resp = await client.post("http://mail-service:5002/api/v1/mail/on-suspicious-activity", json=templ_data,
                                     headers={"Content-Type": "application/json"})
            if resp.status_code != 200:
                await session.rollback()
                return {"status": "False", "message": str(resp.json()["message"])}

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
    try:
        target_url = ROUTES.get(path.split("/")[3])
    except Exception:
        return {
            "status": "False",
            "message": "Invalid URL!"
        }

    if request.headers.get("X-User-Id") == "-1" and ROUTES_NA.get(path.split("/")[3]) is None:
        return {
            "status": "False",
            "message": "Non-registered user!"
        }

    if not target_url:
        return {
            "status": "False",
            "message": "Invalid URL!"
        }

    exc = {"content-length", "host", "connection", "keep-alive", "proxy-authenticate", "te", "trailers",
           "transfer-encoding", "upgrade"}

    if request.url.query:
        final_url = f"{target_url + path}?{request.url.query}"
    else:
        final_url = target_url + path

    headers = {key: value for key, value in request.headers.items() if key.lower() not in exc}

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
