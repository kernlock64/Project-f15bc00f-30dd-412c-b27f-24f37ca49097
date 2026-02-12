import uuid

from fastapi import FastAPI, HTTPException, File, UploadFile, Form, Depends, Request, Response
import httpx
import json
import hashlib
import secrets
from pydantic import BaseModel, EmailStr
import jwt
from datetime import datetime, timedelta
from sqlalchemy import *
import os

from httpx import Limits, AsyncClient, HTTPError
from db_init import get_async_session, engine
from model import AuthDB, TokenDB
from email_validator import EmailNotValidError, validate_email
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session

limits = Limits(
    max_connections=100,
    max_keepalive_connections=20,
)

async def reading_data():
    async with open("./keys/prv.pem") as f:
        private_key = f.read()
    async with open("./keys/pub.pem") as f:
        public_key = f.read()
    return private_key, public_key


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


async def check_data(password, email, username, session: Session = Depends(get_db)):
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

    if await session.execute(select(exists().where(AuthDB.email == email))).scalar():
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


class RegistrationScheme(BaseModel):
    username: str
    email: EmailStr
    password: str
    role: int


class LoginScheme(BaseModel):
    username: str
    email: EmailStr
    password: str


@app.post("/registration")
async def registration(item: RegistrationScheme, response: Response, file: UploadFile = File(...),
                       session: Session = Depends(get_db), client: AsyncClient = Depends(get_async_session)):
    try:
        username = item.username
        password = item.password
        hashed_password = hashlib.sha256(item.password.encode()).hexdigest()
        email = item.email
        role = item.role

        files = {
            "file": (file.filename, file.file, file.content_type),
        }

        check = await check_data(password, email, username)

        if check["status"] == "False":
            return {"status": check["status"], "message": check["message"]}

        user = AuthDB(email=email, hashed_password=hashed_password, date=datetime.now(), uid=uuid.uuid4())
        session.add(user)

        payload = {
            "role": role,
            "uid": user.uid,
            "lifetime": "900",
            "exp": datetime.now() + timedelta(seconds=900),
        }

        data = {
            "username": username,
            "email": email,
            "role": role,
            "uid": user.uid,
        }

        # resp = await client.post("http://user-service:5004/users/add-user", data=data, files=files)
        # if resp.status_code != 200:
        if 0:
            session.rollback()
            return {"status": "False", "message": str(resp.json()["message"])}
        else:
            session.commit()
            prv_key, pub_key = await reading_data()

            access_t = jwt.encode(payload, prv_key, algorithm="RS256")
            refreshment_t = uuid.uuid4()

            token = TokenDB(uid=user.uid, token=refreshment_t,
                            expires_at=(datetime.now() + timedelta(days=30)), is_expired=False)
            session.add(token)
            session.commit()

            response.set_cookie(key="refreshment_t", value=refreshment_t, httponly=True)
            response.set_cookie(key="access_t", value=access_t,)

            return {"status": "True"}


    except HTTPError as err:
        return {
            "status": "False",
            "message": str(err),
        }


@app.post("/login")
async def login(data: LoginScheme):
    """
    password:
    email:
    """

    pass


@app.post("/change-password")
async def change_password():
    """
    uid:
    """

    pass


@app.post("/change-email")
async def change_email():
    """
    uid:
    """

    pass

@app.post("/refresh")
async def refresh():
    pass
