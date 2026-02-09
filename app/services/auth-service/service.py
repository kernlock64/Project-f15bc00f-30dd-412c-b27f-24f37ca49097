import uuid

from fastapi import FastAPI, HTTPException, File, UploadFile, Form, Depends, Request
import httpx
import json
import hashlib
import secrets
from pydantic import BaseModel, EmailStr
import jwt

from sqlalchemy import *

from httpx import Limits, AsyncClient, HTTPError
from db_init import get_async_session, engine
from model import AuthDB
from email_validator import EmailNotValidError, validate_email
from contextlib import asynccontextmanager
from sqlalchemy.orm import Session

limits = Limits(
    max_connections=100,
    max_keepalive_connections=20,
)


class BaseResponse(BaseModel):
    status: str
    message: str


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
    async with get_async_session() as session:
        yield session


async def check_data(password, email, username, session: Session = Depends(get_db())):
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


@app.post("/registration", response_model=BaseResponse)
async def registration(data: RegistrationScheme, request: Request, file: UploadFile = File(...),
                       session: Session = Depends(get_db()), client: AsyncClient = Depends(get_async_session)):
    try:
        username = data.username
        password = data.password
        hashed_password = hashlib.sha256(data.password.encode()).hexdigest()
        email = data.email
        role = data.role

        files = {
            "file": (file.filename, file.file, file.content_type),
        }
        check = check_data(password, email, username)

        if check["status"] == "False":
            return BaseResponse(
                status="False",
                message=check["message"],
            )

        user = AuthDB(email=email, hashed_password=hashed_password, session_id=uuid.uuid4())

        data = {
            "username": username,
            "email": email,
            "role": role,
            "uid"
        }

        client.post()

    except HTTPError as err:
        return BaseResponse(
            status="False",
            message=str(err),
        )


    return BaseResponse(
        status="True",
        message="Success"
    )


@app.post("/login", response_model=BaseResponse)
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
