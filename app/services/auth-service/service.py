from fastapi import FastAPI
import httpx
import json
import hashlib
import secrets
from pydantic import BaseModel, EmailStr

from sqlalchemy import *

from db_init import get_async_session
from model import AuthDB
from email_validator import EmailNotValidError, validate_email

app = FastAPI()

async def check_data(password, email, username):
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

    async with get_async_session() as session:
        if await session.execute(select(exists().where(AuthDB.email==email))).scalar():
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
async def registration(raw_data: RegistrationScheme):
    pass

@app.post("/login")
async def login(raw_data: LoginScheme):

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