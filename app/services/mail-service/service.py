from fastapi import FastAPI, Depends, Request, Form, BackgroundTasks, Body
import httpx
from sqlalchemy import *
import uuid
from datetime import datetime, timezone

from httpx import Limits, AsyncClient, HTTPError
import os

from db_init import get_async_session, engine
from model import MailDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi_mail import ConnectionConfig, MessageSchema, MessageType, FastMail, NameEmail
from pathlib import Path

# conf = ConnectionConfig(
#     MAIL_USERNAME="your_login@gmail.com",
#     MAIL_PASSWORD="your_app_password",
#     MAIL_FROM="noreply@myapp.com",
#     MAIL_PORT=587,
#     MAIL_SERVER="smtp.gmail.com",
#     MAIL_STARTTLS=True,
#     MAIL_SSL_TLS=False,
#     USE_CREDENTIALS=True,
#     TEMPLATE_FOLDER=Path(__file__).parent / 'templates'
# )

conf = ConnectionConfig(
    MAIL_USERNAME="",
    MAIL_PASSWORD="",
    MAIL_FROM="noreply@app.com",
    MAIL_PORT=1025,
    MAIL_SERVER="mailpit",
    MAIL_STARTTLS=False,
    MAIL_SSL_TLS=False,
    USE_CREDENTIALS=False,
    TEMPLATE_FOLDER=Path(__file__).parent / 'templates'
)

limits = Limits(
    max_connections=700,
    max_keepalive_connections=50,
)


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


async def send_email_async(subject: str, email_to: str, body: dict, template_name: str):

    message = MessageSchema(
        subject=subject,
        recipients=[NameEmail(email=email_to, name=body['username'])],
        template_body=body,
        subtype=MessageType.html,
        attachments = [
        {
            "file": "static/images/logo.png",
            "headers": {"Content-ID": "<logo_image>",
                        "Content-Disposition": "inline"},
            "mime_type": "image",
            "mime_subtype": "png"
        }
    ])



    fm = FastMail(conf)
    await fm.send_message(message, template_name=template_name)


@app.post("/mail/on-password-change")
async def password_change(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]
    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc), theme="password-change")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Смена пароля", email_to, data, "on-password-change.html")
    return {
        "status": "True",
    }


@app.post("/mail/on-password-forget")
async def password_forget(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]
    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc), theme="password-forget")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Сброс пароля", email_to, data, "on-password-forget.html")
    return {
        "status": "True",
    }


@app.post("/mail/on-suspicious-activity")
async def suspicious_activity(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]
    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc),
                  theme="suspicious-activity")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Подозрительная активность", email_to, data,
                              "on-suspicious-activity.html")
    return {
        "status": "True",
    }


@app.post("/mail/on-requesting-provider")
async def requesting_provider(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]
    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc),
                  theme="requesting-provider")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Запрос на регистрацию", email_to, data, "on-requesting-provider.html")
    return {
        "status": "True",
    }


@app.post("/mail/on-acceptance-provider")
async def registration_provider(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]
    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc),
                  theme="acceptance-provider")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Регистрация", email_to, data, "on-acceptance-provider.html")
    return {
        "status": "True",
    }


@app.post("/mail/on-deletion")
async def account_deletion(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]
    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc), theme="deletion")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Удаление аккаунта", email_to, data, "on-deletion.html")
    return {
        "status": "True",
    }


@app.post("/mail/on-registration")
async def registration(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]
    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc), theme="registration")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Регистрация", email_to, data, "on-registration.html")
    return {
        "status": "True",
    }
