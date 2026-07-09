from fastapi import FastAPI, Depends, Request, HTTPException, BackgroundTasks, Body, status
from fastapi.responses import JSONResponse
from sqlalchemy import *
import uuid
from datetime import datetime, timezone

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

@asynccontextmanager
async def lifespan(cl: FastAPI):
    factory = get_async_session()
    app.state.db = factory
    try:
        yield
    finally:
        await engine.dispose()

app = FastAPI(lifespan=lifespan)

@app.exception_handler(Exception)
async def universal_server_error_handler(exc: Exception):

    print(exc)

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Что-то пошло не так. Пожалуйста, обратитесь в службу поддержки, если ошибка повторится."}
    )

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


@app.post("/api/v1/mail/on-password-change")
async def password_change(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]

    if not email_to:
        raise HTTPException(
            status_code=404,
            detail="No receiver!",
        )

    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc), theme="password-change")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Смена пароля", email_to, data, "on-password-change.html")



@app.post("/api/v1/mail/on-password-forget")
async def password_forget(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]

    if not email_to:
        raise HTTPException(
            status_code=404,
            detail="No receiver!",
        )

    '''FAKE-URL!'''

    data["url"] = "https://fake.url/" + data["r_token"]

    '''FAKE-URL!'''

    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc), theme="password-forget")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Сброс пароля", email_to, data, "on-password-forget.html")



@app.post("/api/v1/mail/on-suspicious-activity")
async def suspicious_activity(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]\

    if not email_to:
        raise HTTPException(
            status_code=404,
            detail="No receiver!",
        )

    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc),
                  theme="suspicious-activity")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Подозрительная активность", email_to, data,
                              "on-suspicious-activity.html")



@app.post("/api/v1/mail/on-requesting-provider")
async def requesting_provider(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]

    if not email_to:
        raise HTTPException(
            status_code=404,
            detail="No receiver!",
        )

    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc),
                  theme="requesting-provider")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Запрос на регистрацию", email_to, data, "on-requesting-provider.html")


@app.post("/api/v1/mail/on-acceptance-provider")
async def registration_provider(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]

    if not email_to:
        raise HTTPException(
            status_code=404,
            detail="No receiver!",
        )

    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc),
                  theme="acceptance-provider")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Регистрация", email_to, data, "on-acceptance-provider.html")


@app.post("/api/v1/mail/on-confirmation-mail")
async def on_confirming_mail(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]

    if not email_to:
        raise HTTPException(
            status_code=404,
            detail="No receiver!",
        )

    '''FAKE-URL!'''

    data["url"] = "https://fake.url/?token=" + data["token"]

    '''FAKE-URL!'''

    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc), theme="account-confirmation")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Подтверждение регистрации", email_to, data, "on-confirmation.html")



@app.post("/api/v1/mail/on-deletion")
async def account_deletion(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]

    if not email_to:
        raise HTTPException(
            status_code=404,
            detail="No receiver!",
        )

    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc), theme="deletion")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Удаление аккаунта", email_to, data, "on-deletion.html")



@app.post("/api/v1/mail/on-registration")
async def registration(
        background_tasks: BackgroundTasks,
        data: dict = Body(...),
        db: AsyncSession = Depends(get_db),
):

    email_to = data["email_to"]

    if not email_to:
        raise HTTPException(
            status_code=404,
            detail="No receiver!",
        )

    mail = MailDB(id=str(uuid.uuid4()), receiver_email=email_to, date=datetime.now(tz=timezone.utc), theme="registration")
    db.add(mail)
    await db.commit()

    background_tasks.add_task(send_email_async, "Регистрация", email_to, data, "on-registration.html")

