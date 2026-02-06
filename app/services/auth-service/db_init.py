from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base, DeclarativeBase
import os

SqlAlchemyBase = declarative_base()

engine = create_async_engine(
    os.getenv("DATABASE_URL"),
    echo=False,
    pool_pre_ping=True,
    max_overflow=10,
    pool_recycle=3600,
    pool_size=5,
)

def get_async_session():

    global engine
    eng = engine

    __factory = sessionmaker(
        bind=eng,
        class_=AsyncSession,
        expire_on_commit=False
    )
    return __factory


class Base(DeclarativeBase):
    pass