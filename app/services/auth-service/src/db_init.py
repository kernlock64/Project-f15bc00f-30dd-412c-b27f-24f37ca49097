from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base, DeclarativeBase
import os

SqlAlchemyBase = declarative_base()


def get_async_session():
    engine = create_async_engine(
        os.getenv("DATABASE_URL"),
        echo=False,
        pool_pre_ping=True
    )

    __factory = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    return __factory


class Base(DeclarativeBase):
    pass


session = get_async_session()