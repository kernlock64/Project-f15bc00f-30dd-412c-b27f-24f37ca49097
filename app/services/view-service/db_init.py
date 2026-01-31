import sqlalchemy.ext.asyncio as async_sql
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base

SqlAlchemyBase = declarative_base()


def get_async_factory(user, password, host, port, db_name):
    conn_str = f'postgresql+asyncpg://{user}:{password}@{host}:{port}/{db_name}'

    engine = create_async_engine(
        conn_str,
        echo=False,
        pool_pre_ping=True
    )

    factory = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    return factory

ServiceFactory = get_async_factory("user", "pass", "localhost", "5432", "service_1_db")