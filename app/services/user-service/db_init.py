from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base, DeclarativeBase
import os

SqlAlchemyBase = declarative_base()


def get_async_factory(user, password, host, port, db_name):
    engine = create_async_engine(
        f'postgresql+asyncpg://{user}:{password}@{host}:{port}/{db_name}',
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


session = get_async_factory(os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('DB_HOST'), os.getenv('DB_PORT'),
                            os.getenv('SERVICE_NAME'))
