from db_init import Base
from sqlalchemy import Integer, DateTime, Text, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime

class AuthDB(Base):
    __tablename__ = "auth-service-users"

    uid: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False, )
    hashed_password: Mapped[str] = mapped_column(Text, nullable=False)
    date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    role: Mapped[int] = mapped_column(Integer, nullable=False)

class TokenDB(Base):
    __tablename__ = "auth-service-tokens"
    uid: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False, )
    token: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False, )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    is_expired: Mapped[bool] = mapped_column(Boolean, nullable=False)
