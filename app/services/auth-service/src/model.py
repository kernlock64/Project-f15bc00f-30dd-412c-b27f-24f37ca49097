from db_init import Base
from sqlalchemy import Integer, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime

class AuthDB(Base):
    __tablename__ = "auth-service"

    uid: Mapped[int] = mapped_column(Integer, primary_key=True, auto_increment=True, nullable=False)
    hashed_password: Mapped[str] = mapped_column(Text, nullable=False)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    session_id: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False)
