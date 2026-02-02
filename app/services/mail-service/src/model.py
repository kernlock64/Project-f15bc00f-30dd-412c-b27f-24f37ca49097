from db_init import Base
from sqlalchemy import Integer, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime

class MailDB(Base):
    __tablename__ = "mail-service"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, auto_increment=True, nullable=False)
    to: Mapped[int] = mapped_column(Text, nullable=False)
    date: Mapped[datetime] = mapped_column(DateTime, nullable=False)