from db_init import Base
from sqlalchemy import Integer, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime

class MailDB(Base):
    __tablename__ = "mail-service"

    id: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)
    to: Mapped[str] = mapped_column(Text, nullable=False)
    date: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)