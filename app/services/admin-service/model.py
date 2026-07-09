from db_init import Base
from sqlalchemy import Integer, DateTime, Text, JSON
from typing import List
from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column

class TicketsDB(Base):
    __tablename__ = "admin-service-tickets"

    id: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)
    author_id: Mapped[str] = mapped_column(Text)
    ticket_status: Mapped[str] = mapped_column(Text)
    theme: Mapped[str] = mapped_column(Text, nullable=False)
    details: Mapped[List[str]] = mapped_column(JSON)
    date_created: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class PriceListsDB(Base):
    __tablename__ = "admin-service-price-lists"

    id: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)