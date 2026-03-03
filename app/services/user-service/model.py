from db_init import Base
from sqlalchemy import Integer, DateTime, Text
from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column

class CustomersDB(Base):
    __tablename__ = "user-service-customers"

    id: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)
    username: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False)
    image: Mapped[str] = mapped_column(Text, nullable=False)
    date_created: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    full_name: Mapped[str] = mapped_column(Text)

class ProdDB(Base):
    __tablename__ = "user-service-prod"

    id: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    email: Mapped[str] = mapped_column(Text, nullable=False)
    image: Mapped[str] = mapped_column(Text, nullable=False)
    date_created: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    telephone: Mapped[str] = mapped_column(Text)
    description: Mapped[str] = mapped_column(Text)
    delivery_description: Mapped[str] = mapped_column(Text)
    full_name: Mapped[str] = mapped_column(Text)