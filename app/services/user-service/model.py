from db_init import Base
from sqlalchemy import Integer, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column

class CustomersDB(Base):
    __tablename__ = "user-service-customers"

    id: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)
    username: Mapped[Text] = mapped_column(Text, nullable=False)
    email: Mapped[Text] = mapped_column(Text, nullable=False)
    image: Mapped[Integer] = mapped_column(Integer, nullable=False)
    date_created: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)

class ProdDB(Base):
    __tablename__ = "user-service-prod"

    id: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)
    name: Mapped[Text] = mapped_column(Text, nullable=False)
    email: Mapped[Text] = mapped_column(Text, nullable=False)
    image: Mapped[Integer] = mapped_column(Integer, nullable=False)
    date_created: Mapped[DateTime] = mapped_column(DateTime(timezone=True), nullable=False)
    telephone: Mapped[Text] = mapped_column(Text)
    description: Mapped[Text] = mapped_column(Text)
    delivery_description: Mapped[Text] = mapped_column(Text)
    full_name: Mapped[Text] = mapped_column(Text)