from db_init import Base
from sqlalchemy import Integer, DateTime, Text
from sqlalchemy.orm import Mapped, mapped_column

class UserDB(Base):
    __tablename__ = "user-service"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    username: Mapped[Text] = mapped_column(Text, nullable=False)
    email: Mapped[Text] = mapped_column(Text, nullable=False)
    image: Mapped[Integer] = mapped_column(Integer, nullable=False)
    date_created: Mapped[DateTime] = mapped_column(DateTime, nullable=False)