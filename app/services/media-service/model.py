from db_init import Base
from sqlalchemy import Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

class MediaDB(Base):
    __tablename__ = "media-service"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    path: Mapped[Text] = mapped_column(Text, nullable=False)
    filename: Mapped[Text] = mapped_column(Text, nullable=False)
    extension: Mapped[Text] = mapped_column(Text, nullable=False)