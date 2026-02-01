from typing import List

from db_init import Base
from sqlalchemy import Integer, DateTime, Text, JSON
from sqlalchemy.orm import Mapped, mapped_column

class PostDB(Base):
    __tablename__ = "post-service"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, auto_increment=True, nullable=False)
    author_id: Mapped[int] = mapped_column(Integer, nullable=False)
    title: Mapped[Text] = mapped_column(Text, nullable=False)
    description: Mapped[Text] = mapped_column(Text, nullable=False)
    images_array: Mapped[List[int]] = mapped_column(JSON, nullable=False)