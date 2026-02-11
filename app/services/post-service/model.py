from typing import List

from db_init import Base
from sqlalchemy import Integer, Text, JSON
from sqlalchemy.orm import Mapped, mapped_column

class PostDB(Base):
    __tablename__ = "post-service"

    id: Mapped[str] = mapped_column(Text, primary_key=True, nullable=False)
    author_id: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[Text] = mapped_column(Text, nullable=False)
    description: Mapped[Text] = mapped_column(Text, nullable=False)
    images_array: Mapped[List[int]] = mapped_column(JSON, nullable=False)