from datetime import datetime
from typing import List

from db_init import Base
from sqlalchemy import Integer, Text, JSON, ForeignKey, DateTime
from sqlalchemy.orm import Mapped, mapped_column

class PostDB(Base):
    __tablename__ = "post-service"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    author_id: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[Text] = mapped_column(Text, nullable=False)
    description: Mapped[Text] = mapped_column(Text, nullable=False)
    images: Mapped[List[int]] = mapped_column(JSON)
    directory_name: Mapped[str] = mapped_column(Text, nullable=False)
    posted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    category_id: Mapped[int] = mapped_column(Integer, nullable=False)