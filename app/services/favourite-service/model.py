from db_init import Base
from sqlalchemy import Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

class FavouriteDB(Base):
    __tablename__ = "favourite-service"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True, nullable=False)
    uid: Mapped[int] = mapped_column(Integer, nullable=False)
    pid: Mapped[str] = mapped_column(Text, nullable=False)
    p_image: Mapped[str] = mapped_column(Text, nullable=False)
    p_title: Mapped[str] = mapped_column(Text, nullable=False)