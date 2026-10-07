from datetime import datetime
from typing import List

from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape
from db_init import Base
from sqlalchemy import Integer, Text, JSON, DateTime
from sqlalchemy.orm import Mapped, mapped_column

class PostDB(Base):
    __tablename__ = "post-service"

    id: Mapped[str] = mapped_column(Text, nullable=False, primary_key=True)
    author_id: Mapped[str] = mapped_column(Text, nullable=False)
    title: Mapped[Text] = mapped_column(Text, nullable=False)
    description: Mapped[Text] = mapped_column(Text, nullable=False)
    price: Mapped[int] = mapped_column(Integer, nullable=False)
    images: Mapped[List[str]] = mapped_column(JSON)
    title_image: Mapped[str] = mapped_column(Text, nullable=False,)
    directory_name: Mapped[str] = mapped_column(Text)
    posted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    category_id: Mapped[int] = mapped_column(Integer, nullable=False)
    price_list_name: Mapped[str] = mapped_column(Text, nullable=False)
    coordinates: Mapped[str] = mapped_column(
        Geometry(geometry_type='POINT', srid=4326, spatial_index=True)
    )

    @property
    def coordinate(self):
        coordinate = to_shape(self.coordinates)
        return coordinate.x, coordinate.y
