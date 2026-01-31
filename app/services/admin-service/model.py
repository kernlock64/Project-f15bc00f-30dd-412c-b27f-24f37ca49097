from db_init import Base
from sqlalchemy.orm import Mapped, mapped_column
import datetime

class Admin(Base):
    __tablename__ = "admin-service"

    pass