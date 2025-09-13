from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer
from .db import Base

class ProjectMonthly(Base):
    __tablename__ = "project_monthly"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    month: Mapped[str] = mapped_column(String(7), index=True)  # YYYY-MM
    project_id: Mapped[str] = mapped_column(String(64), index=True)
    kam: Mapped[str] = mapped_column(String(128), index=True)
    status: Mapped[str] = mapped_column(String(8))
    potential: Mapped[int] = mapped_column(Integer)   # Potential (t / year)
    est_ay: Mapped[int] = mapped_column(Integer)      # FOC-2026 proxy (EST_AY)
    sop: Mapped[str] = mapped_column(String(32))      # SOP text
