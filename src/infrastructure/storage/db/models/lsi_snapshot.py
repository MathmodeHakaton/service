"""
Модель для снимков LSI (Liquidity Sentiment Index)
"""

from datetime import datetime
from sqlalchemy import Column, Integer, Float, DateTime
from .base import DeclarativeBase


class LSISnapshot(DeclarativeBase):
    """Снимок LSI в конкретный момент времени"""

    __tablename__ = "lsi_snapshots"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(DateTime, index=True, default=datetime.now)
    value = Column(Float, nullable=False)  # 0.0 - 1.0

    # Вклады от модулей
    m1_reserves = Column(Float)
    m2_repo = Column(Float)
    m3_ofz = Column(Float)
    m4_tax = Column(Float)
    m5_treasury = Column(Float)

    # Статусы
    status = Column(Integer)  # 0: normal, 1: warning, 2: critical
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    def __repr__(self):
        return f"<LSISnapshot date={self.date} value={self.value}>"
