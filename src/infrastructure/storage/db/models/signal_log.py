"""
Модель для логирования сигналов модулей
"""

from datetime import datetime
from sqlalchemy import Column, Integer, Float, DateTime, String
from .base import DeclarativeBase


class SignalLog(DeclarativeBase):
    """Лог сигналов от отдельных модулей"""

    __tablename__ = "signal_logs"

    id = Column(Integer, primary_key=True, index=True)
    module_name = Column(String(50), index=True)  # M1, M2, M3, M4, M5
    signal_value = Column(Float)
    flag = Column(String(20))  # "normal", "warning", "critical"
    details = Column(String(500))
    timestamp = Column(DateTime, index=True, default=datetime.now)

    def __repr__(self):
        return f"<SignalLog {self.module_name} value={self.signal_value}>"
