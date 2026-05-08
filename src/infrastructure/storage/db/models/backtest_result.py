"""
Модель для результатов backtesting
"""

from datetime import datetime
from sqlalchemy import Column, Integer, Float, DateTime, String, Text
from .base import DeclarativeBase


class BacktestResult(DeclarativeBase):
    """Результат backtesting за период"""

    __tablename__ = "backtest_results"

    id = Column(Integer, primary_key=True, index=True)
    start_date = Column(DateTime)
    end_date = Column(DateTime)

    # Метрики
    sharpe_ratio = Column(Float)
    max_drawdown = Column(Float)
    total_return = Column(Float)
    win_rate = Column(Float)

    # Параметры
    parameters = Column(Text)  # JSON
    description = Column(String(500))

    created_at = Column(DateTime, default=datetime.now)

    def __repr__(self):
        return f"<BacktestResult {self.start_date} - {self.end_date}>"
