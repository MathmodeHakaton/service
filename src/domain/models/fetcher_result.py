from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class RUONIA_table_row:
    """DTO: строка из таблицы RUONIA"""

    dt: datetime  # Дата ставки
    rate: float  # Ставка RUONIA, % годовых
    volume: float  # Объем сделок, млрд руб.
    deals_count: int  # Количество сделок
    participants: int  # Количество участников
    min_rate: float  # Минимальная ставка, % годовых
    pct_25: float  # 25-й процентиль ставок, % годовых
    pct_75: float  # 75-й процентиль ставок, % годовых
    max_rate: float  # Максимальная ставка, % годовых
    calc_status: str  # Статус расчета (e.g., "Стандартный")
    published_date: datetime  # Дата публикации


@dataclass
class RUONIA_table_result:
    """DTO: результат работы fetcher для таблицы RUONIA"""

    last_updated: datetime
    status: str  # "success", "partial", "error"
    data: list[RUONIA_table_row]
    status_message: Optional[str] = None


@dataclass
class RequiredReserves_table_row:
    """DTO: строка из таблицы Обязательные резервы"""

    period_beining: datetime  # Период усреднения обязательных резервов
    reserve_amount: float  # Фактические среднедневные остатки средств на корсчетах
    min_reserve: float  # Обязательные резервы, подлежащие усреднению на корсчетах
    stagntion_amount: float  # Обязательные резервы на счетах для их учета
    total_organizations: int  # Количество организаций, у которых были обязательные резервы
    active_organizations: int
    # Количество организаций, активных в период усреднения обязательных резервов
    calendar_days: int  # Количество календарных дней в периоде усреднения обязательных резервов


@dataclass
class RequiredReserves_table_result:
    """DTO: результат работы fetcher для таблицы Обязательные резервы"""

    last_updated: datetime
    status: str  # "success", "partial", "error"
    data: list[RequiredReserves_table_row]
    status_message: Optional[str] = None
