import logging
from datetime import datetime
from typing import Optional
import pandas as pd

from src.domain.models.fetcher_result import (
    RequiredReserves_table_result, RequiredReserves_table_row
)
from src.infrastructure.fetchers.base import BaseFetcher
from config.constants import CBR_RESERVES_URL

logger = logging.getLogger(__name__)


class RequiredReservesFetcher(BaseFetcher):
    """Fetcher для получения данных об обязательных резервах из ЦБР"""

    # Индексы колонок в таблице (0-based, после skiprows)
    COL_PERIOD = 0       # Период усреднения (Excel serial date)
    COL_RESERVE = 1      # Фактические среднедневные остатки на корсчетах
    COL_MIN_RESERVE = 2  # Обязательные резервы, подлежащие усреднению
    COL_STAGNATION = 3   # Обязательные резервы на счетах для учёта
    COL_TOTAL_ORGS = 4   # Количество организаций (всего)
    COL_ACTIVE_ORGS = 5  # Количество организаций (действующих)
    COL_CALENDAR_DAYS = 7  # Число календарных дней в периоде

    def fetch(
        self,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None,
    ) -> RequiredReserves_table_result:
        """
        Получить данные об обязательных резервах.

        Args:
            from_date: начальная дата фильтрации (включительно), None = без ограничения
            to_date:   конечная дата фильтрации (включительно), None = без ограничения

        Returns:
            RequiredReserves_table_result с данными или ошибкой
        """
        try:
            df = self._download_excel(CBR_RESERVES_URL)

            if df is None:
                error_msg = "Failed to download required reserves data"
                logger.error(error_msg)
                return RequiredReserves_table_result(
                    last_updated=datetime.now(),
                    status="error",
                    status_message=error_msg,
                    data=[],
                )

            rows = self._parse_reserves_data(df, from_date, to_date)

            if not rows:
                error_msg = "No valid reserves data parsed"
                logger.warning(error_msg)
                return RequiredReserves_table_result(
                    last_updated=datetime.now(),
                    status="partial",
                    status_message=error_msg,
                    data=[],
                )

            logger.info(f"Successfully fetched {len(rows)} reserves records")
            return RequiredReserves_table_result(
                last_updated=datetime.now(),
                status="success",
                status_message=None,
                data=rows,
            )

        except Exception as e:
            error_msg = f"Error fetching required reserves data: {str(e)}"
            logger.exception(error_msg)
            return RequiredReserves_table_result(
                last_updated=datetime.now(),
                status="error",
                status_message=error_msg,
                data=[],
            )

    def _parse_reserves_data(
        self,
        df: pd.DataFrame,
        from_date: Optional[datetime],
        to_date: Optional[datetime],
    ) -> list[RequiredReserves_table_row]:
        """
        Парсинг DataFrame по позициям колонок.

        Колонка периода хранится как Excel serial number (int),
        поэтому конвертируем через pd.to_datetime с unit='D', origin='1899-12-30'.
        """
        rows = []

        for idx, row in df.iterrows():
            try:
                # Пропускаем пустые строки и заголовки
                raw_period = row.iloc[self.COL_PERIOD]
                if pd.isna(raw_period):
                    continue

                period_dt = self._parse_period(raw_period)
                if period_dt is None:
                    continue

                # Фильтрация по датам
                if from_date and period_dt < from_date:
                    continue
                if to_date and period_dt > to_date:
                    continue

                reserve_amount = self._parse_float(row.iloc[self.COL_RESERVE])
                min_reserve = self._parse_float(row.iloc[self.COL_MIN_RESERVE])
                stagnation_amount = self._parse_float(
                    row.iloc[self.COL_STAGNATION])

                # Пропускаем строки без числовых данных (заголовки)
                if reserve_amount < 0 and min_reserve < 0 and stagnation_amount < 0:
                    continue

                total_organizations = self._parse_int(
                    row.iloc[self.COL_TOTAL_ORGS])
                active_organizations = self._parse_int(
                    row.iloc[self.COL_ACTIVE_ORGS])
                calendar_days = self._parse_int(
                    row.iloc[self.COL_CALENDAR_DAYS])

                rows.append(RequiredReserves_table_row(
                    period_beining=period_dt,
                    reserve_amount=reserve_amount,
                    min_reserve=min_reserve,
                    stagntion_amount=stagnation_amount,
                    total_organizations=total_organizations,
                    active_organizations=active_organizations,
                    calendar_days=calendar_days,
                ))

            except Exception as e:
                logger.warning(f"Error parsing reserves row {idx}: {e}")
                continue

        return rows

    def _parse_period(self, value) -> Optional[datetime]:
        """
        Конвертация Excel serial number или строки в datetime.
        Excel считает дни от 1899-12-30 (баг Lotus 123, намеренно сохранён).
        """
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        try:
            # Числовой serial date (38231 → 2004-08-01)
            serial = int(float(str(value)))
            return pd.Timestamp('1899-12-30') + pd.Timedelta(days=serial)
        except (ValueError, TypeError):
            pass
        try:
            return pd.to_datetime(value).to_pydatetime()
        except Exception:
            return None

    def _parse_float(self, value) -> float:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return -1.0
        try:
            return float(str(value).replace(',', '.'))
        except Exception:
            return -1.0

    def _parse_int(self, value) -> int:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return -1
        try:
            # Значение может содержать примечания типа "806 (614)8"
            cleaned = str(value).split()[0].replace(',', '.')
            return int(float(cleaned))
        except Exception:
            return -1
