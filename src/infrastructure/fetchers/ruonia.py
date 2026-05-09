import logging
from datetime import datetime
import pandas as pd
from urllib.parse import quote

from src.domain.models.fetcher_result import (
    RUONIA_table_row, RUONIA_table_result,
)
from src.infrastructure.fetchers.base import BaseFetcher
from config.constants import CBR_RUONIA_URL

logger = logging.getLogger(__name__)


class RuoniaFetcher(BaseFetcher):
    """Fetcher для получения данных RUONIA из ЦБР"""

    def fetch(
        self,
        from_date: datetime,
        to_date: datetime
    ) -> RUONIA_table_result:
        """
        Получить данные RUONIA за период

        Args:
            from_date: начальная дата
            to_date: конечная дата

        Returns:
            RUONIA_table_result с данными или ошибкой
        """
        try:
            # Формирование URL с параметрами дат
            date_fmt = "%m/%d/%Y"
            url = (
                f"{CBR_RUONIA_URL}?"
                f"FromDate={quote(from_date.strftime(date_fmt))}&"
                f"ToDate={quote(to_date.strftime(date_fmt))}&"
                f"posted=False"
            )

            # Загрузка данных
            df = self._download_excel(url)

            if df is None:
                error_msg = f"Failed to download RUONIA data for period {from_date} - {to_date}"
                logger.error(error_msg)
                return RUONIA_table_result(
                    last_updated=datetime.now(),
                    status="error",
                    status_message=error_msg,
                    data=[],
                )

            # Парсинг данных
            rows = self._parse_ruonia_data(df)

            if not rows:
                error_msg = "No valid RUONIA data parsed"
                logger.warning(error_msg)
                return RUONIA_table_result(
                    last_updated=datetime.now(),
                    status="partial",
                    status_message=error_msg,
                    data=[],
                )

            logger.info(f"Successfully fetched {len(rows)} RUONIA records")
            return RUONIA_table_result(
                last_updated=datetime.now(),
                status="success",
                status_message=None,
                data=rows,
            )

        except Exception as e:
            error_msg = f"Error fetching RUONIA data: {str(e)}"
            logger.exception(error_msg)
            return RUONIA_table_result(
                last_updated=datetime.now(),
                status="error",
                status_message=error_msg,
                data=[],
            )

    def _parse_ruonia_data(self, df: pd.DataFrame) -> list[RUONIA_table_row]:
        """
        Парсинг DataFrame в список RUONIA_table_row

        Ожидаемая структура таблицы:
        - DT: Дата ставки
        - ruo: Ставка RUONIA, % годовых
        - vol: Объем сделок, млрд руб.
        - T: Количество сделок
        - C: Количество участников
        - MinRate: Минимальная ставка, % годовых
        - Percentil (1-й): 25-й процентиль ставок
        - Percentil (2-й): 75-й процентиль ставок
        - MaxRate: Максимальная ставка, % годовых
        - StatusXML: Статус расчета
        - DateUpdate: Дата публикации
        """
        rows = []

        # Нормализация названий колонок (убираем пробелы)
        df.columns = [col.strip() for col in df.columns]

        for idx, row in df.iterrows():
            try:
                dt = self._parse_datetime(row.get('DT'))
                rate = self._parse_float(row.get('ruo'))
                volume = self._parse_float(row.get('vol'))
                deals_count = self._parse_int(row.get('T'))
                participants = self._parse_int(row.get('C'))
                min_rate = self._parse_float(row.get('MinRate'))

                # Получаем оба процентиля по индексу в исходном DataFrame
                percentil_values = row[[
                    col for col in df.columns if col == 'Percentil']].tolist()
                pct_25 = self._parse_float(percentil_values[0]) if len(
                    percentil_values) > 0 else -1.0
                pct_75 = self._parse_float(percentil_values[1]) if len(
                    percentil_values) > 1 else -1.0

                max_rate = self._parse_float(row.get('MaxRate'))
                calc_status = str(row.get('StatusXML', "Unknown")).strip()
                published_date = self._parse_datetime(row.get('DateUpdate'))

                ruonia_row = RUONIA_table_row(
                    dt=dt,
                    rate=rate,
                    volume=volume,
                    deals_count=deals_count,
                    participants=participants,
                    min_rate=min_rate,
                    pct_25=pct_25,
                    pct_75=pct_75,
                    max_rate=max_rate,
                    calc_status=calc_status,
                    published_date=published_date,
                )
                rows.append(ruonia_row)

            except Exception as e:
                logger.warning(f"Error parsing RUONIA row {idx}: {e}")
                continue

        return rows

    def _parse_datetime(self, value) -> datetime:
        """Парсинг даты с fallback на текущее время"""
        if value is None or pd.isna(value):
            return datetime.now()
        if isinstance(value, datetime):
            return value
        try:
            return pd.to_datetime(value).to_pydatetime()
        except:
            return datetime.now()

    def _parse_float(self, value) -> float:
        """Парсинг float с fallback на -1.0"""
        if value is None or pd.isna(value):
            return -1.0
        try:
            return float(str(value).replace(',', '.'))
        except:
            return -1.0

    def _parse_int(self, value) -> int:
        """Парсинг int с fallback на -1"""
        if value is None or pd.isna(value):
            return -1
        try:
            return int(float(str(value).replace(',', '.')))
        except:
            return -1
