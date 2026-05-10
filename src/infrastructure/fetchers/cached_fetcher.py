import json
import logging
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy.orm import Session

from src.infrastructure.fetchers.base import BaseFetcher, FetcherResult
from src.infrastructure.storage.db.queries.cache_queries import CacheQueries

logger = logging.getLogger(__name__)


class CachedFetcher:
    """
    Обёртка над любым BaseFetcher с персистентным кешем в PostgreSQL.

    Использование:
        fetcher = CachedFetcher(
            fetcher=MinfinFetcher(),
            source_key="minfin_ofz",
            ttl_hours=12,
            session=db_session,
        )
        result = fetcher.fetch()
    """

    def __init__(
        self,
        fetcher: BaseFetcher,
        source_key: str,
        ttl_hours: float,
        session: Session,
        force_refresh: bool = False,
    ):
        self.fetcher = fetcher
        self.source_key = source_key
        self.ttl = timedelta(hours=ttl_hours)
        self.session = session
        self.force_refresh = force_refresh

    def fetch(self) -> FetcherResult:
        if not self.force_refresh:
            cached = CacheQueries.get_if_fresh(self.session, self.source_key)
            if cached:
                logger.info(
                    "Cache hit for '%s' (%d rows, expires %s)",
                    self.source_key,
                    cached["row_count"],
                    cached["expires_at"],
                )
                return FetcherResult(
                    data=self._payload_to_df(cached["payload"]),
                    last_updated=cached["fetched_at"],
                    source_url=None,
                    status="cached",
                )

        logger.info("Cache miss for '%s', fetching...", self.source_key)
        result = self.fetcher.fetch()

        if result.status == "success" and result.data is not None:
            payload = self._df_to_payload(result.data)
            expires_at = datetime.now() + self.ttl

            CacheQueries.upsert(
                session=self.session,
                source_key=self.source_key,
                payload=payload,
                expires_at=expires_at,
                source_url=result.source_url,
            )
            logger.info(
                "Cached '%s': %d rows, TTL until %s",
                self.source_key, len(payload), expires_at,
            )
        else:
            stale = self._get_stale(self.source_key)
            if stale is not None:
                logger.warning(
                    "Fetch failed for '%s', returning stale cache", self.source_key
                )
                return FetcherResult(
                    data=stale,
                    last_updated=datetime.now(),
                    source_url=None,
                    status="stale",
                )

        return result

    def _get_stale(self, source_key: str) -> pd.DataFrame | None:
        """Получить протухший кеш (без проверки expires_at)"""
        from sqlalchemy import text
        query = text("""
            SELECT payload FROM fetch_cache
            WHERE source_key = :key
            LIMIT 1
        """)
        row = self.session.execute(query, {"key": source_key}).fetchone()
        if row:
            return self._payload_to_df(row._mapping["payload"])
        return None

    @staticmethod
    def _df_to_payload(data: pd.DataFrame | dict) -> list[dict]:
        if isinstance(data, pd.DataFrame):
            records = json.loads(
                data.to_json(orient="records", date_format="iso",
                             force_ascii=False)
            )
            return [{"__key__": "__single__", "__records__": records}]

        # dict[str, DataFrame] — сериализуем каждый
        result = []
        for key, df in data.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                records = json.loads(
                    df.to_json(orient="records", date_format="iso",
                               force_ascii=False)
                )
                result.append({"__key__": key, "__records__": records})
        return result

    @staticmethod
    def _payload_to_df(payload) -> pd.DataFrame | dict:
        if isinstance(payload, str):
            payload = json.loads(payload)

        if not payload:
            return pd.DataFrame()

        # Один DataFrame
        if len(payload) == 1 and payload[0].get("__key__") == "__single__":
            df = pd.DataFrame(payload[0]["__records__"])
            return CachedFetcher._restore_dates(df)

        # dict[str, DataFrame]
        result = {}
        for item in payload:
            key = item["__key__"]
            df = pd.DataFrame(item["__records__"])
            result[key] = CachedFetcher._restore_dates(df)
        return result

    @staticmethod
    def _restore_dates(df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            if "date" in col.lower() or col.endswith("_at"):
                df[col] = pd.to_datetime(df[col], errors="ignore")
        return df
