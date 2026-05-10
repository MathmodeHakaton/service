# infrastructure/storage/db/queries/cache_queries.py
import json
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.orm import Session


class CacheQueries:

    @staticmethod
    def get_if_fresh(session: Session, source_key: str) -> dict | None:
        """Вернуть кеш если он ещё не истёк"""
        query = text("""
            SELECT source_key, fetched_at, expires_at, status, row_count, payload
            FROM fetch_cache
            WHERE source_key = :key
              AND expires_at > NOW()
              AND status = 'fresh'
            LIMIT 1
        """)
        row = session.execute(query, {"key": source_key}).fetchone()
        return dict(row._mapping) if row else None

    @staticmethod
    def upsert(
        session: Session,
        source_key: str,
        payload: list[dict],
        expires_at: datetime,
        source_url: str = None,
        status: str = "fresh",
    ) -> None:
        """Сохранить или обновить кеш для источника"""
        query = text("""
            INSERT INTO fetch_cache
                (source_key, fetched_at, expires_at, status, row_count, source_url, payload)
            VALUES
                (:key, NOW(), :expires_at, :status, :row_count, :source_url, :payload::jsonb)
            ON CONFLICT (source_key) DO UPDATE SET
                fetched_at  = NOW(),
                expires_at  = :expires_at,
                status      = :status,
                row_count   = :row_count,
                source_url  = :source_url,
                payload     = :payload::jsonb
        """)
        session.execute(query, {
            "key": source_key,
            "expires_at": expires_at,
            "status": status,
            "row_count": len(payload),
            "source_url": source_url,
            "payload": json.dumps(payload, ensure_ascii=False, default=str),
        })
        session.commit()

    @staticmethod
    def get_meta(session: Session, source_key: str) -> dict | None:
        """Метаданные кеша без payload (для логов/дашборда)"""
        query = text("""
            SELECT source_key, fetched_at, expires_at, status, row_count
            FROM fetch_cache
            WHERE source_key = :key
            LIMIT 1
        """)
        row = session.execute(query, {"key": source_key}).fetchone()
        return dict(row._mapping) if row else None

    @staticmethod
    def invalidate(session: Session, source_key: str) -> None:
        """Принудительно инвалидировать кеш (expires_at = NOW())"""
        query = text("""
            UPDATE fetch_cache
            SET expires_at = NOW()
            WHERE source_key = :key
        """)
        session.execute(query, {"key": source_key})
        session.commit()
