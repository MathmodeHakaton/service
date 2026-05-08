"""
Raw SQL запросы для LSI
"""

from sqlalchemy.orm import Session
from sqlalchemy import text


def get_latest_lsi(session: Session) -> dict:
    """Получить последний LSI snapshot"""
    result = session.execute(
        text("""
            SELECT value, m1_reserves, m2_repo, m3_ofz, m4_tax, m5_treasury, status
            FROM lsi_snapshots
            ORDER BY date DESC
            LIMIT 1
        """)
    ).first()

    return result if result else None


def get_lsi_history(session: Session, days: int = 30) -> list:
    """Получить историю LSI за N дней"""
    results = session.execute(
        text("""
            SELECT date, value, status
            FROM lsi_snapshots
            WHERE date >= NOW() - INTERVAL ':days days'
            ORDER BY date ASC
        """),
        {"days": days}
    ).fetchall()

    return results
