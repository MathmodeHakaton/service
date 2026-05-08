"""
Запросы для логов сигналов
"""

from sqlalchemy.orm import Session
from sqlalchemy import text


def get_signal_log(session: Session, module_name: str, days: int = 7) -> list:
    """Получить логи сигналов модуля"""
    results = session.execute(
        text("""
            SELECT module_name, signal_value, flag, details, timestamp
            FROM signal_logs
            WHERE module_name = :module_name
            AND timestamp >= NOW() - INTERVAL ':days days'
            ORDER BY timestamp DESC
        """),
        {"module_name": module_name, "days": days}
    ).fetchall()

    return results


def save_signal(
    session: Session,
    module_name: str,
    signal_value: float,
    flag: str,
    details: str = ""
) -> None:
    """Сохранить сигнал модуля"""
    session.execute(
        text("""
            INSERT INTO signal_logs (module_name, signal_value, flag, details, timestamp)
            VALUES (:module_name, :signal_value, :flag, :details, NOW())
        """),
        {
            "module_name": module_name,
            "signal_value": signal_value,
            "flag": flag,
            "details": details,
        }
    )
    session.commit()
