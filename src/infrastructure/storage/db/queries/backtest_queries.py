"""
Запросы для результатов backtesting
"""

from sqlalchemy.orm import Session
from sqlalchemy import text


def get_backtest_results(session: Session, limit: int = 10) -> list:
    """Получить результаты backtesting"""
    results = session.execute(
        text("""
            SELECT start_date, end_date, sharpe_ratio, max_drawdown, total_return, win_rate
            FROM backtest_results
            ORDER BY created_at DESC
            LIMIT :limit
        """),
        {"limit": limit}
    ).fetchall()

    return results


def save_backtest_result(
    session: Session,
    start_date,
    end_date,
    sharpe_ratio: float,
    max_drawdown: float,
    total_return: float,
    win_rate: float,
    parameters: str = "",
    description: str = ""
) -> None:
    """Сохранить результат backtesting"""
    session.execute(
        text("""
            INSERT INTO backtest_results
            (start_date, end_date, sharpe_ratio, max_drawdown, total_return, win_rate, parameters, description, created_at)
            VALUES (:start_date, :end_date, :sharpe_ratio, :max_drawdown, :total_return, :win_rate, :parameters, :description, NOW())
        """),
        {
            "start_date": start_date,
            "end_date": end_date,
            "sharpe_ratio": sharpe_ratio,
            "max_drawdown": max_drawdown,
            "total_return": total_return,
            "win_rate": win_rate,
            "parameters": parameters,
            "description": description,
        }
    )
    session.commit()
