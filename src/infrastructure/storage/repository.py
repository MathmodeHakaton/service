"""
Высокоуровневый интерфейс для работы с БД
"""

from sqlalchemy.orm import Session
from .db.engine import get_session
from .db.queries import lsi_queries, chat_queries, signal_queries, backtest_queries


class Repository:
    """Единая точка доступа к БД"""

    def __init__(self, session: Session = None):
        self.session = session or get_session()

    # LSI операции
    def get_latest_lsi(self):
        return lsi_queries.get_latest_lsi(self.session)

    def get_lsi_history(self, days: int = 30):
        return lsi_queries.get_lsi_history(self.session, days=days)

    # Chat операции
    def get_chat_history(self, session_id: str):
        return chat_queries.get_chat_history(self.session, session_id)

    def save_chat_message(self, session_id: str, role: str, content: str):
        chat_queries.save_chat_message(self.session, session_id, role, content)

    # Signal операции
    def get_signal_log(self, module_name: str, days: int = 7):
        return signal_queries.get_signal_log(self.session, module_name, days=days)

    def save_signal(self, module_name: str, signal_value: float, flag: str, details: str = ""):
        signal_queries.save_signal(
            self.session, module_name, signal_value, flag, details
        )

    # Backtest операции
    def get_backtest_results(self, limit: int = 10):
        return backtest_queries.get_backtest_results(self.session, limit=limit)

    def save_backtest_result(self, **kwargs):
        backtest_queries.save_backtest_result(self.session, **kwargs)

    def close(self):
        self.session.close()
