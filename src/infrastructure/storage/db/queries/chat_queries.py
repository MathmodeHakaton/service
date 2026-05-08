"""
Запросы для чата с LLM
"""

from sqlalchemy.orm import Session
from sqlalchemy import text


def get_chat_history(session: Session, session_id: str) -> list:
    """Получить историю чата"""
    results = session.execute(
        text("""
            SELECT role, content, timestamp
            FROM chat_messages
            WHERE session_id = :session_id
            ORDER BY timestamp ASC
        """),
        {"session_id": session_id}
    ).fetchall()

    return results


def save_chat_message(session: Session, session_id: str, role: str, content: str) -> None:
    """Сохранить сообщение в чат"""
    session.execute(
        text("""
            INSERT INTO chat_messages (session_id, role, content, timestamp)
            VALUES (:session_id, :role, :content, NOW())
        """),
        {"session_id": session_id, "role": role, "content": content}
    )
    session.commit()
