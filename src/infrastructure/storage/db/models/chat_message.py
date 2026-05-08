"""
Модель для сообщений чата с LLM
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, DateTime, Text
from .base import DeclarativeBase


class ChatMessage(DeclarativeBase):
    """Сообщение в чате с аналитиком"""

    __tablename__ = "chat_messages"

    id = Column(Integer, primary_key=True, index=True)
    session_id = Column(String(50), index=True)
    role = Column(String(20))  # "user" или "assistant"
    content = Column(Text)
    timestamp = Column(DateTime, index=True, default=datetime.now)

    def __repr__(self):
        return f"<ChatMessage session={self.session_id} role={self.role}>"
