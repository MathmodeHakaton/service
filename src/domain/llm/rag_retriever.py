"""
RAG Retriever для поиска по истории LSI
"""

from typing import List
from src.infrastructure.storage.repository import Repository


class RAGRetriever:
    """Retriever для RAG (Retrieval Augmented Generation)"""

    def __init__(self, repository: Repository = None):
        self.repository = repository or Repository()

    def retrieve_relevant_context(self, query: str, limit: int = 5) -> List[str]:
        """
        Получить релевантный контекст из истории LSI

        Args:
            query: запрос пользователя
            limit: кол-во результатов

        Returns:
            список релевантных фрагментов текста
        """
        try:
            history = self.repository.get_lsi_history(days=30)

            # TODO: реализовать семантический поиск (например, с помощью embeddings)
            # На данный момент просто возвращаем последние N записей

            contexts = []
            for record in history[:limit]:
                context = f"LSI на {record[0]}: {record[1]:.2%} (статус: {record[2]})"
                contexts.append(context)

            return contexts
        except Exception:
            return []
