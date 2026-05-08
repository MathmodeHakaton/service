"""
Integration тесты для RAG и чата
"""

from src.domain.llm.rag_retriever import RAGRetriever


def test_rag_retriever_initialization():
    """Тестировать инициализацию RAG retriever"""
    retriever = RAGRetriever()

    assert retriever is not None

    # Тестировать получение контекста
    contexts = retriever.retrieve_relevant_context("test query", limit=5)
    assert isinstance(contexts, list)
