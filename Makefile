.PHONY: help setup db-migrate db-up db-down db-status run-pipeline run-scheduler run-backtest run-dashboard docker-up docker-down docker-logs clean

help:
	@echo "RU Liquidity Sentinel - Команды управления"
	@echo ""
	@echo "📦 SETUP И ЗАВИСИМОСТИ"
	@echo "  make setup              - Установить зависимости (используется uv)"
	@echo ""
	@echo "🗄️  БД И МИГРАЦИИ"
	@echo "  make db-up              - Поднять PostgreSQL (docker)"
	@echo "  make db-migrate         - Применить все миграции"
	@echo "  make db-rollback        - Откатить последнюю миграцию"
	@echo "  make db-reset           - Откатить все миграции и пересоздать"
	@echo "  make db-status          - Показать статус миграций"
	@echo "  make db-down            - Остановить и удалить БД контейнер"
	@echo ""
	@echo "▶️  ЗАПУСК ПРИЛОЖЕНИЯ"
	@echo "  make run                - Запустить пайплайн один раз"
	@echo "  make run-schedule       - Запустить планировщик (ежедневно 15:00)"
	@echo "  make run-backtest       - Запустить backtesting"
	@echo "  make run-dashboard      - Запустить Streamlit дашборд (http://localhost:8501)"
	@echo ""
	@echo "🐳 DOCKER"
	@echo "  make docker-build       - Собрать Docker образы"
	@echo "  make docker-up          - Поднять все сервисы (PostgreSQL, Ollama, App, Streamlit)"
	@echo "  make docker-down        - Остановить все сервисы"
	@echo "  make docker-logs        - Показать логи контейнеров"
	@echo "  make docker-logs-app    - Логи приложения"
	@echo "  make docker-logs-db     - Логи БД"
	@echo ""
	@echo "🧹 ОЧИСТКА"
	@echo "  make clean              - Очистить кэш и временные файлы"
	@echo "  make clean-all          - Полная очистка (включая Docker volumes)"

# ═══════════════════════════════════════════════════════════════════
# SETUP
# ═══════════════════════════════════════════════════════════════════

setup:
	uv sync
	mkdir -p cache migrations scripts logs

# ═══════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════

db-up:
	docker-compose up -d postgres
	@echo "⏳ Ожидание запуска БД..."
	@sleep 5
	@echo "✅ PostgreSQL готова на localhost:5432"

db-down:
	docker-compose down postgres
	@echo "✅ PostgreSQL остановлена"

db-migrate:
	uv run python scripts/db_migrate.py up

db-rollback:
	uv run python scripts/db_migrate.py down

db-reset:
	uv run python scripts/db_migrate.py downall
	uv run python scripts/db_migrate.py up
	@echo "✅ БД пересоздана"

db-status:
	uv run python scripts/db_migrate.py status

# ═══════════════════════════════════════════════════════════════════
# RUN
# ═══════════════════════════════════════════════════════════════════

run:
	uv run python main.py run

run-schedule:
	uv run python main.py schedule --start

run-backtest:
	uv run python main.py backtest --start-year 2014 --end-year 2023

run-dashboard:
	uv run streamlit run src/presentation/app.py

# ═══════════════════════════════════════════════════════════════════
# DOCKER
# ═══════════════════════════════════════════════════════════════════

docker-build:
	docker-compose build

docker-up:
	docker-compose up -d
	@echo "⏳ Ожидание инициализации сервисов..."
	@sleep 10
	@echo "✅ Все сервисы запущены:"
	@echo "   - PostgreSQL: localhost:5432"
	@echo "   - Ollama: localhost:11434"
	@echo "   - Streamlit Dashboard: http://localhost:8501"
	@echo "   - Application scheduler работает в фоне"

docker-down:
	docker-compose down

docker-logs:
	docker-compose logs -f

docker-logs-app:
	docker-compose logs -f app

docker-logs-db:
	docker-compose logs -f postgres

docker-logs-streamlit:
	docker-compose logs -f streamlit

# ═══════════════════════════════════════════════════════════════════
# CLEAN
# ═══════════════════════════════════════════════════════════════════

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	rm -rf .coverage htmlcov/ dist/ build/ *.egg-info/
	@echo "✅ Очищены кэши"

clean-all: clean
	docker-compose down -v
	rm -rf cache/
	@echo "✅ Полная очистка завершена"

# ═══════════════════════════════════════════════════════════════════
# CONVENIENT WORKFLOWS
# ═══════════════════════════════════════════════════════════════════

# Первый запуск - всё с нуля
init: setup db-up db-migrate
	@echo "✅ Инициализация завершена"
	@echo "Следующий шаг: make run-dashboard"

# Локальная разработка - БД + дашборд
dev: db-up db-migrate run-dashboard

# Production с Docker
prod: docker-build docker-up
