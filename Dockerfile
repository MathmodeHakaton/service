FROM python:3.11-slim

WORKDIR /app

# Установка зависимостей
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости
COPY pyproject.toml ./
COPY uv.lock ./

# Устанавливаем uv (если используется) или pip
RUN pip install --upgrade pip && \
    pip install -e .

# Копируем код приложения
COPY . .

# Default command - выполнить миграции и запустить пайплайн
CMD ["python", "main.py", "run"]
