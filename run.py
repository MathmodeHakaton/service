#!/usr/bin/env python
"""
Запуск приложения
Варианты:
  python run.py              - Запустить пайплайн один раз
  python run.py --schedule   - Запустить планировщик
  python run.py --dashboard  - Запустить Streamlit дашборд
  python run.py --backtest   - Запустить backtesting
"""

from main import app
import sys
from pathlib import Path

# Добавляем root в path
sys.path.insert(0, str(Path(__file__).parent))


if __name__ == "__main__":
    app()
