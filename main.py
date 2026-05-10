"""
RU Liquidity Sentinel - Entry point
"""

import typer
import subprocess
import sys
from pathlib import Path

from src.infrastructure.storage.db.engine import get_session
from src.application.pipeline import Pipeline
from src.application.scheduler import Scheduler
from src.application.backtest import BacktestRunner

app = typer.Typer(
    name="sentinel",
    help="Система мониторинга ликвидности рубля",
)


@app.command()
def run(
    force_refresh: bool = typer.Option(
        False, "--force", "-f", help="Принудительно обновить кеш"),
):
    """Выполнить пайплайн анализа один раз"""
    session = next(get_session())
    try:
        result = Pipeline(
            session=session, force_refresh=force_refresh).execute()
        typer.echo(f"LSI: {result.value:.2f} [{result.status}]")
    finally:
        session.close()


@app.command()
def schedule(
    start: bool = typer.Option(False, "--start", help="Запустить планировщик"),
    stop:  bool = typer.Option(
        False, "--stop",  help="Остановить планировщик"),
):
    """Управление планировщиком"""
    if start:
        Scheduler().start()
        typer.echo("Планировщик запущен. Ctrl+C для остановки.")
        try:
            import time
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            typer.echo("Остановка...")
    elif stop:
        typer.echo(
            "Планировщик работает в фоне — остановите процесс через Ctrl+C или kill.")
    else:
        typer.echo("Укажите --start или --stop")


@app.command()
def backtest(
    start_year: int = typer.Option(2014, help="Начальный год"),
    end_year:   int = typer.Option(2023, help="Конечный год"),
):
    """Запустить backtesting на исторических эпизодах"""
    session = next(get_session())
    try:
        runner = BacktestRunner(
            start_year=start_year,
            end_year=end_year,
            session=session,
        )
        results = runner.run()
        typer.echo(f"Backtesting завершён: {len(results)} результатов")
    finally:
        session.close()


@app.command()
def dashboard():
    """Запустить Streamlit дашборд"""
    subprocess.run(["streamlit", "run", "src/presentation/app.py"], check=True)


@app.command()
def migrate(
    command: str = typer.Argument("up", help="up/down/downall/status"),
):
    """Управление миграциями БД"""
    script_path = Path(__file__).parent / "scripts" / "db_migrate.py"
    result = subprocess.run(
        ["python", str(script_path), command],
        cwd=Path(__file__).parent,
    )
    sys.exit(result.returncode)


@app.command()
def health():
    """Проверка здоровья приложения (health check)"""
    script_path = Path(__file__).parent / "scripts" / "health_check.py"
    result = subprocess.run(
        ["python", str(script_path)],
        cwd=Path(__file__).parent,
    )
    sys.exit(result.returncode)


if __name__ == "__main__":
    app()
