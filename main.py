"""
RU Liquidity Sentinel - Entry point
"""

import typer
from src.application.pipeline import Pipeline
from src.application.scheduler import Scheduler
from src.application.backtest import BacktestRunner

app = typer.Typer(
    name="sentinel",
    help="Система мониторинга ликвидности рубля",
)


@app.command()
def pipeline(
    run: bool = typer.Option(True, help="Выполнить пайплайн один раз"),
):
    """Выполнить пайплайн анализа"""
    if run:
        pipeline = Pipeline()
        result = pipeline.execute()
        typer.echo(f"LSI: {result.value}")


@app.command()
def scheduler(
    start: bool = typer.Option(False, help="Запустить планировщик"),
    stop: bool = typer.Option(False, help="Остановить планировщик"),
):
    """Управление планировщиком"""
    if start:
        scheduler = Scheduler()
        scheduler.start()
    elif stop:
        scheduler = Scheduler()
        scheduler.stop()


@app.command()
def backtest(
    start_year: int = typer.Option(2014, help="Начальный год"),
    end_year: int = typer.Option(2023, help="Конечный год"),
):
    """Запустить backtesting"""
    runner = BacktestRunner(start_year=start_year, end_year=end_year)
    results = runner.run()
    typer.echo(f"Backtesting завершен: {len(results)} результатов")


if __name__ == "__main__":
    app()
