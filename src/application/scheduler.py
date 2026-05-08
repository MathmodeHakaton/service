"""
Scheduler: ежедневный пересчёт LSI с помощью APScheduler
"""

from apscheduler.schedulers.background import BackgroundScheduler
from src.application.pipeline import Pipeline
from src.infrastructure.storage.repository import Repository
import logging


logger = logging.getLogger(__name__)


class Scheduler:
    """Планировщик для автоматического запуска пайплайна"""

    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.pipeline = Pipeline()
        self.repository = Repository()

    def start(self):
        """Запустить планировщик"""

        # Добавить задачу на каждый день в 15:00 (после закрытия рынка)
        self.scheduler.add_job(
            self._run_pipeline,
            'cron',
            hour=15,
            minute=0,
            id='daily_lsi_calculation'
        )

        self.scheduler.start()
        logger.info("Scheduler started")

    def stop(self):
        """Остановить планировщик"""
        self.scheduler.shutdown()
        logger.info("Scheduler stopped")

    def _run_pipeline(self):
        """Выполнить пайплайн и сохранить результат"""
        try:
            result = self.pipeline.execute()
            logger.info(f"LSI calculated: {result.value:.2%}")

            # TODO: сохранить результат в БД
            # self.repository.save_lsi_snapshot(result)

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
