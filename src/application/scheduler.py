"""
Scheduler: ежедневный пересчёт LSI с помощью APScheduler
"""

import logging
from datetime import date

from apscheduler.schedulers.background import BackgroundScheduler

from src.infrastructure.storage.db.engine import get_session
from src.infrastructure.storage.db.queries.lsi_queries import LSIQueries
from src.application.pipeline import Pipeline

logger = logging.getLogger(__name__)


class Scheduler:
    """Планировщик для автоматического запуска пайплайна"""

    def __init__(self):
        self.scheduler = BackgroundScheduler()

    def start(self):
        """Запустить планировщик"""
        self.scheduler.add_job(
            self._run_pipeline,
            "cron",
            hour=15,
            minute=0,
            id="daily_lsi_calculation",
            replace_existing=True,
        )
        self.scheduler.start()
        logger.info("Scheduler started, next run at 15:00")

    def stop(self):
        """Остановить планировщик"""
        self.scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")

    def _run_pipeline(self):
        """Выполнить пайплайн и сохранить снапшот в БД"""
        session = next(get_session())
        try:
            result = Pipeline(session=session).execute()
            logger.info(
                "LSI calculated: %.2f [%s]", result.value, result.status)

            LSIQueries.insert_snapshot(
                session=session,
                date_=date.today(),
                result=result,
            )
            logger.info("LSI snapshot saved for %s", date.today())

        except Exception as e:
            logger.error("Pipeline execution failed: %s", e, exc_info=True)
        finally:
            session.close()
