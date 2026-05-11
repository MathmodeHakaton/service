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
        # Ежедневный inference (быстрый: predict + SHAP, без fit).
        # 15:30 — после устаканивания данных ЦБ за день.
        self.scheduler.add_job(
            self._daily_inference,
            "cron",
            hour=15,
            minute=30,
            id="daily_lsi_inference",
            replace_existing=True,
        )
        # Еженедельное переобучение CatBoost — воскресенье 16:00,
        # когда нагрузка минимальна и есть запас на CV/fit.
        self.scheduler.add_job(
            self._weekly_retrain,
            "cron",
            day_of_week="sun",
            hour=16,
            minute=0,
            id="weekly_lsi_retrain",
            replace_existing=True,
        )
        self.scheduler.start()
        logger.info("Scheduler started: pipeline 15:00 daily, "
                    "inference 15:30 daily, retrain 16:00 Sun")

    def _daily_inference(self):
        from src.application.lsi_refresh import refresh_lsi
        try:
            rep = refresh_lsi(mode="inference")
            logger.info("Daily inference: ok=%s, mode=%s, copied=%d, err=%s",
                        rep.ok, rep.mode, rep.artifacts_copied, rep.error)
        except Exception as e:
            logger.error("Daily inference failed: %s", e, exc_info=True)

    def _weekly_retrain(self):
        from src.application.lsi_refresh import refresh_lsi
        try:
            rep = refresh_lsi(mode="retrain")
            logger.info("Weekly retrain: ok=%s, mode=%s, copied=%d, err=%s",
                        rep.ok, rep.mode, rep.artifacts_copied, rep.error)
        except Exception as e:
            logger.error("Weekly retrain failed: %s", e, exc_info=True)

    def stop(self):
        """Остановить планировщик"""
        self.scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")

    def _run_pipeline(self):
        """Выполнить пайплайн и сохранить снапшот в БД"""
        session = get_session()
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
