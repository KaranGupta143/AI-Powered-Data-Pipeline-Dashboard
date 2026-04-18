import time
import logging
import schedule

from pipeline import run_pipeline
from config import SCHEDULE_HOURS
from logging_config import setup_logging


logger = logging.getLogger(__name__)


def run_scheduled_pipeline() -> None:
    success = run_pipeline()
    if success:
        logger.info("Scheduled run completed successfully.")
    else:
        logger.error("Scheduled run completed with errors.")


if __name__ == "__main__":
    setup_logging()

    run_scheduled_pipeline()
    schedule.every(SCHEDULE_HOURS).hours.do(run_scheduled_pipeline)

    logger.info(
        "Scheduler running every %s hour(s). Press Ctrl+C to stop.",
        SCHEDULE_HOURS,
    )
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user.")
