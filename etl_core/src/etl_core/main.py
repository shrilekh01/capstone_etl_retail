from etl_core.config.settings import load_config
from etl_core.logging.logger import get_logger
from etl_core.orchestration.pipeline import run_daily_pipeline


def main():
    logger = get_logger("etl")

    config = load_config()

    logger.info("Starting ETL Platform")
    logger.info(f"Environment: {config.env}")
    logger.info(f"Source MySQL Host: {config.source_mysql.host}")
    logger.info(f"DWH MySQL Host: {config.dwh_mysql.host}")

    # 🚀 RUN FULL PIPELINE
    run_daily_pipeline()

    logger.info("ETL Platform run completed")


if __name__ == "__main__":
    main()
