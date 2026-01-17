from etl_core.config.settings import load_config
from etl_core.logging.logger import setup_logging
import logging


def main():
    setup_logging()
    logger = logging.getLogger("etl")

    config = load_config()

    logger.info("Starting ETL Platform")
    logger.info(f"Environment: {config.env}")
    logger.info(f"MySQL Host: {config.mysql.host}")

    print("ETL Platform Infrastructure OK")


if __name__ == "__main__":
    main()
