import logging
from sqlalchemy import create_engine, text
from etl_core.config.settings import DatabaseConfig
from etl_core.staging.mysql_staging_loader import load_to_mysql_staging

logger = logging.getLogger(__name__)


def truncate_and_load(df, table_name: str, db_config: DatabaseConfig):
    logger.info(f"Truncating and loading staging table: {table_name}")

    url = (
        f"mysql+pymysql://{db_config.user}:{db_config.password}"
        f"@{db_config.host}:{db_config.port}/{db_config.database}"
    )

    engine = create_engine(url)

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))

    load_to_mysql_staging(df, table_name, db_config, if_exists="append")

    logger.info(f"Truncate and load completed for: {table_name}")
