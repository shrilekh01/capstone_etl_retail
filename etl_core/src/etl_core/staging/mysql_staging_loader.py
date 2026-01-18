import logging
from sqlalchemy import create_engine
from etl_core.config.settings import DatabaseConfig

logger = logging.getLogger(__name__)


def load_to_mysql_staging(df, table_name: str, db_config: DatabaseConfig, if_exists: str = "replace"):
    """
    Load DataFrame into MySQL staging table.

    if_exists:
        - "replace" = drop & recreate
        - "append" = append to existing
    """
    logger.info(f"Loading {len(df)} records into staging table: {table_name}")

    url = (
        f"mysql+pymysql://{db_config.user}:{db_config.password}"
        f"@{db_config.host}:{db_config.port}/{db_config.database}"
    )

    engine = create_engine(url)

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
    )

    logger.info(f"Loaded data into staging table: {table_name}")
