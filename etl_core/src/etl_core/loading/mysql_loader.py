import logging
from sqlalchemy import create_engine
from etl_core.config.settings import DatabaseConfig

logger = logging.getLogger(__name__)


def load_dataframe(df, table_name: str, db_config: DatabaseConfig, if_exists: str = "append"):
    """
    Load DataFrame into a MySQL final table.

    if_exists:
        - "append" = insert into existing table
        - "replace" = drop & recreate (use carefully)
    """
    logger.info(f"Loading {len(df)} records into final table: {table_name}")

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

    logger.info(f"Loaded data into final table: {table_name}")
