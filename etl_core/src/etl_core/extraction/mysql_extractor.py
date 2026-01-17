import logging
import pandas as pd
from etl_core.connectors.mysql import MySQLConnector
from etl_core.config.settings import DatabaseConfig


logger = logging.getLogger(__name__)


def extract_from_mysql(query: str, db_config: DatabaseConfig) -> pd.DataFrame:
    logger.info("Extracting data from MySQL")

    connector = MySQLConnector(db_config)
    conn = connector.connect()

    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Extracted {len(df)} records from MySQL")
        return df
    finally:
        connector.close()
