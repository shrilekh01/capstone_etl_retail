import logging
import pandas as pd
from etl_core.connectors.mysql import MySQLConnector
from etl_core.config.settings import DatabaseConfig

logger = logging.getLogger(__name__)


def extract_from_mysql(query: str, db_config: DatabaseConfig) -> pd.DataFrame:
    """
    Extract data from MySQL.
    If a table name is passed, convert it to SELECT * FROM table.
    """
    logger.info("Extracting data from MySQL")

    connector = MySQLConnector(db_config)
    conn = connector.connect()

    try:
        # ✅ FIX: Handle table name vs SQL
        if not query.strip().lower().startswith("select"):
            query = f"SELECT * FROM {query}"

        df = pd.read_sql(query, conn)
        logger.info(f"Extracted {len(df)} records from MySQL")
        return df

    finally:
        connector.close()
