import logging
import pandas as pd
from etl_core.connectors.oracle import OracleConnector
from etl_core.config.settings import DatabaseConfig


logger = logging.getLogger(__name__)


def extract_from_oracle(query: str, db_config: DatabaseConfig) -> pd.DataFrame:
    logger.info("Extracting data from Oracle")

    connector = OracleConnector(db_config)
    conn = connector.connect()

    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Extracted {len(df)} records from Oracle")
        return df
    finally:
        connector.close()
