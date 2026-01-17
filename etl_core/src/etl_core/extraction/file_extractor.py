import logging
from etl_core.connectors.file import FileConnector


logger = logging.getLogger(__name__)


def extract_file(path: str):
    logger.info(f"Extracting file: {path}")
    connector = FileConnector()
    df = connector.read(path)
    logger.info(f"Extracted {len(df)} records from {path}")
    return df
