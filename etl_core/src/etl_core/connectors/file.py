import pandas as pd
import logging
from pathlib import Path


class FileConnector:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def read(self, path: str) -> pd.DataFrame:
        file_path = Path(path)

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        self.logger.info(f"Reading file: {path}")

        if path.endswith(".csv"):
            return pd.read_csv(path)
        elif path.endswith(".json"):
            return pd.read_json(path)
        elif path.endswith(".xml"):
            return pd.read_xml(path)
        else:
            raise ValueError(f"Unsupported file type: {path}")
