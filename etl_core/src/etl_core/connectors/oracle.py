import logging
from etl_core.connectors.base import BaseConnector
from etl_core.config.settings import DatabaseConfig

try:
    import oracledb
except ImportError:
    oracledb = None


class OracleConnector(BaseConnector):
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def connect(self):
        if oracledb is None:
            raise RuntimeError("oracledb library is not installed")

        self.logger.info("Connecting to Oracle...")

        dsn = f"{self.config.host}:{self.config.port}/{self.config.database}"

        self.connection = oracledb.connect(
            user=self.config.user,
            password=self.config.password,
            dsn=dsn,
        )

        self.logger.info("Oracle connection established.")
        return self.connection

    def close(self):
        if self.connection:
            self.logger.info("Closing Oracle connection.")
            self.connection.close()
            self.connection = None
