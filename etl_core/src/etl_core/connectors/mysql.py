import pymysql
import logging
from etl_core.connectors.base import BaseConnector
from etl_core.config.settings import DatabaseConfig


class MySQLConnector(BaseConnector):
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def connect(self):
        self.logger.info("Connecting to MySQL...")
        self.connection = pymysql.connect(
            host=self.config.host,
            user=self.config.user,
            password=self.config.password,
            database=self.config.database,
            port=self.config.port,
        )
        self.logger.info("MySQL connection established.")
        return self.connection

    def close(self):
        if self.connection:
            self.logger.info("Closing MySQL connection.")
            self.connection.close()
            self.connection = None
