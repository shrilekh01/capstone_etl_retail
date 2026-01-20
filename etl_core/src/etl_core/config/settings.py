import os
from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass
class AppConfig:
    env: str
    source_mysql: DatabaseConfig
    dwh_mysql: DatabaseConfig


def load_config() -> AppConfig:
    env = os.getenv("APP_ENV", "dev")

    source_mysql = DatabaseConfig(
        host=os.getenv("SRC_MYSQL_HOST", "localhost"),
        port=int(os.getenv("SRC_MYSQL_PORT", "3306")),
        user=os.getenv("SRC_MYSQL_USER", "root"),
        password=os.getenv("SRC_MYSQL_PASSWORD", "sunbeam"),
        database=os.getenv("SRC_MYSQL_DB", "retail_src"),
    )

    dwh_mysql = DatabaseConfig(
        host=os.getenv("DWH_MYSQL_HOST", "localhost"),
        port=int(os.getenv("DWH_MYSQL_PORT", "3306")),
        user=os.getenv("DWH_MYSQL_USER", "root"),
        password=os.getenv("DWH_MYSQL_PASSWORD", "sunbeam"),
        database=os.getenv("DWH_MYSQL_DB", "retaildwh"),
    )

    return AppConfig(env=env, source_mysql=source_mysql, dwh_mysql=dwh_mysql)
