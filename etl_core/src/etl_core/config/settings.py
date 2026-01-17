from dataclasses import dataclass
import os


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
    mysql: DatabaseConfig


def load_config() -> AppConfig:
    env = os.getenv("APP_ENV", "dev")

    mysql = DatabaseConfig(
        host=os.getenv("MYSQL_HOST", "localhost"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", "root"),
        database=os.getenv("MYSQL_DB", "retail"),
    )

    return AppConfig(env=env, mysql=mysql)
