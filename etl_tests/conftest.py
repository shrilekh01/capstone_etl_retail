import pytest
import pandas as pd
from sqlalchemy import create_engine

# 🔧 Database connections
SOURCE_DB_URL = "mysql+pymysql://root:sunbeam@localhost/retail_src"
DWH_DB_URL = "mysql+pymysql://root:sunbeam@localhost/retaildwh"


@pytest.fixture(scope="session")
def source_engine():
    engine = create_engine(SOURCE_DB_URL)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def dwh_engine():
    engine = create_engine(DWH_DB_URL)
    yield engine
    engine.dispose()


@pytest.fixture
def read_sql():
    def _read_sql(query, engine):
        if not isinstance(query, str):
            raise ValueError(f"Query must be str, got {type(query)}")
        return pd.read_sql(query, con=engine)
    return _read_sql