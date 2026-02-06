import os
import pytest
import pandas as pd
from sqlalchemy import create_engine

# Detect CI environment
IS_CI = os.getenv("CI") == "true"

SOURCE_DB_URL = "mysql+pymysql://root:sunbeam@localhost/retail_src"
DWH_DB_URL = "mysql+pymysql://root:sunbeam@localhost/retaildwh"


def pytest_collection_modifyitems(items):
    for item in items:
        if "etl_tests" in item.nodeid:
            if any(layer in item.nodeid for layer in [
                "layer1_",
                "layer2_",
                "layer3_",
                "layer4_",
                "layer5_"
            ]):
                item.add_marker(pytest.mark.db)
                
@pytest.fixture(scope="session")
def source_engine():
    if IS_CI:
        pytest.skip("Skipping DB connection in CI")

    engine = create_engine(SOURCE_DB_URL)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def dwh_engine():
    if IS_CI:
        pytest.skip("Skipping DB connection in CI")

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
