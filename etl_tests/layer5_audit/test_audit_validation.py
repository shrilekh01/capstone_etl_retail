import pytest
from etl_core.orchestration.pipeline import run_daily_pipeline
pytestmark = pytest.mark.integration
# -------------------------------------------------------
# Run ETL once before audit tests
# -------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def run_etl_before_audit_tests():
    run_daily_pipeline()


# -------------------------------------------------------
# Tests
# -------------------------------------------------------

def test_latest_etl_run_success(dwh_engine, read_sql):
    df = read_sql(
        """
        SELECT status
        FROM retaildwh.etl_run_audit
        ORDER BY run_id DESC
        LIMIT 1
        """,
        dwh_engine
    )

    assert df.iloc[0]["status"] == "SUCCESS"


def test_table_load_audit_entries_exist(dwh_engine, read_sql):
    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM retaildwh.table_load_audit
        """,
        dwh_engine
    )

    assert df.iloc[0]["cnt"] > 0


def test_each_table_logged(dwh_engine, read_sql):
    df = read_sql(
        """
        SELECT DISTINCT table_name
        FROM retaildwh.table_load_audit
        """,
        dwh_engine
    )

    expected = {
        "fact_sales",
        "inventory_levels_by_store",
        "monthly_sales_summary",
    }

    assert expected.issubset(set(df["table_name"]))


def test_table_rows_logged(dwh_engine, read_sql):
    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM retaildwh.table_load_audit
        WHERE rows_loaded < 0
        """,
        dwh_engine
    )

    assert df.iloc[0]["cnt"] == 0


def test_audit_run_to_table_mapping(dwh_engine, read_sql):
    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM retaildwh.table_load_audit t
        LEFT JOIN retaildwh.etl_run_audit r
            ON t.run_id = r.run_id
        WHERE r.run_id IS NULL
        """,
        dwh_engine
    )

    assert df.iloc[0]["cnt"] == 0
