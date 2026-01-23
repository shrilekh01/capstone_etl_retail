import pytest

pytestmark = pytest.mark.audit


def test_etl_run_audit_exists(dwh_engine, read_sql):
    """
    Ensure at least one ETL run exists.
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM retaildwh.etl_run_audit
        """,
        dwh_engine
    )

    assert df.iloc[0]["cnt"] > 0, "No ETL run records found"


def test_latest_etl_run_success(dwh_engine, read_sql):
    """
    Latest ETL run must be SUCCESS.
    """

    df = read_sql(
        """
        SELECT status
        FROM retaildwh.etl_run_audit
        ORDER BY run_id DESC
        LIMIT 1
        """,
        dwh_engine
    )

    assert df.iloc[0]["status"] == "SUCCESS", "Latest ETL run did not succeed"


def test_table_load_audit_entries_exist(dwh_engine, read_sql):
    """
    Ensure table-level audit entries exist.
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM retaildwh.table_load_audit
        """
    )

    assert df.iloc[0]["cnt"] > 0, "No table load audit records found"


def test_each_table_logged(dwh_engine, read_sql):
    """
    Validate expected tables are logged in audit.
    """

    df = read_sql(
        """
        SELECT DISTINCT table_name
        FROM retaildwh.table_load_audit
        """
    )

    expected_tables = {
        "fact_sales",
        "inventory_levels_by_store",
        "monthly_sales_summary"
    }

    logged_tables = set(df["table_name"].tolist())

    missing = expected_tables - logged_tables

    assert not missing, f"Missing audit entries for tables: {missing}"


def test_table_rows_logged(dwh_engine, read_sql):
    """
    Rows loaded must be greater than zero.
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM retaildwh.table_load_audit
        WHERE rows_loaded <= 0
        """
    )

    assert df.iloc[0]["cnt"] == 0, "Some tables have zero rows loaded"


def test_audit_run_to_table_mapping(dwh_engine, read_sql):
    """
    Every table audit must map to a valid ETL run.
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM retaildwh.table_load_audit t
        LEFT JOIN retaildwh.etl_run_audit r
            ON t.run_id = r.run_id
        WHERE r.run_id IS NULL
        """
    )

    assert df.iloc[0]["cnt"] == 0, "Orphan audit records found"
