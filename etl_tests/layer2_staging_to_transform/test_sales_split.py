import pytest

pytestmark = pytest.mark.layer2


def test_high_sales_threshold(dwh_engine, read_sql):
    """
    All records in intermediate_high_sales must have total_sales >= 1000
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_high_sales
        WHERE total_sales < 1000
        """,
        dwh_engine
    )

    bad_count = df.iloc[0]["cnt"]

    assert bad_count == 0, (
        f"Found {bad_count} rows in intermediate_high_sales with total_sales < 1000"
    )


def test_low_sales_threshold(dwh_engine, read_sql):
    """
    All records in intermediate_low_sales must have total_sales < 1000
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_low_sales
        WHERE total_sales >= 1000
        """,
        dwh_engine
    )

    bad_count = df.iloc[0]["cnt"]

    assert bad_count == 0, (
        f"Found {bad_count} rows in intermediate_low_sales with total_sales >= 1000"
    )


def test_high_low_sales_reconciliation(dwh_engine, read_sql):
    """
    high_sales + low_sales must equal filtered_sales
    """

    high_df = read_sql(
        "SELECT COUNT(*) AS cnt FROM intermediate_high_sales",
        dwh_engine
    )
    low_df = read_sql(
        "SELECT COUNT(*) AS cnt FROM intermediate_low_sales",
        dwh_engine
    )
    filtered_df = read_sql(
        "SELECT COUNT(*) AS cnt FROM intermediate_filtered_sales",
        dwh_engine
    )

    high_count = high_df.iloc[0]["cnt"]
    low_count = low_df.iloc[0]["cnt"]
    filtered_count = filtered_df.iloc[0]["cnt"]

    assert high_count + low_count == filtered_count, (
        f"Mismatch: high({high_count}) + low({low_count}) != filtered({filtered_count})"
    )
