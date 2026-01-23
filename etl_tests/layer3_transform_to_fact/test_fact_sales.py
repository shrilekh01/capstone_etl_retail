import pytest

pytestmark = pytest.mark.layer3


def test_fact_sales_rowcount_matches_intermediate(dwh_engine, read_sql):
    """
    fact_sales must match intermediate_sales_with_details
    """

    fact_df = read_sql(
        "SELECT COUNT(*) AS cnt FROM fact_sales",
        dwh_engine
    )

    inter_df = read_sql(
        "SELECT COUNT(*) AS cnt FROM intermediate_sales_with_details",
        dwh_engine
    )

    assert fact_df.iloc[0]["cnt"] == inter_df.iloc[0]["cnt"], (
        "fact_sales row count does not match intermediate_sales_with_details"
    )


def test_fact_sales_no_nulls(dwh_engine, read_sql):
    """
    No NULLs allowed in critical fact columns
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM fact_sales
        WHERE sales_id IS NULL
           OR product_id IS NULL
           OR store_id IS NULL
           OR total_sales IS NULL
        """,
        dwh_engine
    )

    assert df.iloc[0]["cnt"] == 0, "NULL values found in fact_sales"
