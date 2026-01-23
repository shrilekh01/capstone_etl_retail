import pytest

pytestmark = pytest.mark.layer3


def test_monthly_summary_matches_intermediate(dwh_engine, read_sql):
    """
    monthly_sales_summary must match intermediate_monthly_sales_summary_source
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM monthly_sales_summary
        """,
        dwh_engine
    )

    inter_df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_monthly_sales_summary_source
        """,
        dwh_engine
    )

    assert df.iloc[0]["cnt"] == inter_df.iloc[0]["cnt"], (
        "monthly_sales_summary row count mismatch"
    )


def test_monthly_sales_values(dwh_engine, read_sql):
    """
    Validate monthly totals match intermediate values
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM monthly_sales_summary m
        JOIN intermediate_monthly_sales_summary_source i
            ON m.product_id = i.product_id
           AND m.year = i.year
           AND m.month = i.month
        WHERE m.monthly_total_sales != i.monthly_total_sales
        """,
        dwh_engine
    )

    assert df.iloc[0]["cnt"] == 0, "Mismatch in monthly sales totals"
