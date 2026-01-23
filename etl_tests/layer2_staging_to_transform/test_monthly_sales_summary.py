import pytest

pytestmark = pytest.mark.layer2


def test_monthly_sales_summary_no_nulls(dwh_engine, read_sql):
    """
    monthly_sales_summary should not contain NULLs
    in key aggregation columns.
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_monthly_sales_summary_source
        WHERE product_id IS NULL
           OR year IS NULL
           OR month IS NULL
           OR monthly_total_sales IS NULL
        """,
        dwh_engine
    )

    null_count = df.iloc[0]["cnt"]

    assert null_count == 0, (
        f"Found {null_count} NULL values in monthly_sales_summary"
    )


def test_monthly_sales_aggregation_correctness(dwh_engine, read_sql):
    """
    Validate that monthly aggregation matches
    sum(total_sales) from intermediate_filtered_sales.
    """

    df = read_sql(
        """
        SELECT 
            m.product_id,
            m.year,
            m.month,
            m.monthly_total_sales,
            SUM(f.total_sales) AS expected_total
        FROM intermediate_monthly_sales_summary_source m
        JOIN intermediate_filtered_sales f
            ON m.product_id = f.product_id
           AND YEAR(f.sale_date) = m.year
           AND MONTH(f.sale_date) = m.month
        GROUP BY m.product_id, m.year, m.month, m.monthly_total_sales
        HAVING m.monthly_total_sales != SUM(f.total_sales)
        """,
        dwh_engine
    )

    mismatch_count = len(df)

    assert mismatch_count == 0, (
        f"Found {mismatch_count} mismatched monthly aggregations"
    )


def test_monthly_sales_no_negative_values(dwh_engine, read_sql):
    """
    Monthly sales should never be negative.
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_monthly_sales_summary_source
        WHERE monthly_total_sales < 0
        """,
        dwh_engine
    )

    bad_count = df.iloc[0]["cnt"]

    assert bad_count == 0, (
        f"Found {bad_count} records with negative monthly sales"
    )
