import pytest

pytestmark = pytest.mark.layer2


def test_intermediate_filtered_sales_respects_date_filter(dwh_engine, read_sql):
    """
    intermediate_filtered_sales should NOT contain any rows
    with sale_date < 2024-01-01.
    """

    bad_rows_df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_filtered_sales
        WHERE sale_date < '2024-01-01'
        """,
        dwh_engine
    )

    bad_count = bad_rows_df.iloc[0]["cnt"]

    print("Rows violating date filter:", bad_count)

    assert bad_count == 0, (
        f"Found {bad_count} rows in intermediate_filtered_sales with sale_date < 2024-01-01"
    )
