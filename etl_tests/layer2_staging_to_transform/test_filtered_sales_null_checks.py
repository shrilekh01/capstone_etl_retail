import pytest

pytestmark = pytest.mark.layer2


def test_intermediate_filtered_sales_no_null_keys(dwh_engine, read_sql):
    """
    intermediate_filtered_sales should not contain NULLs in key columns.
    """

    nulls_df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_filtered_sales
        WHERE sales_id IS NULL
           OR product_id IS NULL
           OR store_id IS NULL
           OR sale_date IS NULL
        """,
        dwh_engine
    )

    null_count = nulls_df.iloc[0]["cnt"]

    print("Rows with NULLs in key columns:", null_count)

    assert null_count == 0, (
        f"Found {null_count} rows with NULLs in key columns in intermediate_filtered_sales"
    )
