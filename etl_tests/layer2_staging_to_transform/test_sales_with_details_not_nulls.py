import pytest

pytestmark = pytest.mark.layer2


def test_sales_with_details_no_null_enrichment_columns(dwh_engine, read_sql):
    """
    intermediate_sales_with_details should not have NULLs in enrichment columns
    coming from product and store joins.
    """

    nulls_df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_sales_with_details
        WHERE product_name IS NULL
           OR category IS NULL
           OR price IS NULL
           OR store_name IS NULL
        """,
        dwh_engine
    )

    null_count = nulls_df.iloc[0]["cnt"]

    print("Rows with NULL enrichment columns:", null_count)

    assert null_count == 0, (
        f"Found {null_count} rows with NULLs in enrichment columns in intermediate_sales_with_details"
    )
