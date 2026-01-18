import pytest

pytestmark = pytest.mark.layer1


CRITICAL_COLUMNS = [
    "sales_id",
    "product_id",
    "store_id",
    "sale_date",
    "quantity",
]


@pytest.mark.parametrize("column", CRITICAL_COLUMNS)
def test_staging_sales_no_nulls_in_critical_columns(dwh_engine, read_sql, column):
    """
    Critical columns in staging_sales must not contain NULLs.
    """

    df = read_sql(
        f"""
        SELECT COUNT(*) AS cnt
        FROM staging_sales
        WHERE {column} IS NULL
        """,
        dwh_engine,
    )

    null_count = int(df.iloc[0]["cnt"])
    print(f"Null count in {column}:", null_count)

    assert null_count == 0, f"Found {null_count} NULLs in staging_sales.{column}"
