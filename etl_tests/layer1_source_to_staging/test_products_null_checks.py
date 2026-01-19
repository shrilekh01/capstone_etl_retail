import pytest

CRITICAL_COLUMNS = ["product_id"]

@pytest.mark.layer1
@pytest.mark.parametrize("column", CRITICAL_COLUMNS)
def test_staging_products_no_nulls(dwh_engine, read_sql, column):
    df = read_sql(f"""
        SELECT COUNT(*) AS cnt
        FROM staging_product
        WHERE {column} IS NULL
    """, dwh_engine)

    assert df.iloc[0]["cnt"] == 0, f"NULLs found in staging_product.{column}"
