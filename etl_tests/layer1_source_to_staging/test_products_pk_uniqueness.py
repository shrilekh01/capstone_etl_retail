import pytest

@pytest.mark.layer1
def test_staging_products_pk_unique(dwh_engine, read_sql):
    df = read_sql("""
        SELECT product_id, COUNT(*) cnt
        FROM staging_product
        GROUP BY product_id
        HAVING COUNT(*) > 1
    """, dwh_engine)

    assert df.empty, "Duplicate product_id found in staging_product"
