import pytest

@pytest.mark.layer1
def test_products_column_count_source_vs_staging(source_engine, dwh_engine, read_sql):
    src_df = read_sql("SELECT * FROM products LIMIT 1", source_engine)
    stg_df = read_sql("SELECT * FROM staging_product LIMIT 1", dwh_engine)

    assert len(src_df.columns) == len(stg_df.columns), "Column count mismatch for products"
