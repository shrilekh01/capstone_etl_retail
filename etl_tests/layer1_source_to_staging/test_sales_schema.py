import pytest

pytestmark = pytest.mark.layer1


def test_sales_column_count_source_vs_staging(source_engine, dwh_engine, read_sql):
    """
    Source sales and staging_sales must have same number of columns.
    """

    src_df = read_sql("SELECT * FROM sales LIMIT 1", source_engine)
    stg_df = read_sql("SELECT * FROM staging_sales LIMIT 1", dwh_engine)

    src_cols = set(src_df.columns)
    stg_cols = set(stg_df.columns)

    print("Source columns:", src_cols)
    print("Staging columns:", stg_cols)

    assert src_cols == stg_cols, f"Schema mismatch. Source={src_cols}, Staging={stg_cols}"
