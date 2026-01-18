import pytest

pytestmark = pytest.mark.layer1


def test_sales_rowcount_source_vs_staging(source_engine, dwh_engine, read_sql):
    """
    Source sales count must match staging_sales count.
    """

    src_df = read_sql("SELECT COUNT(*) AS cnt FROM sales", source_engine)
    stg_df = read_sql("SELECT COUNT(*) AS cnt FROM staging_sales", dwh_engine)

    src_count = int(src_df.iloc[0]["cnt"])
    stg_count = int(stg_df.iloc[0]["cnt"])

    print("Source count:", src_count)
    print("Staging count:", stg_count)

    assert src_count == stg_count, f"Mismatch: source={src_count}, staging={stg_count}"
