import pytest

@pytest.mark.layer1
def test_stores_rowcount_source_vs_staging(source_engine, dwh_engine, read_sql):
    src_df = read_sql("SELECT COUNT(*) AS cnt FROM stores", source_engine)
    stg_df = read_sql("SELECT COUNT(*) AS cnt FROM staging_stores", dwh_engine)

    assert src_df.iloc[0]["cnt"] == stg_df.iloc[0]["cnt"], "Row count mismatch for stores"
