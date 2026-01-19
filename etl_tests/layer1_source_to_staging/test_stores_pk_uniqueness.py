import pytest

@pytest.mark.layer1
def test_staging_stores_pk_unique(dwh_engine, read_sql):
    df = read_sql("""
        SELECT store_id, COUNT(*) cnt
        FROM staging_stores
        GROUP BY store_id
        HAVING COUNT(*) > 1
    """, dwh_engine)

    assert df.empty, "Duplicate store_id found in staging_stores"
