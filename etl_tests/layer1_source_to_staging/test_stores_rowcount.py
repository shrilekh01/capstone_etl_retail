import pytest

pytestmark = pytest.mark.layer1


def test_stores_rowcount_not_zero(dwh_engine, read_sql):
    """
    Staging stores table should not be empty after ETL run.
    """
    df = read_sql("SELECT COUNT(*) AS cnt FROM staging_stores", dwh_engine)
    cnt = int(df.iloc[0]["cnt"])

    print("staging_stores count:", cnt)

    assert cnt > 0, "staging_stores is empty"
