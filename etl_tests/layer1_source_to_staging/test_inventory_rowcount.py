import pytest

pytestmark = pytest.mark.layer1


def test_inventory_rowcount_not_zero(dwh_engine, read_sql):
    """
    Staging inventory should not be empty after ETL run.
    """
    df = read_sql("SELECT COUNT(*) AS cnt FROM staging_inventory", dwh_engine)
    cnt = int(df.iloc[0]["cnt"])

    print("staging_inventory count:", cnt)

    assert cnt > 0, "staging_inventory is empty"
