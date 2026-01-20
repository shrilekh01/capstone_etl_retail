import pytest

# Mark this test as part of Layer-1 (Source -> Staging) test suite
pytestmark = pytest.mark.layer1


def test_supplier_rowcount_not_zero(dwh_engine, read_sql):
    """
    Staging supplier table should not be empty after ETL run.
    """

    df = read_sql("SELECT COUNT(*) AS cnt FROM staging_supplier", dwh_engine)
    cnt = int(df.iloc[0]["cnt"])

    print("staging_supplier count:", cnt)

    assert cnt > 0, "staging_supplier is empty"
