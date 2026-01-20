import pytest

# Mark this test as part of Layer-1 (Source -> Staging) test suite
pytestmark = pytest.mark.layer1


def test_product_rowcount_not_zero(dwh_engine, read_sql):
    """
    Staging product table should not be empty after ETL run.
    """

    df = read_sql("SELECT COUNT(*) AS cnt FROM staging_product", dwh_engine)
    cnt = int(df.iloc[0]["cnt"])

    print("staging_product count:", cnt)

    assert cnt > 0, "staging_product is empty"
