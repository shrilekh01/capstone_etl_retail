import pytest

pytestmark = pytest.mark.layer1


def test_staging_sales_pk_unique(dwh_engine, read_sql):
    """
    staging_sales.sales_id must be unique (no duplicates).
    """

    df = read_sql(
        """
        SELECT sales_id, COUNT(*) AS cnt
        FROM staging_sales
        GROUP BY sales_id
        HAVING COUNT(*) > 1
        """,
        dwh_engine,
    )

    print("Duplicate PK rows:", df)

    assert df.empty, f"Duplicate sales_id found in staging_sales:\n{df}"
