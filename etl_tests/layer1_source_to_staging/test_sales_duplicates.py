import pytest

pytestmark = pytest.mark.layer1


def test_no_duplicate_sales_ids_in_staging(dwh_engine, read_sql):
    """
    staging_sales should not contain duplicate sales_id.
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

    duplicate_count = len(df)

    print("Duplicate sales_id rows:", duplicate_count)

    assert duplicate_count == 0, f"Found {duplicate_count} duplicate sales_id in staging_sales"
