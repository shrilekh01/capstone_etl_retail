import pytest

pytestmark = pytest.mark.layer1


def test_product_no_duplicates(dwh_engine, read_sql):
    """
    Ensure that there are no duplicate records in staging_product
    based on the logical primary key (product_id).
    """
    dup_df = read_sql(
        """
        SELECT product_id, COUNT(*) AS cnt
        FROM staging_product
        GROUP BY product_id
        HAVING COUNT(*) > 1
        """,
        dwh_engine,
    )

    duplicate_groups = len(dup_df)

    if duplicate_groups > 0:
        print("Duplicate product records found:")
        print(dup_df)

    assert duplicate_groups == 0, (
        f"Found {duplicate_groups} duplicate product_id groups in staging_product"
    )
