import pytest

pytestmark = pytest.mark.layer1


def test_product_pk_uniqueness(dwh_engine, read_sql):
    """
    Ensure that product_id is unique in staging_product.
    This is the logical primary key.
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

    duplicate_count = len(dup_df)

    if duplicate_count > 0:
        print("Duplicate product_id values found:")
        print(dup_df)

    assert duplicate_count == 0, (
        f"Found {duplicate_count} duplicate product_id values in staging_product"
    )
