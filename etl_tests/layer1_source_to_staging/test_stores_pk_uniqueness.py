import pytest

pytestmark = pytest.mark.layer1


def test_stores_pk_uniqueness(dwh_engine, read_sql):
    """
    Ensure that store_id is unique in staging_stores.
    This is the logical primary key.
    """
    dup_df = read_sql(
        """
        SELECT store_id, COUNT(*) AS cnt
        FROM staging_stores
        GROUP BY store_id
        HAVING COUNT(*) > 1
        """,
        dwh_engine,
    )

    duplicate_count = len(dup_df)

    if duplicate_count > 0:
        print("Duplicate store_id values found:")
        print(dup_df)

    assert duplicate_count == 0, (
        f"Found {duplicate_count} duplicate store_id values in staging_stores"
    )
