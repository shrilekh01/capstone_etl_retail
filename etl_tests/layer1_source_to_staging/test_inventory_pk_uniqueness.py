import pytest

# Mark this test as part of Layer-1 (Source -> Staging) test suite
pytestmark = pytest.mark.layer1


def test_inventory_pk_uniqueness(dwh_engine, read_sql):
    """
    Ensure that (product_id, store_id) combination is unique in staging_inventory.
    This represents the logical primary key for inventory snapshot.
    """

    # Query to find duplicate (product_id, store_id) combinations
    dup_df = read_sql(
        """
        SELECT product_id, store_id, COUNT(*) AS cnt
        FROM staging_inventory
        GROUP BY product_id, store_id
        HAVING COUNT(*) > 1
        """,
        dwh_engine,
    )

    # If any rows are returned, it means duplicates exist
    duplicate_count = len(dup_df)

    # Print duplicates for debugging if test fails
    if duplicate_count > 0:
        print("Duplicate inventory keys found:")
        print(dup_df)

    # Assert that no duplicate PK combinations exist
    assert duplicate_count == 0, (
        f"Found {duplicate_count} duplicate (product_id, store_id) keys in staging_inventory"
    )
