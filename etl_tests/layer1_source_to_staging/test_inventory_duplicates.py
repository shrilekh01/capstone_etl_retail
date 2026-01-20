import pytest

# Mark this test as part of Layer-1 (Source -> Staging) test suite
pytestmark = pytest.mark.layer1


def test_inventory_no_duplicates(dwh_engine, read_sql):
    """
    Ensure that there are no duplicate records in staging_inventory
    based on the logical primary key (product_id, store_id).
    """

    # Query to find duplicate logical keys
    dup_df = read_sql(
        """
        SELECT product_id, store_id, COUNT(*) AS cnt
        FROM staging_inventory
        GROUP BY product_id, store_id
        HAVING COUNT(*) > 1
        """,
        dwh_engine,
    )

    # Count number of duplicate key groups
    duplicate_groups = len(dup_df)

    # Print duplicates for debugging if any exist
    if duplicate_groups > 0:
        print("Duplicate inventory records found:")
        print(dup_df)

    # Assert that no duplicate records exist
    assert duplicate_groups == 0, (
        f"Found {duplicate_groups} duplicate (product_id, store_id) groups in staging_inventory"
    )
