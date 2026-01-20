import pytest

# Mark this test as part of Layer-1 (Source -> Staging) test suite
pytestmark = pytest.mark.layer1


def test_supplier_pk_uniqueness(dwh_engine, read_sql):
    """
    Ensure that supplier_id is unique in staging_supplier.
    This is the logical primary key for supplier.
    """

    # Query to find duplicate supplier_id values
    dup_df = read_sql(
        """
        SELECT supplier_id, COUNT(*) AS cnt
        FROM staging_supplier
        GROUP BY supplier_id
        HAVING COUNT(*) > 1
        """,
        dwh_engine,
    )

    duplicate_count = len(dup_df)

    # Print duplicates for debugging if any exist
    if duplicate_count > 0:
        print("Duplicate supplier_id values found:")
        print(dup_df)

    # Assert no duplicate supplier_id exists
    assert duplicate_count == 0, (
        f"Found {duplicate_count} duplicate supplier_id values in staging_supplier"
    )
