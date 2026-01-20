import pytest

# Mark this test as part of Layer-1 (Source -> Staging) test suite
pytestmark = pytest.mark.layer1


def test_supplier_no_duplicates(dwh_engine, read_sql):
    """
    Ensure that there are no duplicate records in staging_supplier
    based on the logical primary key (supplier_id).
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

    duplicate_groups = len(dup_df)

    # Print duplicates for debugging if any exist
    if duplicate_groups > 0:
        print("Duplicate supplier records found:")
        print(dup_df)

    # Assert that no duplicate supplier records exist
    assert duplicate_groups == 0, (
        f"Found {duplicate_groups} duplicate supplier_id groups in staging_supplier"
    )
