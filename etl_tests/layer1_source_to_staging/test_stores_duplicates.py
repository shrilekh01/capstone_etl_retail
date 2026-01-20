import pytest

pytestmark = pytest.mark.layer1


def test_stores_no_duplicates(dwh_engine, read_sql):
    """
    Ensure that there are no duplicate records in staging_stores
    based on the logical primary key (store_id).
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

    duplicate_groups = len(dup_df)

    if duplicate_groups > 0:
        print("Duplicate store records found:")
        print(dup_df)

    assert duplicate_groups == 0, (
        f"Found {duplicate_groups} duplicate store_id groups in staging_stores"
    )
