import pytest

pytestmark = pytest.mark.layer1


def test_stores_schema_matches_expected(dwh_engine, read_sql):
    """
    Validate that staging_stores table has exactly the expected schema.
    """
    df = read_sql("SELECT * FROM staging_stores LIMIT 1", dwh_engine)

    actual_columns = set(df.columns)

    expected_columns = {
        "store_id",
        "store_name",
    }

    print("Actual columns:", actual_columns)

    assert actual_columns == expected_columns, (
        f"Schema mismatch. Expected={expected_columns}, Actual={actual_columns}"
    )
