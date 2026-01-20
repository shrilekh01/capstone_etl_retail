import pytest

# Mark this test as part of Layer-1 (Source -> Staging) test suite
pytestmark = pytest.mark.layer1


def test_inventory_schema_matches_expected(dwh_engine, read_sql):
    """
    Validate that staging_inventory table has exactly the expected schema.
    """

    # Read one row from staging_inventory just to fetch column names
    df = read_sql("SELECT * FROM staging_inventory LIMIT 1", dwh_engine)

    # Get the set of actual column names from the dataframe
    actual_columns = set(df.columns)

    # Define the expected column names as per inventory data contract
    expected_columns = {
        "product_id",
        "store_id",
        "quantity_on_hand",
        "last_updated",
    }

    # Print columns for debugging purpose in case test fails
    print("Actual columns:", actual_columns)

    # Assert that actual schema exactly matches expected schema
    assert actual_columns == expected_columns, (
        f"Schema mismatch. Expected={expected_columns}, Actual={actual_columns}"
    )
