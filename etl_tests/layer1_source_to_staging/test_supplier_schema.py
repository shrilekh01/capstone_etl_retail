import pytest

# Mark this test as part of Layer-1 (Source -> Staging) test suite
pytestmark = pytest.mark.layer1


def test_supplier_schema_matches_expected(dwh_engine, read_sql):
    """
    Validate that staging_supplier table has exactly the expected schema.
    This protects against schema drift (missing/extra/renamed columns).
    """

    # Read one row just to get the column names
    df = read_sql("SELECT * FROM staging_supplier LIMIT 1", dwh_engine)

    # Actual columns present in the table
    actual_columns = set(df.columns)

    # Expected columns as per supplier data contract
    expected_columns = {
        "supplier_id",
        "supplier_name",
        "contact_name",
        "contact_email",
        "contact_phone",
        "country",
        "rating",
        "last_order_date",
    }

    # Print for debugging in case of failure
    print("Actual columns:", actual_columns)

    # Assert schema matches exactly
    assert actual_columns == expected_columns, (
        f"Schema mismatch. Expected={expected_columns}, Actual={actual_columns}"
    )
