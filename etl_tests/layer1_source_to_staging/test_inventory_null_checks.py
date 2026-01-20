import pytest

# Mark this test as part of Layer-1 (Source -> Staging) test suite
pytestmark = pytest.mark.layer1


def test_inventory_null_checks(dwh_engine, read_sql):
    """
    Ensure that mandatory columns in staging_inventory do not contain NULL values.
    Mandatory columns:
      - product_id
      - store_id
      - quantity_on_hand
    """

    # List of columns that must never be NULL
    mandatory_columns = ["product_id", "store_id", "quantity_on_hand"]

    for col in mandatory_columns:
        # Build query to count NULLs in each mandatory column
        query = f"""
            SELECT COUNT(*) AS null_cnt
            FROM staging_inventory
            WHERE {col} IS NULL
        """

        df = read_sql(query, dwh_engine)
        null_cnt = int(df.iloc[0]["null_cnt"])

        # Print debug info if nulls are found
        if null_cnt > 0:
            print(f"Found {null_cnt} NULL values in column {col}")

        # Assert that no NULLs exist in mandatory columns
        assert null_cnt == 0, f"Column {col} contains {null_cnt} NULL values"
