import pytest

# Mark this test as part of Layer-1 (Source -> Staging) test suite
pytestmark = pytest.mark.layer1


def test_supplier_null_checks(dwh_engine, read_sql):
    """
    Ensure that mandatory columns in staging_supplier do not contain NULL values.
    Mandatory columns:
      - supplier_id
      - supplier_name
    """

    # List of columns that must never be NULL
    mandatory_columns = ["supplier_id", "supplier_name"]

    for col in mandatory_columns:
        # Query to count NULLs in each mandatory column
        query = f"""
            SELECT COUNT(*) AS null_cnt
            FROM staging_supplier
            WHERE {col} IS NULL
        """

        df = read_sql(query, dwh_engine)
        null_cnt = int(df.iloc[0]["null_cnt"])

        # Print debug info if nulls are found
        if null_cnt > 0:
            print(f"Found {null_cnt} NULL values in column {col}")

        # Assert that no NULLs exist in mandatory columns
        assert null_cnt == 0, f"Column {col} contains {null_cnt} NULL values"
