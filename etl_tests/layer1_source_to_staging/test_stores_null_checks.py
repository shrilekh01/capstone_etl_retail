import pytest

pytestmark = pytest.mark.layer1


def test_stores_null_checks(dwh_engine, read_sql):
    """
    Ensure that mandatory columns in staging_stores do not contain NULL values.
    Mandatory columns:
      - store_id
      - store_name
    """
    mandatory_columns = ["store_id", "store_name"]

    for col in mandatory_columns:
        query = f"""
            SELECT COUNT(*) AS null_cnt
            FROM staging_stores
            WHERE {col} IS NULL
        """

        df = read_sql(query, dwh_engine)
        null_cnt = int(df.iloc[0]["null_cnt"])

        if null_cnt > 0:
            print(f"Found {null_cnt} NULL values in column {col}")

        assert null_cnt == 0, f"Column {col} contains {null_cnt} NULL values"
