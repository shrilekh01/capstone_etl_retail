import pytest

pytestmark = pytest.mark.layer1


def test_product_null_checks(dwh_engine, read_sql):
    """
    Ensure that mandatory columns in staging_product do not contain NULL values.
    Mandatory columns:
      - product_id
      - product_name
    """
    mandatory_columns = ["product_id", "product_name"]

    for col in mandatory_columns:
        query = f"""
            SELECT COUNT(*) AS null_cnt
            FROM staging_product
            WHERE {col} IS NULL
        """

        df = read_sql(query, dwh_engine)
        null_cnt = int(df.iloc[0]["null_cnt"])

        if null_cnt > 0:
            print(f"Found {null_cnt} NULL values in column {col}")

        assert null_cnt == 0, f"Column {col} contains {null_cnt} NULL values"
