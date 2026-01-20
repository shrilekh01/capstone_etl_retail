import pytest

pytestmark = pytest.mark.layer2


def test_intermediate_filtered_sales_rowcount(dwh_engine, read_sql):
    """
    intermediate_filtered_sales should contain exactly the rows
    from staging_sales after applying the date filter (>= 2024-01-01).
    """

    # Count in intermediate table
    intermediate_count_df = read_sql(
        "SELECT COUNT(*) AS cnt FROM intermediate_filtered_sales",
        dwh_engine
    )
    intermediate_count = intermediate_count_df.iloc[0]["cnt"]

    # Count in staging table after applying same filter
    staging_filtered_count_df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM staging_sales
        WHERE sale_date >= '2024-01-01'
        """,
        dwh_engine
    )
    staging_filtered_count = staging_filtered_count_df.iloc[0]["cnt"]

    print("Intermediate count:", intermediate_count)
    print("Expected (staging filtered) count:", staging_filtered_count)

    assert intermediate_count == staging_filtered_count, (
        f"Rowcount mismatch: intermediate_filtered_sales={intermediate_count}, "
        f"expected from staging_sales={staging_filtered_count}"
    )
