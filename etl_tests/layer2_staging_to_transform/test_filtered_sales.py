import pytest

pytestmark = pytest.mark.layer2


def test_intermediate_filtered_sales_respects_date_filter(dwh_engine, read_sql):
    """
    intermediate_filtered_sales should NOT contain any rows
    with sale_date < 2024-01-01.
    """

    bad_rows_df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_filtered_sales
        WHERE sale_date < '2024-01-01'
        """,
        dwh_engine
    )

    bad_count = bad_rows_df.iloc[0]["cnt"]

    print("Rows violating date filter:", bad_count)

    assert bad_count == 0, (
        f"Found {bad_count} rows in intermediate_filtered_sales with sale_date < 2024-01-01"
    )


def test_intermediate_filtered_sales_no_null_keys(dwh_engine, read_sql):
    """
    intermediate_filtered_sales should not contain NULLs in key columns.
    """

    nulls_df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_filtered_sales
        WHERE sales_id IS NULL
           OR product_id IS NULL
           OR store_id IS NULL
           OR sale_date IS NULL
        """,
        dwh_engine
    )

    null_count = nulls_df.iloc[0]["cnt"]

    print("Rows with NULLs in key columns:", null_count)

    assert null_count == 0, (
        f"Found {null_count} rows with NULLs in key columns in intermediate_filtered_sales"
    )


def test_intermediate_filtered_sales_rowcount(dwh_engine, read_sql):
    """
    intermediate_filtered_sales should contain exactly the rows
    from staging_sales after applying the date filter.
    """

    intermediate_count_df = read_sql(
        "SELECT COUNT(*) AS cnt FROM intermediate_filtered_sales",
        dwh_engine
    )
    intermediate_count = intermediate_count_df.iloc[0]["cnt"]

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
