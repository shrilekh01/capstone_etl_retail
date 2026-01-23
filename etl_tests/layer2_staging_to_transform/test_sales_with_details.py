import pytest

pytestmark = pytest.mark.layer2


def test_sales_with_details_no_null_enrichment_columns(dwh_engine, read_sql):
    """
    intermediate_sales_with_details should not have NULLs
    in enrichment columns coming from joins.
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_sales_with_details
        WHERE product_name IS NULL
           OR category IS NULL
           OR price IS NULL
           OR store_name IS NULL
        """,
        dwh_engine
    )

    null_count = df.iloc[0]["cnt"]

    assert null_count == 0, (
        f"Found {null_count} rows with NULL enrichment columns "
        f"in intermediate_sales_with_details"
    )


def test_sales_with_details_rowcount_matches_filtered_sales(dwh_engine, read_sql):
    """
    Row count of intermediate_sales_with_details must match
    intermediate_filtered_sales (no loss or duplication).
    """

    details_df = read_sql(
        "SELECT COUNT(*) AS cnt FROM intermediate_sales_with_details",
        dwh_engine
    )
    filtered_df = read_sql(
        "SELECT COUNT(*) AS cnt FROM intermediate_filtered_sales",
        dwh_engine
    )

    details_count = details_df.iloc[0]["cnt"]
    filtered_count = filtered_df.iloc[0]["cnt"]

    assert details_count == filtered_count, (
        f"Rowcount mismatch: details={details_count}, "
        f"filtered={filtered_count}"
    )


def test_sales_with_details_join_integrity(dwh_engine, read_sql):
    """
    Ensure all records have valid joins with product and store tables.
    No orphan product_id or store_id should exist.
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_sales_with_details d
        LEFT JOIN staging_product p
            ON d.product_id = p.product_id
        LEFT JOIN staging_stores s
            ON d.store_id = s.store_id
        WHERE p.product_id IS NULL
           OR s.store_id IS NULL
        """,
        dwh_engine
    )

    orphan_count = df.iloc[0]["cnt"]

    assert orphan_count == 0, (
        f"Found {orphan_count} rows with invalid product/store joins"
    )


def test_sales_with_details_calculation_consistency(dwh_engine, read_sql):
    """
    Validate business consistency:
    total_sales = quantity * price
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_sales_with_details
        WHERE total_sales != quantity * price
        """,
        dwh_engine
    )

    bad_calc_count = df.iloc[0]["cnt"]

    assert bad_calc_count == 0, (
        f"Found {bad_calc_count} rows where total_sales != quantity * price"
    )
