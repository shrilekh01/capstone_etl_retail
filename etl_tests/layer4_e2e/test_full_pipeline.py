import pytest

pytestmark = pytest.mark.e2e


def test_e2e_sales_flow(dwh_engine, read_sql):
    """
    Validate full flow:
    staging_sales → intermediate → fact_sales
    """

    staging_cnt = read_sql(
        "SELECT COUNT(*) AS cnt FROM staging_sales",
        dwh_engine
    ).iloc[0]["cnt"]

    intermediate_cnt = read_sql(
        "SELECT COUNT(*) AS cnt FROM intermediate_filtered_sales",
        dwh_engine
    ).iloc[0]["cnt"]

    fact_cnt = read_sql(
        "SELECT COUNT(*) AS cnt FROM fact_sales",
        dwh_engine
    ).iloc[0]["cnt"]

    assert staging_cnt >= intermediate_cnt, (
        "Filtered sales should not exceed staging sales"
    )

    assert intermediate_cnt == fact_cnt, (
        "Fact sales count does not match intermediate sales"
    )


def test_e2e_monthly_aggregation(dwh_engine, read_sql):
    """
    Validate monthly aggregation consistency
    """

    mismatch = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM monthly_sales_summary f
        JOIN intermediate_monthly_sales_summary_source i
            ON f.product_id = i.product_id
           AND f.year = i.year
           AND f.month = i.month
        WHERE f.monthly_total_sales != i.monthly_total_sales
        """,
        dwh_engine
    ).iloc[0]["cnt"]

    assert mismatch == 0, "Monthly aggregation mismatch found"


def test_e2e_inventory_consistency(dwh_engine, read_sql):
    """
    Validate inventory aggregation flow
    """

    mismatch = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM inventory_levels_by_store f
        JOIN intermediate_aggregated_inventory_level i
            ON f.store_id = i.store_id
        WHERE f.total_inventory != i.total_inventory
        """,
        dwh_engine
    ).iloc[0]["cnt"]

    assert mismatch == 0, "Inventory mismatch between intermediate and final"
