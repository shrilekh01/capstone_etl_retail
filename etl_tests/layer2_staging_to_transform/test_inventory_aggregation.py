import pytest

pytestmark = pytest.mark.layer2


def test_inventory_aggregation_correctness(dwh_engine, read_sql):
    """
    inventory aggregation must match sum(quantity_on_hand)
    from staging_inventory grouped by store_id
    """

    df = read_sql(
        """
        SELECT 
            a.store_id,
            a.total_inventory,
            SUM(s.quantity_on_hand) AS expected_inventory
        FROM intermediate_aggregated_inventory_level a
        JOIN staging_inventory s
            ON a.store_id = s.store_id
        GROUP BY a.store_id, a.total_inventory
        HAVING a.total_inventory != SUM(s.quantity_on_hand)
        """,
        dwh_engine
    )

    mismatch_count = len(df)

    assert mismatch_count == 0, (
        f"Found {mismatch_count} mismatched inventory aggregation records"
    )


def test_inventory_no_negative_values(dwh_engine, read_sql):
    """
    Inventory levels should never be negative.
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM intermediate_aggregated_inventory_level
        WHERE total_inventory < 0
        """,
        dwh_engine
    )

    bad_count = df.iloc[0]["cnt"]

    assert bad_count == 0, (
        f"Found {bad_count} negative inventory values"
    )
