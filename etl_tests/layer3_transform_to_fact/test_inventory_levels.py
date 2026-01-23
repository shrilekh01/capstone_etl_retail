import pytest

pytestmark = pytest.mark.layer3


def test_inventory_levels_match_intermediate(dwh_engine, read_sql):
    """
    inventory_levels_by_store must match intermediate_aggregated_inventory_level
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM inventory_levels_by_store f
        JOIN intermediate_aggregated_inventory_level i
            ON f.store_id = i.store_id
        WHERE f.total_inventory != i.total_inventory
        """,
        dwh_engine
    )

    assert df.iloc[0]["cnt"] == 0, (
        "Mismatch between inventory fact and intermediate inventory"
    )


def test_inventory_no_negative_values(dwh_engine, read_sql):
    """
    Inventory should never be negative
    """

    df = read_sql(
        """
        SELECT COUNT(*) AS cnt
        FROM inventory_levels_by_store
        WHERE total_inventory < 0
        """,
        dwh_engine
    )

    assert df.iloc[0]["cnt"] == 0, "Negative inventory values found"
