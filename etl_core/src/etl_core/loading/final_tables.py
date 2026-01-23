from etl_core.logging.logger import get_logger

logger = get_logger(__name__)


def load_fact_sales(engine):
    """
    Load fact_sales from intermediate_sales_with_details
    """
    logger.info("Starting load for fact_sales")

    with engine.connect() as conn:
        conn.execute("TRUNCATE TABLE retaildwh.fact_sales")

        conn.execute("""
            INSERT INTO retaildwh.fact_sales (
                sales_id,
                product_id,
                store_id,
                quantity,
                total_sales,
                sale_date,
                product_name,
                category,
                price,
                store_name
            )
            SELECT
                sales_id,
                product_id,
                store_id,
                quantity,
                total_sales,
                sale_date,
                product_name,
                category,
                price,
                store_name
            FROM retaildwh.intermediate_sales_with_details
        """)

    logger.info("fact_sales loaded successfully")


def load_monthly_sales_summary(engine):
    """
    Load monthly_sales_summary from intermediate_monthly_sales_summary_source
    """
    logger.info("Starting load for monthly_sales_summary")

    with engine.connect() as conn:
        conn.execute("TRUNCATE TABLE retaildwh.monthly_sales_summary")

        conn.execute("""
            INSERT INTO retaildwh.monthly_sales_summary (
                product_id,
                year,
                month,
                monthly_total_sales
            )
            SELECT
                product_id,
                year,
                month,
                monthly_total_sales
            FROM retaildwh.intermediate_monthly_sales_summary_source
        """)

    logger.info("monthly_sales_summary loaded successfully")


def load_inventory_levels(engine):
    """
    Load inventory_levels_by_store from intermediate_aggregated_inventory_level
    """
    logger.info("Starting load for inventory_levels_by_store")

    with engine.connect() as conn:
        conn.execute("TRUNCATE TABLE retaildwh.inventory_levels_by_store")

        conn.execute("""
            INSERT INTO retaildwh.inventory_levels_by_store (
                store_id,
                total_inventory
            )
            SELECT
                store_id,
                total_inventory
            FROM retaildwh.intermediate_aggregated_inventory_level
        """)

    logger.info("inventory_levels_by_store loaded successfully")
