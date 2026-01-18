import logging
from etl_core.config.settings import load_config

# Extraction
from etl_core.extraction.file_extractor import extract_file
from etl_core.extraction.mysql_extractor import extract_from_mysql
from etl_core.extraction.oracle_extractor import extract_from_oracle

# Staging
from etl_core.staging.staging_manager import truncate_and_load

# Transformations
from etl_core.transformations.basic import filter_sales_by_date, drop_nulls
from etl_core.transformations.joins import join_sales_with_products, join_sales_with_stores
from etl_core.transformations.aggregations import monthly_sales_summary, inventory_by_store

# Loading
from etl_core.loading.final_tables import (
    load_fact_sales,
    load_fact_inventory,
    load_monthly_sales_summary,
    load_inventory_levels_by_store,
)

logger = logging.getLogger(__name__)


def run_daily_pipeline():
    logger.info("Starting daily ETL pipeline")

    config = load_config()

    # -------------------------------------------------
    # STEP 1: EXTRACTION
    # -------------------------------------------------

    products_df = extract_file("data/products.csv")

    sales_df = extract_from_mysql(
        query="SELECT * FROM sales",
        db_config=config.mysql,
    )

    stores_df = extract_from_oracle(
        query="SELECT * FROM stores",
        db_config=config.mysql,  # later: oracle config
    )

    # -------------------------------------------------
    # STEP 2: STAGING
    # -------------------------------------------------

    truncate_and_load(products_df, "staging_products", config.mysql)
    truncate_and_load(sales_df, "staging_sales", config.mysql)
    truncate_and_load(stores_df, "staging_stores", config.mysql)

    # -------------------------------------------------
    # STEP 3: TRANSFORMATIONS
    # -------------------------------------------------

    # Basic cleaning
    sales_df = drop_nulls(sales_df, ["sales_id", "product_id", "store_id"])
    sales_df = filter_sales_by_date(sales_df, min_date="2024-01-01")

    # Enrichment
    sales_enriched = join_sales_with_products(sales_df, products_df)
    sales_enriched = join_sales_with_stores(sales_enriched, stores_df)

    # Aggregations
    monthly_summary_df = monthly_sales_summary(sales_enriched)
    inventory_summary_df = inventory_by_store(sales_enriched)

    # -------------------------------------------------
    # STEP 4: FINAL LOAD
    # -------------------------------------------------

    load_fact_sales(sales_enriched, config.mysql)
    load_fact_inventory(sales_enriched, config.mysql)
    load_monthly_sales_summary(monthly_summary_df, config.mysql)
    load_inventory_levels_by_store(inventory_summary_df, config.mysql)

    logger.info("Daily ETL pipeline completed successfully")
