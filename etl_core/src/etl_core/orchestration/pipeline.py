import logging

from etl_core.config.settings import DATA_DIR
from etl_core.config.settings import load_config

# ===== EXTRACTION =====
from etl_core.extraction.file_extractor import extract_file
from etl_core.extraction.xml_inventory_reader import read_inventory_xml
from etl_core.extraction.json_supplier_reader import read_supplier_json
from etl_core.extraction.mysql_extractor import extract_from_mysql

# ===== STAGING =====
from etl_core.staging.staging_manager import truncate_and_load

# ===== TRANSFORMATIONS =====
from etl_core.transformations.basic import drop_nulls, filter_sales_by_date
from etl_core.transformations.joins import join_sales_with_products, join_sales_with_stores
from etl_core.transformations.aggregations import monthly_sales_summary, inventory_by_store
from etl_core.transformations.routing import route_by_threshold

# ===== FINAL LOADERS (GENERIC, SAFE) =====
from etl_core.loading.mysql_loader import load_dataframe


logger = logging.getLogger(__name__)


def run_daily_pipeline():
    logger.info("Starting daily ETL pipeline")

    config = load_config()

    # ============================================================
    # STEP 1: EXTRACT
    # ============================================================

    products_df = extract_file(str(DATA_DIR / "products.csv"))

    sales_df = extract_from_mysql(
        query="SELECT * FROM sales",
        db_config=config.source_mysql,
    )

    stores_df = extract_from_mysql(
        query="SELECT * FROM stores",
        db_config=config.source_mysql,
    )

    inventory_df = read_inventory_xml(str(DATA_DIR / "inventory_data.xml"))
    supplier_df = read_supplier_json(str(DATA_DIR / "supplier_data.json"))

    # ============================================================
    # STEP 2: LOAD TO STAGING
    # ============================================================

    truncate_and_load(products_df, "staging_product", config.dwh_mysql)
    truncate_and_load(sales_df, "staging_sales", config.dwh_mysql)
    truncate_and_load(stores_df, "staging_stores", config.dwh_mysql)
    truncate_and_load(inventory_df, "staging_inventory", config.dwh_mysql)
    truncate_and_load(supplier_df, "staging_supplier", config.dwh_mysql)

    # ============================================================
    # STEP 3: LAYER 2 — INTERMEDIATE
    # ============================================================

    # 3.1 Filter sales
    sales_df = drop_nulls(sales_df, ["sales_id", "product_id", "store_id"])
    sales_df = filter_sales_by_date(sales_df, min_date="2024-01-01")

    truncate_and_load(
        sales_df,
        "intermediate_filtered_sales",
        config.dwh_mysql
    )

    # 3.2 High / Low sales split
    high_df, low_df = route_by_threshold(
        sales_df,
        column="total_sales",
        threshold=1000
    )

    truncate_and_load(high_df, "intermediate_high_sales", config.dwh_mysql)
    truncate_and_load(low_df, "intermediate_low_sales", config.dwh_mysql)

    # 3.3 Monthly aggregation
    monthly_summary_source_df = monthly_sales_summary(sales_df)

    truncate_and_load(
        monthly_summary_source_df,
        "intermediate_monthly_sales_summary_source",
        config.dwh_mysql
    )

    # 3.4 Inventory aggregation
    inventory_agg_df = inventory_by_store(inventory_df)

    truncate_and_load(
        inventory_agg_df,
        "intermediate_aggregated_inventory_level",
        config.dwh_mysql
    )

    # 3.5 Enrichment joins
    sales_enriched = join_sales_with_products(sales_df, products_df)
    sales_enriched = join_sales_with_stores(sales_enriched, stores_df)

    truncate_and_load(
        sales_enriched,
        "intermediate_sales_with_details",
        config.dwh_mysql
    )

    # ============================================================
    # STEP 4: LAYER 3 — FINAL TABLES
    # ============================================================

    # FACT SALES
    load_dataframe(
        sales_enriched,
        "fact_sales",
        config.dwh_mysql,
        if_exists="replace"
    )

    # INVENTORY SNAPSHOT
    load_dataframe(
        inventory_agg_df,
        "inventory_levels_by_store",
        config.dwh_mysql,
        if_exists="replace"
    )

    # MONTHLY SUMMARY
    load_dataframe(
        monthly_summary_source_df,
        "monthly_sales_summary",
        config.dwh_mysql,
        if_exists="replace"
    )

    logger.info("ETL pipeline completed successfully")
