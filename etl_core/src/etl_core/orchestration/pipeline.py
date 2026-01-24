import logging

from etl_core.config.settings import load_config
import pandas as pd

# ===== EXTRACTION =====
from etl_core.extraction.file_extractor import extract_file
from etl_core.extraction.xml_inventory_reader import read_inventory_xml
from etl_core.extraction.json_supplier_reader import read_supplier_json
from etl_core.extraction.mysql_extractor import extract_from_mysql

# ===== STAGING =====
from etl_core.staging.staging_manager import truncate_and_load

# ===== TRANSFORMATIONS =====
from etl_core.transformations.basic import drop_nulls, filter_sales_by_date
from etl_core.transformations.joins import (
    join_sales_with_products,
    join_sales_with_stores,
)
from etl_core.transformations.aggregations import (
    monthly_sales_summary,
    inventory_by_store,
)
from etl_core.transformations.routing import route_by_threshold
from etl_core.transformations.incremental import (
    get_last_fact_sales_watermark,
)

# ===== LOADERS =====
from etl_core.loading.mysql_loader import load_dataframe

# ===== AUDIT =====
from etl_core.audit.audit_logger import (
    start_etl_run,
    end_etl_run,
    log_table_load,
)

from etl_core.connectors.mysql import MySQLConnector

logger = logging.getLogger(__name__)


def run_daily_pipeline():
    logger.info("Starting daily ETL pipeline")

    config = load_config()
    mysql_connector = MySQLConnector(config.dwh_mysql)
    conn = mysql_connector.connect()

    run_id = start_etl_run(conn, "retail_etl_pipeline")

    total_tables = 0
    total_rows = 0

    try:
        # =============================
        # 1️⃣ EXTRACT
        # =============================
        sales_df = extract_from_mysql(
            "SELECT * FROM sales",
            config.source_mysql
        )

        products_df = extract_from_mysql(
            "SELECT * FROM products",
            config.source_mysql
        )

        stores_df = extract_from_mysql(
            "SELECT * FROM stores",
            config.source_mysql
        )

        # =============================
        # 2️⃣ CLEAN
        # =============================
        sales_df = drop_nulls(
            sales_df,
            required_columns=[
                "sales_id",
                "product_id",
                "store_id",
                "sale_date",
                "total_sales"
            ]
        )

        # =============================
        # 3️⃣ INCREMENTAL FILTER
        # =============================
        last_sale_date, last_sales_id = get_last_fact_sales_watermark(conn)

        # 🔴 IMPORTANT FIX
        sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"])

        if last_sale_date:
            last_sale_ts = pd.to_datetime(last_sale_date)

            sales_df = sales_df[
                (sales_df["sale_date"] > last_sale_ts)
                | (
                        (sales_df["sale_date"] == last_sale_ts)
                        & (sales_df["sales_id"] > last_sales_id)
                )
                ]

        # =============================
        # 4️⃣ JOIN DIMENSIONS
        # =============================
        sales_df = join_sales_with_products(sales_df, products_df)
        sales_df = join_sales_with_stores(sales_df, stores_df)

        # =============================
        # 5️⃣ LOAD FACT SALES
        # =============================
        if not sales_df.empty:
            load_dataframe(
                sales_df,
                "fact_sales",
                config.dwh_mysql,
                if_exists="append",
            )

            rows = len(sales_df)
            log_table_load(conn, run_id, "fact_sales", rows, "SUCCESS")

            total_tables += 1
            total_rows += rows

        # =============================
        # 6️⃣ INVENTORY SNAPSHOT
        # =============================
        inventory_df = inventory_by_store(sales_df)

        load_dataframe(
            inventory_df,
            "inventory_levels_by_store",
            config.dwh_mysql,
            if_exists="replace",
        )

        log_table_load(
            conn,
            run_id,
            "inventory_levels_by_store",
            len(inventory_df),
            "SUCCESS",
        )

        total_tables += 1
        total_rows += len(inventory_df)

        # =============================
        # 7️⃣ MONTHLY SUMMARY
        # =============================
        monthly_df = monthly_sales_summary(sales_df)

        load_dataframe(
            monthly_df,
            "monthly_sales_summary",
            config.dwh_mysql,
            if_exists="replace",
        )

        log_table_load(
            conn,
            run_id,
            "monthly_sales_summary",
            len(monthly_df),
            "SUCCESS",
        )

        total_tables += 1
        total_rows += len(monthly_df)

        # =============================
        # 8️⃣ FINALIZE
        # =============================
        end_etl_run(
            conn,
            run_id,
            "SUCCESS",
            total_tables,
            total_rows,
        )

        logger.info("ETL pipeline completed successfully")

    except Exception as e:
        logger.exception("ETL pipeline failed")

        end_etl_run(
            conn,
            run_id,
            "FAILED",
            total_tables,
            total_rows,
            str(e),
        )
        raise

