import logging
import pandas as pd

from etl_core.config.settings import load_config
from etl_core.connectors.mysql import MySQLConnector

# ===== EXTRACTION =====
from etl_core.extraction.mysql_extractor import extract_from_mysql

# ===== TRANSFORMATIONS =====
from etl_core.transformations.incremental import get_last_fact_sales_watermark

# ===== LOADERS =====
from etl_core.loading.mysql_loader import load_dataframe

# ===== AUDIT =====
from etl_core.audit.audit_logger import (
    start_etl_run,
    end_etl_run,
    log_table_load,
)

logger = logging.getLogger(__name__)


def run_daily_pipeline():
    logger.info("Starting ETL Platform")

    config = load_config()
    conn = MySQLConnector(config.dwh_mysql).connect()

    run_id = start_etl_run(conn, "retail_etl_pipeline")

    total_tables = 0
    total_rows = 0
    success = False
    error_msg = None

    try:
        # =========================================================
        # FACT SALES (INCREMENTAL)
        # =========================================================
        sales_df = extract_from_mysql("sales", config.source_mysql)

        last_sale_date, _ = get_last_fact_sales_watermark(conn)

        if last_sale_date:
            sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"])
            sales_df = sales_df[sales_df["sale_date"] > last_sale_date]

        sales_rows = len(sales_df)

        if sales_rows > 0:
            load_dataframe(
                sales_df,
                table_name="fact_sales",
                db_config=config.dwh_mysql,
                if_exists="append"
            )

        log_table_load(
            conn,
            run_id,
            "fact_sales",
            sales_rows,
            "SUCCESS"
        )

        total_tables += 1
        total_rows += sales_rows

        # =========================================================
        # INVENTORY
        # =========================================================
        inventory_df = extract_from_mysql(
            "intermediate_aggregated_inventory_level",
            config.dwh_mysql
        )

        inventory_rows = len(inventory_df)

        if inventory_rows > 0:
            load_dataframe(
                inventory_df,
                table_name="inventory_levels_by_store",
                db_config=config.dwh_mysql,
                if_exists="replace"
            )

        log_table_load(
            conn,
            run_id,
            "inventory_levels_by_store",
            inventory_rows,
            "SUCCESS"
        )

        total_tables += 1
        total_rows += inventory_rows

        # =========================================================
        # MONTHLY SUMMARY
        # =========================================================
        monthly_df = extract_from_mysql(
            "intermediate_monthly_sales_summary_source",
            config.dwh_mysql
        )

        monthly_rows = len(monthly_df)

        if monthly_rows > 0:
            load_dataframe(
                monthly_df,
                table_name="monthly_sales_summary",
                db_config=config.dwh_mysql,
                if_exists="replace"
            )

        log_table_load(
            conn,
            run_id,
            "monthly_sales_summary",
            monthly_rows,
            "SUCCESS"
        )

        total_tables += 1
        total_rows += monthly_rows

        success = True

    except Exception as e:
        logger.exception("ETL pipeline failed")
        error_msg = str(e)

    finally:
        end_etl_run(
            conn,
            run_id,
            "SUCCESS" if success else "FAILED",
            total_tables,
            total_rows,
            error_msg
        )
