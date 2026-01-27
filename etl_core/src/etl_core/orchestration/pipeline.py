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
    logger.info("Starting ETL Platform")

    config = load_config()
    conn = MySQLConnector(config.dwh_mysql).connect()


    run_id = start_etl_run(conn, "retail_etl_pipeline")

    total_tables = 0
    total_rows = 0
    success = False
    error_msg = None

    try:
        # =========================
        # FACT SALES
        # =========================
        sales_df = extract_from_mysql("sales", config.source_mysql)

        last_sale_date = get_last_loaded_date(
            conn,
            table_name="fact_sales",
            date_column="sale_date"
        )

        if last_sale_date:
            sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"])
            sales_df = sales_df[sales_df["sale_date"] > last_sale_date]

        sales_rows = len(sales_df)

        if sales_rows > 0:
            load_dataframe(
                sales_df,
                "fact_sales",
                config.dwh_mysql,
                if_exists="append"
            )

        log_table_load(conn, run_id, "fact_sales", sales_rows, "SUCCESS")
        total_tables += 1
        total_rows += sales_rows

        # =========================
        # INVENTORY
        # =========================
        inventory_df = extract_from_mysql(
            "intermediate_aggregated_inventory_level",
            config.dwh_mysql
        )

        inventory_rows = len(inventory_df)

        if inventory_rows > 0:
            load_dataframe(
                inventory_df,
                "inventory_levels_by_store",
                config.dwh_mysql,
                if_exists="replace"
            )

        log_table_load(conn, run_id, "inventory_levels_by_store", inventory_rows, "SUCCESS")
        total_tables += 1
        total_rows += inventory_rows

        # =========================
        # MONTHLY
        # =========================
        monthly_df = extract_from_mysql(
            "intermediate_monthly_sales_summary_source",
            config.dwh_mysql
        )

        monthly_rows = len(monthly_df)

        if monthly_rows > 0:
            load_dataframe(
                monthly_df,
                "monthly_sales_summary",
                config.dwh_mysql,
                if_exists="replace"
            )

        log_table_load(conn, run_id, "monthly_sales_summary", monthly_rows, "SUCCESS")
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


