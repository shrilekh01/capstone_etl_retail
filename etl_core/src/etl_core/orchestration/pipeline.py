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

from etl_core.audit.audit_logger import (
    start_etl_run,
    end_etl_run,
    log_table_load
)

from etl_core.connectors.mysql import MySQLConnector

logger = logging.getLogger(__name__)


def run_daily_pipeline():
    logger.info("Starting daily ETL pipeline")

    config = load_config()

    # ✅ Proper DB connection
    mysql_connector = MySQLConnector(config.dwh_mysql)
    conn = mysql_connector.connect()

    run_id = start_etl_run(conn, "retail_etl_pipeline")

    total_tables = 0
    total_rows = 0

    try:
        # ETL logic...
        pass

    except Exception as e:
        end_etl_run(
            conn,
            run_id,
            "FAILED",
            total_tables,
            total_rows,
            str(e)
        )
        raise

