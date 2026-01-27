from datetime import datetime
from etl_core.logging.logger import get_logger

logger = get_logger(__name__)


def start_etl_run(conn, pipeline_name):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO retaildwh.etl_run_audit
        (pipeline_name, status)
        VALUES (%s, %s)
    """, (pipeline_name, "STARTED"))

    run_id = cursor.lastrowid
    conn.commit()
    return run_id


def end_etl_run(conn, run_id, status, total_tables, total_rows, error_message=None):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE retaildwh.etl_run_audit
        SET
            run_end_time = %s,
            status = %s,
            total_tables_loaded = %s,
            total_rows_loaded = %s,
            error_message = %s
        WHERE run_id = %s
    """, (
        datetime.now(),
        status,
        total_tables,
        total_rows,
        error_message,
        run_id
    ))
    conn.commit()


def log_table_load(conn, run_id, table_name, rows_loaded, status, error_message=None):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO retaildwh.table_load_audit
        (run_id, table_name, rows_loaded, status, error_message)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        run_id,
        table_name,
        rows_loaded,
        status,
        error_message
    ))
    conn.commit()
