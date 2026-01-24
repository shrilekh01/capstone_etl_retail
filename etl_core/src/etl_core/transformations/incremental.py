import pandas as pd


def get_last_fact_sales_watermark(conn):
    """
    Returns last processed sale_date and sales_id
    Used for incremental fact_sales load
    """
    query = """
        SELECT 
            MAX(sale_date) AS last_sale_date,
            MAX(sales_id) AS last_sales_id
        FROM retaildwh.fact_sales
    """
    df = pd.read_sql(query, conn)

    if df.empty or df.iloc[0]["last_sale_date"] is None:
        return None, None

    return df.iloc[0]["last_sale_date"], df.iloc[0]["last_sales_id"]
