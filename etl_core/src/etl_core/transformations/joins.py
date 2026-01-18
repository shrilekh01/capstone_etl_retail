import pandas as pd
import logging

logger = logging.getLogger(__name__)


def join_sales_with_products(sales_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Joining sales with products")

    result = sales_df.merge(
        products_df,
        on="product_id",
        how="left",
        suffixes=("", "_product"),
    )

    logger.info(f"Result records after product join: {len(result)}")
    return result


def join_sales_with_stores(sales_df: pd.DataFrame, stores_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Joining sales with stores")

    result = sales_df.merge(
        stores_df,
        on="store_id",
        how="left",
        suffixes=("", "_store"),
    )

    logger.info(f"Result records after store join: {len(result)}")
    return result
