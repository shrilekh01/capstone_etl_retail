import pandas as pd
import logging

logger = logging.getLogger(__name__)


def monthly_sales_summary(sales_df: pd.DataFrame, date_column: str = "sale_date") -> pd.DataFrame:
    logger.info("Calculating monthly sales summary")

    df = sales_df.copy()
    df[date_column] = pd.to_datetime(df[date_column])

    df["year"] = df[date_column].dt.year
    df["month"] = df[date_column].dt.month

    summary = (
        df.groupby(["product_id", "year", "month"], as_index=False)["total_sales"]
        .sum()
        .rename(columns={"total_sales": "monthly_total_sales"})
    )

    logger.info(f"Monthly summary records: {len(summary)}")
    return summary


def inventory_by_store(inventory_df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Calculating inventory by store")

    summary = (
        inventory_df.groupby(["store_id"], as_index=False)["quantity_on_hand"]
        .sum()
        .rename(columns={"quantity_on_hand": "total_inventory"})
    )

    logger.info(f"Inventory summary records: {len(summary)}")
    return summary
