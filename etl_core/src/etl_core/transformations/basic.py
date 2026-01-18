import pandas as pd
import logging

logger = logging.getLogger(__name__)


def filter_sales_by_date(df: pd.DataFrame, min_date: str, date_column: str = "sale_date") -> pd.DataFrame:
    logger.info(f"Filtering sales data from date >= {min_date}")

    df = df.copy()
    df[date_column] = pd.to_datetime(df[date_column])

    filtered = df[df[date_column] >= pd.to_datetime(min_date)]

    logger.info(f"Records before: {len(df)}, after filter: {len(filtered)}")
    return filtered


def drop_nulls(df: pd.DataFrame, required_columns: list[str]) -> pd.DataFrame:
    logger.info(f"Dropping rows with NULLs in columns: {required_columns}")

    before = len(df)
    cleaned = df.dropna(subset=required_columns)
    after = len(cleaned)

    logger.info(f"Records before: {before}, after drop_nulls: {after}")
    return cleaned
