import pandas as pd
import logging

logger = logging.getLogger(__name__)


def route_by_threshold(df: pd.DataFrame, column: str, threshold: float):
    """
    Splits DataFrame into two:
      - high_df: column >= threshold
      - low_df:  column < threshold
    """
    logger.info(f"Routing data by threshold on {column}: {threshold}")

    high_df = df[df[column] >= threshold]
    low_df = df[df[column] < threshold]

    logger.info(f"High records: {len(high_df)}, Low records: {len(low_df)}")

    return high_df, low_df
