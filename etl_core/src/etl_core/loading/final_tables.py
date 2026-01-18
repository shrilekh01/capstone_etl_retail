from etl_core.loading.mysql_loader import load_dataframe
from etl_core.config.settings import DatabaseConfig


def load_fact_sales(df, db_config: DatabaseConfig):
    load_dataframe(df, table_name="fact_sales", db_config=db_config, if_exists="append")


def load_fact_inventory(df, db_config: DatabaseConfig):
    load_dataframe(df, table_name="fact_inventory", db_config=db_config, if_exists="append")


def load_monthly_sales_summary(df, db_config: DatabaseConfig):
    load_dataframe(df, table_name="monthly_sales_summary", db_config=db_config, if_exists="replace")


def load_inventory_levels_by_store(df, db_config: DatabaseConfig):
    load_dataframe(df, table_name="inventory_levels_by_store", db_config=db_config, if_exists="replace")
