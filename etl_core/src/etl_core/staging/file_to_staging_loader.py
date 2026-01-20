from sqlalchemy import text

def load_dataframe_to_table(df, engine, table_name: str):
    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False
    )
