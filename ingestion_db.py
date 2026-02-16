import logging


def ingest_db(df, table_name, conn):
    """
    It will ingest dataframe into database
    """

    try:
        df.to_sql(
            table_name,
            con=conn,
            if_exists='replace',   # change to 'append' if needed
            index=False
        )

        logging.info(f"{table_name} table loaded successfully.")

    except Exception as e:
        logging.error(f"Error while loading table: {e}")
        raise
