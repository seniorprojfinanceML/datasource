
def get_create_table_query(table_name):
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            stockname VARCHAR(255) NOT NULL,
            datetime TIMESTAMP NOT NULL,
            open_price FLOAT,
            close_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            volume INTEGER,
            PRIMARY KEY (stockname, datetime)
        )
        """
    return create_table_query