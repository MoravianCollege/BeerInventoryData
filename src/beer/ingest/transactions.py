
import pandas as pd


def compute_transactions(first, second):
    """
    Compute all transaction between two dataframes of inventory. A "product" is defined as (product_id, size_id,
    category_id, case_pack), and a transactions is changes in the quantity for a product.  This means that if
    the quantities are the same in both inventories, there will NOT be a row for the product in the result.

    The results have the following fields:
    * 'pre_inventory_id' - the inventory id from the first inventory
    * 'post_inventory_id' - the inventory id from the second inventory
    * 'product_id' - the product id
    * 'category_id' - the category id
    * 'size_id' - the size id
    * 'case_pack' the case pack size
    * 'transaction_quantity' - the transaction quantity.  Postive means sale, negative means stock received
    * 'timestamp' - the timestamp from the second inventory
    * 'retail' - the cost from the second inventory
    * 'case_retail' - the cost of a case from the second inventory

    If a product exists in the second inventory but not the first, the result will be a negative transaction
    quantity.  This represents a new product added to inventory.

    If a product exists in the first inventory but not the second, the product is ignored (no result).

    :param first: a dataframe representing the first inventory
    :param second: a dataframe repreasenting the second inventory
    :return: a dataframe with the form above, may contain zero rows
    """

    # right join to ensure that new products are included
    # fillna for missing products from first (so the difference in assign works)
    # astype on pre_inventory_id because NaN for missing products causes type to be float

    #,

    import numpy as np

    return first.merge(second, on=['product_id', 'size_id', 'category_id', 'case_pack'], how='right') \
        .fillna({'quantity_x': 0, 'inventory_id_x': -1}) \
        .assign(transaction_quantity=lambda x : x['quantity_x'] - x['quantity_y']) \
        .rename(columns={'inventory_id_x': 'pre_inventory_id',
                         'inventory_id_y': 'post_inventory_id',
                         'timestamp_y': 'timestamp',
                         'retail_y': 'retail',
                         'case_retail_y': 'case_retail'}) \
        .astype({'pre_inventory_id': 'int64'}) \
        .loc[:,['pre_inventory_id', 'post_inventory_id', 'product_id', 'category_id', 'size_id', 'case_pack',
                'transaction_quantity', 'timestamp', 'retail', 'case_retail']] \
        .query('transaction_quantity != 0')


def update_inventory(previous, current):
    return pd.concat([previous, current]) \
             .sort_values(by='timestamp') \
             .drop_duplicates(subset=['product_id', 'size_id', 'category_id', 'case_pack'], keep='last')


if __name__ == '__main__':
    import psycopg2
    import pandas.io.sql as psql
    import dotenv
    import os
    import io
    from datetime import datetime, timedelta

    dotenv.load_dotenv()
    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    database_name = os.getenv('DB_NAME')

    conn = psycopg2.connect(dbname=database_name, host=host, user=user, password=password)
    cur = conn.cursor()

    timestamps = psql.read_sql_query('SELECT * FROM timestamps', con=conn)

    first_timestamp = timestamps['timestamp'][0]
    inventory = psql.read_sql_query("SELECT * FROM inventory WHERE timestamp='{}'".format(first_timestamp), con=conn)

    prev = first_timestamp
    for timestamp in timestamps['timestamp']:
        print(timestamp, end=': ')
        if timestamp == first_timestamp:
            continue

        prev_dt = pd.to_datetime(prev)
        curr_dt = pd.to_datetime(timestamp)

        delta = curr_dt - prev_dt
        threshold = timedelta(minutes=60)

        current = psql.read_sql_query("SELECT * FROM inventory WHERE timestamp='{}'".format(timestamp), con=conn)

        if delta <= threshold:

            transactions = compute_transactions(inventory, current)

            print(len(transactions), end='... ')

            if len(transactions) > 0:
                output = io.StringIO()
                transactions.to_csv(output, header=False, index=False)
                output.seek(0)

                # column names are required here because the transaction_id will be auto-filled for each row
                columns = ('pre_inventory_id', 'post_inventory_id', 'product_id', 'category_id', 'size_id', 'case_pack',
                           'transaction_quantity', 'timestamp', 'retail', 'case_retail')
                cur.copy_from(output, 'transactions', sep=',', columns=columns, null='NULL')
                conn.commit()
                print('saved')
            else:
                print('nothing to save')
        else:
            print('gap to big, skipping')

        inventory = update_inventory(inventory, current)
        prev = timestamp
