

import logging
from beer.ingest.keymap import KeyMap
from beer.ingest.keymap_database import KeyMapDatabase
from beer.ingest.timestamp_database import TimestampDatabase
import pandas as pd
import dotenv
import os
import sqlalchemy
import time
import io


import psycopg2

class Ingest:
    """
    Ingest one CSV file into the Inventory table.  This will convert name, size, and category to
    integer values form the corresponding tables (adding new entries, if necessary) and
    truncate the fractional part of the timestamp.


    This script may be called directly to ingest all files into the inventory table (with
    necessary additions to products, sizes, and categories).
    """

    def __init__(self):
        logging.basicConfig(filename='ingest.log', level=logging.DEBUG)

        # Maps to look up (or add new) integer values for these fields
        self.products = KeyMap(KeyMapDatabase('products'))
        self.sizes = KeyMap(KeyMapDatabase('sizes'))
        self.categories = KeyMap(KeyMapDatabase('categories'))

        self.timestamp_db = TimestampDatabase()

        host = os.getenv('DB_HOST')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database_name = os.getenv('DB_NAME')

        self.conn = psycopg2.connect(dbname=database_name, host=host, user=user, password=password)
        self.cur = self.conn.cursor()

    def ingest(self, filename):
        """
        Convert a file and make necessary additions to inventory, products, sizes, categories, and timestamps.
        :param filename: the file to process
        :return: None
        """
        data = pd.read_csv(filename)
        data = data.dropna()

        # If there is a bad line in the file, the type of 'Case Pack' will end up
        # as a float, and writing to the DB will fail.
        data['Case Pack'] = data['Case Pack'].astype(int)

        # Map name, size, and category to integer values
        # Remove commas from the name and then apply the map
        data['Name'] = data['Name'].apply(lambda x: self.products.get_value(x.replace(',', '')))
        data['size'] = data['size'].apply(lambda x: self.sizes.get_value(x))
        data['Category'] = data['Category'].apply(lambda x: self.categories.get_value(x))

        # Remove fractional part from timestamp
        data['timestamp'] = data['timestamp'].apply(lambda x: x.split('.')[0])

        # Rename columns to match database columns
        name_map = {'Name': 'product_id',
                    'size': 'size_id',
                    'Category': 'category_id',
                    'Quantity_Available': 'quantity',
                    'Retail': 'retail',
                    'Case Retail': 'case_retail',
                    'Case Pack': 'case_pack'}
        data = data.rename(columns=name_map)

        # Remove rows that are full duplicates
        data.drop_duplicates(
            subset=['product_id', 'category_id', 'size_id', 'case_pack', 'quantity', 'retail', 'case_retail'],
            inplace=True)

        # Aggregate duplicates by adding quantities and taking maximum of retail and case_retail
        data = data.groupby(['product_id', 'category_id', 'size_id', 'case_pack', 'timestamp'], as_index=False).agg(
            {'quantity': 'sum', 'retail': 'max', 'case_retail': 'max'})

        # The last value is the timestamp.  Every row has the same value, so we
        # can use the last one
        # We have to write this before the inventory data because inventory(timestamp)
        # depends on timestamps(timestamp).
        # All the timestamps in a file are the same
        timestamp = data['timestamp'].unique()[0]
        self.timestamp_db.add(timestamp)

        # Convert dataframe to csv as a string
        output = io.StringIO()
        data.to_csv(output, header=False, index=False)
        output.seek(0)

        # column names are required here because the inventory_id will be auto-filled for each row
        columns = ('product_id', 'category_id', 'size_id', 'case_pack', 'timestamp ', 'quantity', 'retail', 'case_retail')
        self.cur.copy_from(output, 'inventory', sep=',', columns=columns)
        self.conn.commit()


if __name__ == '__main__':

    import os
    import sys

    if len(sys.argv) != 2:
        print('Provide path to CSV files')
        sys.exit(1)

    path = sys.argv[1]

    i = Ingest()

    files = sorted(os.listdir(path))
    count = 0
    total = len(files)

    for file in files:
        if not file.endswith('csv'):
            continue
        print('{}/{} ({}) {}'.format(count, total, count/total, file))
        count += 1
        i.ingest(path + '/' + file)
