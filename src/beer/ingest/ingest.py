

from beer.ingest.keymap import KeyMap
from beer.ingest.timestamp_database import TimestampDatabase
from beer.ingest.convert import Converter
import pandas as pd
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

    def __init__(self, conn, products, sizes, categories, timestamps):
        # Maps to look up (or add new) integer values for these fields
        self.conn = conn
        self.products = products
        self.sizes = sizes
        self.categories = categories
        self.converter = Converter(products, sizes, categories)
        self.timestamps = timestamps
        self.cur = self.conn.cursor()

    def ingest(self, filename):
        """
        Convert a file and make necessary additions to inventory, products, sizes, categories, and timestamps.
        :param filename: the file to process
        :return: None
        """
        data = pd.read_csv(filename)
        data = self.converter.convert(data)

        # We have to write this before the inventory data because inventory(timestamp)
        # depends on timestamps(timestamp).
        timestamp = data['timestamp'].unique()[0]
        self.timestamps.add(timestamp)

        # We save newly discovered keys for the same reason as the timestamp: inventory
        # depends on the values
        self.products.save_new_keys()
        self.sizes.save_new_keys()
        self.categories.save_new_keys()

        # Convert dataframe to csv as a string
        output = io.StringIO()
        data.to_csv(output, header=False, index=False)
        # seek to the beginning so copy_from can read it
        output.seek(0)

        # Save the inventory
        # column names are required here because the inventory_id will be auto-filled for each row
        columns = ('product_id', 'category_id', 'size_id', 'case_pack', 'timestamp ',
                   'quantity', 'retail', 'case_retail')
        self.cur.copy_from(output, 'inventory', sep=',', columns=columns)
        self.conn.commit()


if __name__ == '__main__':

    import sys
    import dotenv
    import os

    if len(sys.argv) != 2:
        print('Provide path to CSV files')
        sys.exit(1)

    path = sys.argv[1]

    dotenv.load_dotenv()
    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    database_name = os.getenv('DB_NAME')

    conn = psycopg2.connect(dbname=database_name, host=host, user=user, password=password)

    # Maps to look up (or add new) integer values for these fields
    products = KeyMap(conn, 'products')
    sizes = KeyMap(conn, 'sizes')
    categories = KeyMap(conn, 'categories')
    timestamps = TimestampDatabase(conn)

    i = Ingest(conn, products, sizes, categories, timestamps)

    files = sorted(os.listdir(path))
    count = 0
    total = len(files)

    for file in files:
        if not file.endswith('csv'):
            continue
        print('{}/{} ({}) {}'.format(count, total, count/total, file))
        count += 1
        i.ingest(path + '/' + file)
