from beer.ingest.db_transactions import DBTransactions
from beer.ingest.name_map import NameMap
from beer.ingest.product_map import ProductMap
from beer.ingest.current_csv_inventory import CurrentCSVInventory
from beer.ingest.size_parser import parse_size
from beer.ingest.transactions import compute_transactions
import pandas as pd
import psycopg2
from datetime import timedelta


class Ingest:
    """
    Ingest one CSV file into the Inventory table.  This will:

    * Remove lines containing NA
    * Remove fractional time from the timestamp
    * Remove commas from names
    * Remove exact duplicate rows
    * Aggregate duplicate products (sum quantity, max retail, max case_retail)
    * Add missing products based on previous inventory
    * Separate the size column into container_quantity, container_volume, and container_type
    * Map names to integers and place values in the names table
    * Map products (name, container_quantity, container_volume, and container_type, case_pack, category)
      to integers and place vlues in the products table
    * Remove any rows with zero quantity
    * Save to the inventory table
    * Compute transactions from the previous inventory and save to the transactions table

    This script may be called directly to ingest all files in a specified directory
    """

    def __init__(self, database_transactions):
        # Maps to look up (or add new) integer values for these fields
        self.database_transactions = database_transactions
        products_df = database_transactions.get_table('products')
        self.products = ProductMap(products_df)
        names_df = database_transactions.get_table('names')
        names = NameMap(names_df)
        self.names = names
        current_csv_inventory_df = database_transactions.get_table('current_inventory')
        self.current_csv_inventory = CurrentCSVInventory(current_csv_inventory_df)
        self.prev_timestamp = None

    def ingest(self, filename):
        """
        Convert a file and make necessary additions to inventory, products, sizes, categories, and timestamps.
        :param filename: the file to process
        :return: None
        """
        data = pd.read_csv(filename)
        data = self.convert(data)
        self.save_tables(data)
        self.compute_and_save_transactions(data)

    def convert(self, data):
        # Rename the columns to match the database values (remove spaces, etc.)
        name_map = {'Name': 'name',
                    'Quantity_Available': 'quantity',
                    'Retail': 'retail',
                    'Case Retail': 'case_retail',
                    'Case Pack': 'case_pack',
                    'Category': 'category'}
        data = data.rename(columns=name_map)

        # if the file contains any incomplete lines, drop them
        # Done in place so we don't generate SettingWithCopyWarning on later steps
        data.dropna(inplace=True)
        # If there is a bad line in the file, the type of 'Case Pack' will end up
        # as a float, and writing to the DB will fail.
        data['case_pack'] = data['case_pack'].astype(int)
        # Remove fractional part from timestamp
        data['timestamp'] = pd.DataFrame(data['timestamp'].str.split('.').values.tolist()).iloc[:, 0]
        # Remove commas from the name
        data['name'] = data['name'].str.replace(',', '')

        # Remove rows that duplicate the quantity and prices
        data.drop_duplicates(subset=['name', 'size', 'category', 'case_pack', 'quantity', 'retail', 'case_retail'], inplace=True)
        # Aggregate remaining duplicates by adding quantities and taking maximum of retail and case_retail
        data = data.groupby(['name', 'size', 'category', 'case_pack', 'timestamp'],
                            as_index=False).agg({'quantity': 'sum', 'retail': 'max', 'case_retail': 'max'})

        # Update the current inventory in CSV format
        data = self.current_csv_inventory.update_inventory(data)
        # Split the size column into the quantity, volume, and type of container
        keg_re = r'(\d/\d).*KEG.*'

        def keg_match(m):
            size = m.group(1)
            return '1,{} keg,keg'.format(size)

        metric_keg_re = r'(\d+)\s+(LITER|LTR|OZ).*KEG.*'

        def metric_keg_match(m):
            size = m.group(1)
            return '1,{} liter keg,keg'.format(size)

        growler_re = r'32/64 OZ GROWLER'

        def growler_match(m):
            return '1,32 or 64 oz growler,growler'

        containers = {'KEG': 'keg', 'BTL': 'bottle', 'CAN': 'can', 'ALUM': 'can', 'POUCH': 'pouch',
                      "GROWLER": 'growler',
                      'BAG': 'bag', 'JAR': 'jar', 'PLASTIC': 'plastic', 'SLUSHEE': 'slushee', 'HOT CIDER': 'hot cider',
                      'NR': 'bottle'}
        units = {'OZ': 'oz', 'LITER': 'liter', 'LTR': 'liter', 'ML': 'ml'}
        containers_re = '|'.join(containers.keys())
        units_re = '|'.join(units.keys())

        group_re = r'(\d+)/(\d+(.\d+)?)\s+({})\s+({}).*'.format(units_re, containers_re)

        def group_match(m):
            quantity = int(m.group(1))
            size = m.group(2)
            unit = units[m.group(4)]
            container = containers[m.group(5)]
            return '{},{} {},{}'.format(quantity, size, unit, container)

        single_re = r'(\d+(.\d+)?)\s+({})\s+({}).*'.format(units_re, containers_re)

        def single_match(m):
            size = m.group(1)
            unit = units[m.group(3)]
            container = containers[m.group(4)]
            return '1,{} {},{}'.format(size, unit, container)

        each_re = r'EACH'

        data[['container_quantity', 'container_volume', 'container_type']] = \
            pd.DataFrame(
                data['size'].str.replace(keg_re, keg_match) \
                             .str.replace(metric_keg_re, metric_keg_match) \
                             .str.replace(growler_re, growler_match) \
                             .str.replace(group_re, group_match) \
                             .str.replace(single_re, single_match) \
                             .str.replace(each_re, '1, each, each') \
                             .str.split(',') \
                             .values.tolist()
            )

        # Apply the name map
        data['name_id'] = data['name'].apply(lambda x: self.names.get_value(x))
        # The product_id is computed using multiple values from a row.  axis=1 tells Pandas to
        # pass the entire row to the function.  We pass the timestamp in case a new value is added
        # The DB table holds the date the product first appeared.
        data['product_id'] = data.apply(
            lambda row: self.products.get_value(row['name_id'], row['container_quantity'], row['case_pack'],
                                                row['container_volume'], row['container_type'],
                                                row['category'], row['timestamp']),
            axis=1)

        # Make the table hold only the columns we want (and in the right order)
        columns = ['product_id', 'quantity', 'retail', 'case_retail', 'timestamp']
        data = data[columns]

        # Remove records with zero quantity (negative is kept)
        return data[data['quantity'] != 0].copy().reset_index(drop=True)

    def save_tables(self, data):
        # We have to write this before the inventory data because inventory(timestamp)
        # depends on timestamps(timestamp).
        timestamp = data['timestamp'].drop_duplicates()
        self.database_transactions.add_to_table('timestamps', timestamp, ['timestamp'])
        names_columns = ['name_id', 'tanczos_name']
        self.database_transactions.add_to_table('names', self.names.get_new_names(), names_columns)
        product_columns = ['product_id', 'name_id', 'container_quantity', 'quantity_in_case', 'container_volume',
                   'container_type', 'tanczos_category', 'date_added']
        self.database_transactions.add_to_table('products', self.products.get_new_products(), product_columns)

        csv_inventory_columns = ('name', 'size', 'category', 'quantity', 'retail',
                                 'case_retail', 'case_pack', 'timestamp')
        self.database_transactions.delete_all_rows('current_inventory')

        self.database_transactions.add_to_table('current_inventory', self.current_csv_inventory.get_current_inventory(),
                                                csv_inventory_columns)
        inventory_columns = ('product_id', 'quantity', 'retail', 'case_retail', 'timestamp')
        self.database_transactions.add_to_table('inventory', data, inventory_columns)

    def compute_and_save_transactions(self, data):
        columns = ['pre_inventory_id', 'post_inventory_id', 'product_id',
                   'transaction_quantity', 'timestamp', 'retail', 'case_retail']
        current_timestamp = data['timestamp'][0]
        if self.prev_timestamp is None:
            self.prev_timestamp = current_timestamp;
            # Return an empty DataFrame with the proper columns
            return pd.DataFrame(columns=columns)

        prev_dt = pd.to_datetime(self.prev_timestamp)
        curr_dt = pd.to_datetime(current_timestamp)

        delta = curr_dt - prev_dt
        threshold = timedelta(minutes=60)

        if delta > threshold:
            self.prev_timestamp = current_timestamp;
            # Return an empty DataFrame with the proper columns
            return pd.DataFrame(columns=columns)

        # Get the previous timestamp and the inventory for that timestamp.  We will use them after
        prev_inventory = self.database_transactions.get_inventory(self.prev_timestamp)
        curr_inventory = self.database_transactions.get_inventory(current_timestamp)

        # Compute the transactions
        transactions = compute_transactions(prev_inventory, curr_inventory)

        # there may not be transactions to save
        if len(transactions) > 0:
            self.database_transactions.add_to_table('transactions', transactions, columns)


if __name__ == '__main__':

    import sys
    import dotenv
    import os
    import time
    import datetime

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
    database_transactions = DBTransactions(conn)
    i = Ingest(database_transactions)

    files = sorted(os.listdir(path))
    count = 0
    total = len(files)

    start = time.time()

    for file in files:
        if not file.endswith('csv'):
            continue
        count += 1

        print('{}/{} ({:.2%}) {}'.format(count, total, count/total, file))

        current = time.time()
        elapsed = current - start
        percent_remaining = 1 - count / total
        remaining = elapsed * total / count - elapsed
        average = elapsed / count
        eta = start + elapsed * total / count
        print('Remaining: {}     ETA: {}   Average Time Per File: {:.2}' \
              .format(datetime.timedelta(seconds=int(remaining)), datetime.datetime.fromtimestamp(eta).strftime("%H:%M:%S"), average))

        i.ingest(path + '/' + file)


def profile_code():

    import cProfile
    from pstats import SortKey
    from beer.mocks.mock_database_transactions import MockDBTransactions

    dbt = MockDBTransactions([[], [], [], []])
    i = Ingest(dbt)

    filename = '~/smalldata/2017-11-30_18:43:47.476167.csv'

    cProfile.run('i.ingest(filename)', sort=SortKey.TIME)

    import sys
    sys.exit(0)
