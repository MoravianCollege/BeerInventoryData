
from io import StringIO
import csv
import dotenv
import psycopg2
import os


class InventoryWriter:
    """
    Collect the rows for the inventory table and then write them in bulk
    """

    def __init__(self):
        self.records = StringIO()
        self.writer = csv.writer(self.records)

    def add_record(self, record):
        """
        Cache one record to be written.
        :param record: a tuple in the form [name, size, category, quantity_available, case_retail, case_pack, timestamp]
        :return: None
        """
        self.writer.writerow(record)

    def write(self):
        """
        Write all records to the inventory table.  This commits the changes and closes the connection.
        :return:
        """
        dotenv.load_dotenv()
        host = os.getenv('DB_HOST')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database_name = os.getenv('DB_NAME')

        conn = psycopg2.connect(dbname=database_name, host=host, user=user, password=password)
        cur = conn.cursor()

        # seek to the beginning of the string so that copy_from can read it.
        self.records.seek(0)

        # column names are required here because the inventory_id will be auto-filled for each row
        columns = ('product_id', 'size_id', 'category_id', 'quantity', 'retail', 'case_retail', 'case_pack,timestamp')
        cur.copy_from(self.records, 'inventory', sep=',', columns=columns)
        conn.commit()
        cur.close()
        conn.close()


