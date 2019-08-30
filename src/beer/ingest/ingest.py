

import csv
import logging
from beer.ingest.transform import Transform
from beer.ingest.keymap import KeyMap
from beer.ingest.keymap_database import KeyMapDatabase
from beer.ingest.inventory_writer import InventoryWriter
from beer.ingest.timestamp_database import TimestampDatabase


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
        products = KeyMap(KeyMapDatabase('products'))
        sizes = KeyMap(KeyMapDatabase('sizes'))
        categories = KeyMap(KeyMapDatabase('categories'))

        self.transform = Transform(products, sizes, categories)
        self.timestamp_db = TimestampDatabase()

    def ingest(self, filename):
        """
        Convert a file and make necessary additions to inventory, products, sizes, categories, and timestamps.
        :param filename: the file to process
        :return: None
        """

        writer = InventoryWriter()

        with open(filename) as f:
            reader = csv.reader(f)
            # ignore the header
            next(reader)
            # Our data occasionally has bad lines, either lines that do not
            # contain all the fields, or lines that contain a sequence of NULL
            # characters (or both).  The instances we've seen all occur as the
            # last line of the file, but to be safe, this solution will handle
            # the errors anywhere.  Unfortunately, we have to use a while True
            # loop because the NULL characters are detected when a line is read.
            # As a result, the loop 'for row in reader' won't work - the exception
            # will be caught outside the loop, resulting in only a portion of the
            # file being read.
            while True:
                try:
                    # next can throw the csv.Error
                    row = next(reader)
                    values = self.transform.transform(row)
                    writer.add_record(values)

                except IndexError:
                    logging.warning('IndexError in {}'.format(filename))
                except csv.Error:
                    logging.error('CSV Error in {}'.format(filename))
                except StopIteration:
                    break

        # The last value is the timestamp.  Every row has the same value, so we
        # can use the last one
        # We have to write this before the inventory data because inventory(timestamp)
        # depends on timestamps(timestamp).
        timestamp = values[-1]
        self.timestamp_db.add(timestamp)

        # It is more efficient to write in bulk, so we do it for each file.
        # This will commit the changes and close the connection
        writer.write()



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
        print('{}/{} ({}) {}'.format(count, total, count/total, file))
        count += 1
        i.ingest(path + '/' + file)
