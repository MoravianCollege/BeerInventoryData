

import psycopg2
import dotenv
import os


class DBSetup:

    def __init__(self, host, user, password, dbname):
        self.conn = psycopg2.connect(dbname=dbname, host=host, user=user, password=password)
        self.cur = self.conn.cursor()

    def remove_tables_if_present(self):
        self.cur.execute('DROP TABLE IF EXISTS inventory;')
        self.cur.execute('DROP TABLE IF EXISTS products;')
        self.cur.execute('DROP TABLE IF EXISTS sizes;')
        self.cur.execute('DROP TABLE IF EXISTS categories;')

    def create_tables(self):
        create_products = """CREATE TABLE products (
                            product_id SERIAL PRIMARY KEY,
                            tanczos_name text
                            );
                          """

        create_sizes = """CREATE TABLE sizes (
                          size_id SERIAL PRIMARY KEY,
                          tanczos_size text
                          );
                       """

        create_categories = """CREATE TABLE categories (
                               category_id SERIAL PRIMARY KEY,
                               tanczos_category text
                               );
                            """

        #  inventory_id is BIGSERIAL because we generate approximately 200 million per year
        create_inventory = """CREATE TABLE inventory (
                              inventory_id BIGSERIAL PRIMARY KEY,
                              product_id INTEGER REFERENCES products(product_id),
                              size_id INTEGER REFERENCES sizes(size_id),
                              category_id INTEGER REFERENCES categories(category_id),
                              quantity NUMERIC(8,3),
                              retail MONEY,
                              case_retail MONEY,
                              case_pack INTEGER,
                              timestamp TIMESTAMP
                              );
                            """

        self.cur.execute(create_products)
        self.cur.execute(create_sizes)
        self.cur.execute(create_categories)
        self.cur.execute(create_inventory)

    def init_products(self, filename):
        with open(filename) as f:
            self.cur.copy_from(f, 'products', sep=',')

    def init_sizes(self, filename):
        with open(filename) as f:
            self.cur.copy_from(f, 'sizes', sep=',')

    def init_categories(self, filename):
        with open(filename) as f:
            self.cur.copy_from(f, 'categories', sep=',')

    def init_inventory(self, filename):
        columns = ('product_id', 'size_id', 'category_id', 'quantity', 'retail', 'case_retail', 'case_pack,timestamp')
        with open(filename) as f:
            self.cur.copy_from(f, 'inventory', sep=',', columns=columns)

    def commit(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()


if __name__ == '__main__':
    dotenv.load_dotenv()

    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    dbname = os.getenv('DB_NAME')

    dbs = DBSetup(host, user, password, dbname)

    dbs.remove_tables_if_present()
    dbs.create_tables()
    dbs.init_products('Keys/names.csv')
    dbs.init_sizes('Keys/sizes.csv')
    dbs.init_categories('Keys/categories.csv')

    inventory_files = os.listdir('NewFiles')

    count = 0
    total = len(inventory_files)
    for inventory_file in inventory_files:
        count += 1
        print('{}/{} ({}) {}'.format(count, total, count/total, inventory_file))
        dbs.init_inventory('NewFiles/' + inventory_file)

    dbs.commit()