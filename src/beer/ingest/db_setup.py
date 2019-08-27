

import psycopg2
import dotenv
import os


class DBSetup:
    """
    Methods to remove / create tables

    This script may be run directly to remove existing tables and re-create new, empty tables.
    """

    def __init__(self, host, user, password, dbname):
        self.conn = psycopg2.connect(dbname=dbname, host=host, user=user, password=password)
        self.cur = self.conn.cursor()

    def remove_tables_if_present(self):
        """
        Drop inventory, products, sizes, and categories (if they exist)
        :return: None
        """
        self.cur.execute('DROP TABLE IF EXISTS inventory;')
        self.cur.execute('DROP TABLE IF EXISTS products;')
        self.cur.execute('DROP TABLE IF EXISTS sizes;')
        self.cur.execute('DROP TABLE IF EXISTS categories;')

    def create_tables(self):
        """"
        Create the producdts, sizes, categories, and inventory tables.
        """
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

    def commit(self):
        """
        Commit changes and close the connection
        :return:
        """
        self.conn.commit()
        self.cur.close()
        self.conn.close()


if __name__ == '__main__':
    dotenv.load_dotenv()

    host = os.getenv('DB_HOST')
    user = os.getenv('DB_USER')
    password = os.getenv('DB_PASSWORD')
    database_name = os.getenv('DB_NAME')

    dbs = DBSetup(host, user, password, database_name)

    dbs.remove_tables_if_present()
    dbs.create_tables()
    dbs.commit()