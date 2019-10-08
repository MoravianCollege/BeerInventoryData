

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
        self.cur.execute('DROP TABLE IF EXISTS transactions;')
        self.cur.execute('DROP TABLE IF EXISTS inventory;')
        self.cur.execute('DROP TABLE IF EXISTS products;')
        self.cur.execute('DROP TABLE IF EXISTS names')
        self.cur.execute('DROP TABLE IF EXISTS timestamps')
        self.cur.execute('DROP TABLE IF EXISTS current_inventory')

    def create_tables(self):
        """"
        Create the producdts, sizes, categories, and inventory tables.
        """
        create_timestamps = """CREATE TABLE timestamps (
                               timestamp TIMESTAMP PRIMARY KEY
                               );
                            """

        create_names = """CREATE TABLE names (
                            name_id SERIAL PRIMARY KEY,
                            tanczos_name TEXT
                            );
                          """

        create_products = """
                          CREATE TABLE products (
                            product_id SERIAL PRIMARY KEY,
                            name_id SERIAL REFERENCES names(name_id),
                            container_quantity INTEGER,
                            quantity_in_case INTEGER,
                            container_volume TEXT,
                            container_type TEXT,
                            tanczos_category TEXT,
                            date_added TIMESTAMP REFERENCES timestamps(timestamp)
                            );                            
                          """
        #  inventory_id is BIGSERIAL because we generate approximately 200 million per year
        create_inventory = """CREATE TABLE inventory (
                              inventory_id BIGSERIAL PRIMARY KEY,
                              product_id INTEGER REFERENCES products(product_id),
                              quantity NUMERIC(8,3),
                              retail NUMERIC(8,4),
                              case_retail NUMERIC(8,4),
                              timestamp TIMESTAMP REFERENCES timestamps(timestamp),
                              UNIQUE(product_id, timestamp)
                              );
                            """

        # pre_inventory_id may be -1 if new product
        create_transactions = """CREATE TABLE transactions (
                                 transaction_id BIGSERIAL PRIMARY KEY,
                                 pre_inventory_id BIGSERIAL, 
                                 post_inventory_id BIGSERIAL REFERENCES inventory(inventory_id),
                                 product_id SERIAL REFERENCES products(product_id),
                                 transaction_quantity NUMERIC(8,3),
                                 retail NUMERIC(8,4),
                                 case_retail NUMERIC(8,4),
                                 timestamp TIMESTAMP REFERENCES timestamps(timestamp)
                                 );
                              """

        create_current_inventory = """
                                   CREATE TABLE current_inventory (
                                   name TEXT,
                                   size TEXT,
                                   category TEXT,
                                   quantity NUMERIC(8,3),
                                   retail NUMERIC(8,4),
                                   case_retail NUMERIC(8,4),
                                   case_pack INTEGER,
                                   timestamp TIMESTAMP
                                   );                               
                                """

        self.cur.execute(create_timestamps)
        self.cur.execute(create_names)
        self.cur.execute(create_products)
        self.cur.execute(create_inventory)
        self.cur.execute(create_transactions)
        self.cur.execute(create_current_inventory)

    def create_indexes(self):

        self.cur.execute('CREATE INDEX inventory_timestamp_index ON inventory(timestamp);')

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
    dbs.create_indexes()
    dbs.commit()