
import psycopg2
import dotenv
import os


class KeyMapDatabase:

    def __init__(self, table):
        dotenv.load_dotenv()

        host = os.getenv('DB_HOST')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database_name = os.getenv('DB_NAME')

        self.conn = psycopg2.connect(dbname=database_name, host=host, user=user, password=password)
        self.cur = self.conn.cursor()
        self.table = table

    def read_map(self):
        self.cur.execute('SELECT * FROM {};'.format(self.table))

        products = {}
        for (id, product) in self.cur.fetchall():
            products[product] = id

        return products

    def add(self, product, id):
        """
        :param product: the name of the product to add
        :param id: the id of the product
        :return: None
        :except: psycopg2.errors.UniqueViolation if the id already exists
        """
        self.cur.execute('INSERT INTO {} VALUES (%s, %s);'.format(self.table), (id, product))
        self.conn.commit()


if __name__ == '__main__':
    kmd = KeyMapDatabase('categories')

    kmd.add('junk', 55)
    print(kmd.read_map())