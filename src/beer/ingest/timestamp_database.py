
import dotenv
import os
import psycopg2

class TimestampDatabase:
    """
    Interface to write timestamps to the database
    """

    def __init__(self):
        dotenv.load_dotenv()

        host = os.getenv('DB_HOST')
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database_name = os.getenv('DB_NAME')

        self.conn = psycopg2.connect(dbname=database_name, host=host, user=user, password=password)
        self.cur = self.conn.cursor()

    def add(self, timestamp):
        """
        :param timestamp: the timestamp to add
        :return: None
        :except: psycopg2.errors.UniqueViolation if the timestamp already exists
        """
        self.cur.execute("INSERT INTO timestamps VALUES ('{}');".format(timestamp))
        self.conn.commit()
