
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
        self.timestamps = set()

        self.cur.execute('SELECT * FROM timestamps');
        for timestamp, in self.cur.fetchall():
            self.timestamps.add(str(timestamp))

    def add(self, timestamp):
        """
        :param timestamp: (string) the timestamp to add
        :return: None
        :except: psycopg2.errors.UniqueViolation if the timestamp already exists
        """
        if self.contains(timestamp):
            return

        self.cur.execute("INSERT INTO timestamps (timestamp) VALUES ('{}');".format(timestamp))
        self.conn.commit()
        self.timestamps.add(timestamp)

    def contains(self, timestamp):
        return timestamp in self.timestamps
