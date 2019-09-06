
import dotenv
import os
import psycopg2


class TimestampDatabase:
    """
    Interface to write timestamps to the database
    """

    def __init__(self, conn):
        self.conn = conn
        self.cur = self.conn.cursor()
        self.timestamps = set()

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
