import pandas as pd
import io


class DBTransactions:
    """
    This class provides an methods to read and write between
    dataframes and database tables.
    """

    def __init__(self, conn):
        """
        Establish the connection
        :param conn: the connection
        """
        self.conn = conn
        self.cur = self.conn.cursor()

    def get_table(self, table_name):
        """
        Get all data of a table as a DataFram
        :param table_name: the table to fetch
        :return: a Dataframe containing all data from the table
        """
        query_str = 'SELECT * FROM {}'.format(table_name)
        return pd.read_sql_query(query_str, con=self.conn)

    def add_to_table(self, table, data, columns):
        """
        Add data to a table
        :param table: the table
        :param data: the data to add
        :param columns: the columns of the table
        :return:
        """
        output = io.StringIO()
        data.to_csv(output, header=False, index=False)
        # seek to the beginning so copy_from can read it
        output.seek(0)
        self.cur.copy_from(output, table, sep=',', columns=columns)
        self.conn.commit()

    def delete_all_rows(self, table):
        """
        Remove all rows from a table
        :param table: the table
        :return: None
        """
        query_str = 'DELETE FROM {};'.format(table)
        self.cur.execute(query_str)

    def get_inventory(self, timestamp):
        """
        Get data from the inventory table
        :param timestamp: the timestamp to fetch
        :return: a DataFrame representing the inventory
        """
        query_str = "SELECT * FROM inventory WHERE timestamp='{}'".format(timestamp)
        return pd.read_sql_query(query_str, con=self.conn)

