import pandas as pd

class MockDBTransactions:

    def __init__(self, results):
        self.results = results
        self.commands = []

    def get_table(self, table_name):
        if table_name == 'current_inventory':
            data = self.results.pop(0)
            columns = ['Name','size','Category','Quantity_Available','Retail','Case Retail','Case Pack','timestamp']
            return pd.DataFrame(columns=columns, data=data)
        return self.results.pop(0)

    def add_to_table(self, table, data, columns):
        self.commands.append((table, data, columns))

    def delete_all_rows(self, table):
        pass

    def get_inventory(self, timestamp):
        # This will fail if it is ever used because the method is supposed
        # to return a dataframe.
        pass

