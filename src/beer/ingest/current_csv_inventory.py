
import pandas as pd


class CurrentCSVInventory:
    """
    This class keeps track of the products in the inventory, and fills in any
    missing products.  Missing values will have the same timestamp as the other
    rows in the DataFrame - i.e. we assume that the inventory is unchanged
    for missing rows.

    This representation of the inventory *includes* products with 0 quantity.
    This is necessary because when we compute the transactions we need to know
    when the last of a product is sold.

    The expected input is a DataFrame created from a CSV file with NAs removed.
    """

    def __init__(self, current_inventory):
        self.prev = current_inventory

    def update_inventory(self, df):
        """
        Set the current inventory, filling in any missing products from the previous inventory
        :param df: the dataframe to use as the base of the new inventory
        :return: a Dataframe representing the current inventory (with missing values filled in)
        """

        # All values in the CSV are the same, so grab the first one
        timestamp = df['timestamp'][0]

        # Concat preserves order, which we need for drop_duplicates to work
        # reset_index renumbers the indexes to 0, 1, 2, ...  necessary for testing
        self.prev = pd.concat([self.prev, df], sort=False) \
            .drop_duplicates(subset=['name', 'size', 'category', 'case_pack'], keep='last')
            # Force filled timestamps to match the rest of the dataframe
        # i.e. assume the inventory is unchanged from previous
        self.prev['timestamp'] = timestamp
        columns = ['name', 'size', 'category', 'quantity', 'retail', 'case_retail', 'case_pack', 'timestamp']
        return self.prev[columns].copy()

        # All values in the CSV are the same, so grab the first one
        timestamp = df['timestamp'][0]

        merged_df = pd.concat([self.prev, df], sort=False)

        idx = merged_df.groupby(['name', 'size', 'category', 'case_pack']).transform(max)['timestamp'] == merged_df[
            'timestamp']
        self.prev = merged_df[idx].reset_index(drop=True).copy()

        # Force filled timestamps to match the rest of the dataframe
        # i.e. assume the inventory is unchanged from previous
        self.prev['timestamp'] = timestamp
        columns = ['name','size','category','quantity','retail','case_retail','case_pack','timestamp']
        return self.prev[columns]

    def get_current_inventory(self):
        columns=['name','size','category','quantity','retail','case_retail','case_pack','timestamp']
        return self.prev[columns]
