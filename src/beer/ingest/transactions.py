

def compute_transactions(first, second):
    """
    Compute all transaction between two dataframes of inventory.  A transactions is changes in the quantity
    for a product.  This means that if the quantities are the same in both inventories, there will NOT be a
    row for the product in the result.

    The results have the following fields:
    * 'pre_inventory_id' - the inventory id from the first inventory
    * 'post_inventory_id' - the inventory id from the second inventory
    * 'product_id' - the product id
    * 'transaction_quantity' - the transaction quantity.  Postive means sale, negative means stock received
    * 'timestamp' - the timestamp from the second inventory
    * 'retail' - the cost from the second inventory
    * 'case_retail' - the cost of a case from the second inventory

    If a product exists in the second inventory but not the first, the result will be a negative transaction
    quantity.  This represents a new product added to inventory.

    If a product exists in the first inventory but not the second, the product is ignored (no result).

    :param first: a dataframe representing the first inventory
    :param second: a dataframe repreasenting the second inventory
    :return: a dataframe with the form above, may contain zero rows
    """

    # right join to ensure that new products are included
    # fillna for missing products from first (so the difference in assign works)
    # astype on pre_inventory_id because NaN for missing products causes type to be float
    return first.merge(second, on=['product_id'], how='right') \
        .fillna({'quantity_x': 0, 'inventory_id_x': -1}) \
        .assign(transaction_quantity=lambda x: x['quantity_x'] - x['quantity_y']) \
        .rename(columns={'inventory_id_x': 'pre_inventory_id',
                         'inventory_id_y': 'post_inventory_id',
                         'timestamp_y': 'timestamp',
                         'retail_y': 'retail',
                         'case_retail_y': 'case_retail'}) \
        .astype({'pre_inventory_id': 'int64'}) \
        .loc[:,['pre_inventory_id', 'post_inventory_id', 'product_id', 'transaction_quantity',
                'timestamp', 'retail', 'case_retail']] \
        .query('transaction_quantity != 0')

